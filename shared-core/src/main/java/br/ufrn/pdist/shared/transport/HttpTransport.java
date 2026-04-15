package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.Response;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public final class HttpTransport implements TransportLayer {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() { };
    private final TcpTransport tcpTransport = new TcpTransport();
    private final AtomicInteger nextClientStreamId = new AtomicInteger(1);

    @Override
    public void startServer(int port, RequestHandler handler) {
        tcpTransport.startSocketServer(port, "http2", socket -> handleConnection(socket, handler));
    }

    @Override
    public Response send(Request request, Instance target) {
        int streamId = nextClientStreamId.getAndAdd(2);
        HttpTarget actionTarget = parseActionTarget(request.action());
        try {
            return tcpTransport.executeClient(target, socket -> {
                BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());
                BufferedInputStream input = new BufferedInputStream(socket.getInputStream());
                byte[] body = OBJECT_MAPPER.writeValueAsBytes(request.payload());
                writeHttp2Request(output, streamId, actionTarget.method(), actionTarget.path(), body);
                output.flush();
                Http2Response http2Response = readHttp2Response(input, streamId);
                Map<String, Object> payload = parseBodyMap(http2Response.body());
                return new Response(http2Response.statusCode(), extractMessage(payload, http2Response.statusCode()), payload);
            });
        } catch (SocketTimeoutException exception) {
            return transportError(504, "Transport timeout", request.requestId());
        } catch (JsonProcessingException exception) {
            return transportError(400, "Malformed transport payload", request.requestId());
        } catch (IOException exception) {
            return transportError(502, "Transport I/O failure: " + exception.getMessage(), request.requestId());
        }
    }

    private void handleConnection(Socket clientSocket, RequestHandler handler) {
        String remote = clientSocket.getRemoteSocketAddress().toString();
        try (Socket socket = clientSocket;
             BufferedInputStream input = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream())) {
            Instant start = Instant.now();
            try {
                Http2Request incoming = readHttp2Request(input);
                String method = incoming.headers().getOrDefault(":method", "POST");
                String path = incoming.headers().getOrDefault(":path", "/");
                if (!isSupportedMethod(method)) {
                    writeHttp2Response(output, incoming.streamId(), 405, Map.of("message", "Method not allowed"));
                    output.flush();
                    logRequest(method, path, 405, start, incoming.streamId());
                    return;
                }
                Map<String, Object> payload = parseBodyMap(incoming.body());
                String requestId = resolveRequestId(payload);
                Response response = handler.handle(new Request(requestId, method + " " + path, payload));
                Map<String, Object> responsePayload = new LinkedHashMap<>(response.payload());
                responsePayload.putIfAbsent("requestId", requestId);
                responsePayload.putIfAbsent("message", response.message());
                writeHttp2Response(output, incoming.streamId(), response.statusCode(), responsePayload);
                output.flush();
                logRequest(method, path, response.statusCode(), start, incoming.streamId());
            } catch (Exception exception) {
                writeHttp2Response(output, 1, 400, Map.of("message", exception.getMessage()));
                output.flush();
                System.out.printf("transport=http2 event=bad-request remote=%s error=\"%s\"%n", remote, exception.getMessage());
            }
        } catch (IOException exception) {
            System.out.printf("transport=http2 event=io-failure remote=%s error=\"%s\"%n", remote, exception.getMessage());
        }
    }

    private static void writeHttp2Request(BufferedOutputStream output, int streamId, String method, String path, byte[] body)
            throws IOException {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(":method", method);
        headers.put(":path", path);
        headers.put(":scheme", "http");
        headers.put("content-type", "application/json");
        headers.put("te", "trailers");
        Http2Wire.writeFrame(output, streamId, Http2Wire.FRAME_HEADERS, Http2Wire.encodeHeaders(headers));
        Http2Wire.writeFrame(output, streamId, Http2Wire.FRAME_DATA, body);
    }

    private static Http2Request readHttp2Request(BufferedInputStream input) throws IOException {
        Http2Wire.Frame h = Http2Wire.readFrame(input);
        Http2Wire.Frame d = Http2Wire.readFrame(input);
        if (h.type() != Http2Wire.FRAME_HEADERS || d.type() != Http2Wire.FRAME_DATA || h.streamId() != d.streamId()) {
            throw new Http2Wire.Http2ProtocolException("invalid request frame sequence");
        }
        return new Http2Request(h.streamId(), Http2Wire.decodeHeaders(h.payload()), d.payload());
    }

    private static void writeHttp2Response(BufferedOutputStream output, int streamId, int statusCode, Map<String, Object> payload)
            throws IOException {
        byte[] body = OBJECT_MAPPER.writeValueAsBytes(payload);
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(":status", String.valueOf(statusCode));
        headers.put("content-type", "application/json");
        Http2Wire.writeFrame(output, streamId, Http2Wire.FRAME_HEADERS, Http2Wire.encodeHeaders(headers));
        Http2Wire.writeFrame(output, streamId, Http2Wire.FRAME_DATA, body);
    }

    private static Http2Response readHttp2Response(BufferedInputStream input, int expectedStreamId) throws IOException {
        Http2Wire.Frame h = Http2Wire.readFrame(input);
        Http2Wire.Frame d = Http2Wire.readFrame(input);
        if (h.type() != Http2Wire.FRAME_HEADERS || d.type() != Http2Wire.FRAME_DATA || h.streamId() != expectedStreamId
                || d.streamId() != expectedStreamId) {
            throw new Http2Wire.Http2ProtocolException("invalid response frame sequence");
        }
        String rawStatus = Http2Wire.decodeHeaders(h.payload()).get(":status");
        if (rawStatus == null || rawStatus.isBlank()) {
            throw new Http2Wire.Http2ProtocolException("missing :status");
        }
        return new Http2Response(Integer.parseInt(rawStatus), d.payload());
    }

    private static HttpTarget parseActionTarget(String action) {
        if (action == null || action.isBlank()) {
            return new HttpTarget("POST", "/");
        }
        String[] parts = action.trim().split(" ", 2);
        if (parts.length == 2 && isSupportedMethod(parts[0].toUpperCase(Locale.ROOT))) {
            return new HttpTarget(parts[0].toUpperCase(Locale.ROOT), parts[1]);
        }
        return new HttpTarget("POST", "/" + action.trim().replace(' ', '-'));
    }

    private static boolean isSupportedMethod(String method) {
        return "GET".equals(method) || "POST".equals(method);
    }

    private static Map<String, Object> parseBodyMap(byte[] body) throws IOException {
        return body.length == 0 ? new LinkedHashMap<>() : OBJECT_MAPPER.readValue(body, MAP_TYPE);
    }

    private static String resolveRequestId(Map<String, Object> payload) {
        Object value = payload.get("requestId");
        if (value == null || value.toString().isBlank()) {
            String generated = UUID.randomUUID().toString();
            payload.put("requestId", generated);
            return generated;
        }
        return value.toString();
    }

    private static String extractMessage(Map<String, Object> payload, int statusCode) {
        Object message = payload.get("message");
        return message == null ? "HTTP/2 " + statusCode : message.toString();
    }

    private static void logRequest(String method, String path, int status, Instant start, int streamId) {
        long elapsed = Duration.between(start, Instant.now()).toMillis();
        System.out.printf("transport=http2 event=request method=%s path=%s status=%d streamId=%d durationMs=%d%n",
                method, path, status, streamId, elapsed);
    }

    private static Response transportError(int statusCode, String message, String requestId) {
        return new Response(statusCode, message, Map.of("requestId", requestId));
    }

    private record HttpTarget(String method, String path) { }
    private record Http2Request(int streamId, Map<String, String> headers, byte[] body) { }
    private record Http2Response(int statusCode, byte[] body) { }
}
