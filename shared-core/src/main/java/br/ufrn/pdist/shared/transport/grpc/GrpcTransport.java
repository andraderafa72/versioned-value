package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.Response;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class GrpcTransport implements TransportLayer {
    private static final String CONTENT_TYPE = "application/grpc";
    private static final String TE = "trailers";
    private static final int OK = 0;
    private final TcpTransport tcpTransport = new TcpTransport();
    private final AtomicInteger nextClientStreamId = new AtomicInteger(1);
    private final Map<String, AtomicInteger> activeStreamsByConnection = new ConcurrentHashMap<>();

    @Override
    public void startServer(int port, RequestHandler handler) {
        tcpTransport.startSocketServer(port, "grpc", socket -> handleServerConnection(socket, handler));
    }

    @Override
    public Response send(Request request, Instance target) {
        int streamId = nextClientStreamId.getAndAdd(2);
        try {
            return tcpTransport.executeClient(target, socket -> executeClientCall(socket, request, streamId));
        } catch (SocketTimeoutException exception) {
            return grpcTransportError(14, "deadline exceeded", request.requestId());
        } catch (IOException exception) {
            return grpcTransportError(14, "unavailable: " + exception.getMessage(), request.requestId());
        } catch (RuntimeException exception) {
            return grpcTransportError(13, exception.getMessage(), request.requestId());
        }
    }

    private Response executeClientCall(Socket socket, Request request, int streamId) throws IOException {
        BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());
        BufferedInputStream input = new BufferedInputStream(socket.getInputStream());
        Instant start = Instant.now();
        String path = resolvePath(request.action());

        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(":method", "POST");
        headers.put(":path", path);
        headers.put(":scheme", "http");
        headers.put("content-type", CONTENT_TYPE);
        headers.put("te", TE);
        Http2Wire.writeFrame(output, streamId, Http2Wire.FRAME_HEADERS, Http2Wire.encodeHeaders(headers));
        Http2Wire.writeFrame(output, streamId, Http2Wire.FRAME_DATA, encodeGrpcData(ProtoCodec.encodeRequest(request)));
        output.flush();

        Http2Wire.Frame h = Http2Wire.readFrame(input);
        Http2Wire.Frame d = Http2Wire.readFrame(input);
        Http2Wire.Frame t = Http2Wire.readFrame(input);
        ensure(expected(streamId, h, Http2Wire.FRAME_HEADERS), "expected response headers");
        ensure(expected(streamId, d, Http2Wire.FRAME_DATA), "expected response data");
        ensure(expected(streamId, t, Http2Wire.FRAME_TRAILERS), "expected response trailers");

        Map<String, String> responseHeaders = Http2Wire.decodeHeaders(h.payload());
        if (!CONTENT_TYPE.equals(responseHeaders.getOrDefault("content-type", ""))) {
            return grpcTransportError(13, "invalid content-type", request.requestId());
        }
        Map<String, String> trailers = Http2Wire.decodeHeaders(t.payload());
        int grpcStatus = parseStatus(trailers.get("grpc-status"));
        String grpcMessage = trailers.getOrDefault("grpc-message", "");
        logExchange("client", path, streamId, d.payload().length, grpcStatus, start);
        if (grpcStatus != OK) {
            return grpcTransportError(grpcStatus, grpcMessage, request.requestId());
        }
        Response decoded = ProtoCodec.decodeResponse(decodeGrpcData(d.payload()));
        Map<String, Object> payload = new LinkedHashMap<>(decoded.payload());
        payload.putIfAbsent("requestId", request.requestId());
        return new Response(decoded.statusCode(), decoded.message(), payload);
    }

    private void handleServerConnection(Socket clientSocket, RequestHandler handler) {
        String remote = clientSocket.getRemoteSocketAddress().toString();
        track(remote, 1);
        try (Socket socket = clientSocket;
             BufferedInputStream input = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream())) {
            Instant start = Instant.now();
            Http2Wire.Frame h = Http2Wire.readFrame(input);
            Http2Wire.Frame d = Http2Wire.readFrame(input);
            if (!expected(h.streamId(), h, Http2Wire.FRAME_HEADERS) || !expected(h.streamId(), d, Http2Wire.FRAME_DATA)) {
                writeError(output, h.streamId(), 13, "invalid request frame sequence");
                return;
            }
            Map<String, String> headers = Http2Wire.decodeHeaders(h.payload());
            validateHeaders(headers);
            Request request = ProtoCodec.decodeRequest(decodeGrpcData(d.payload()));
            Response handled = handler.handle(request);
            Map<String, Object> payload = new LinkedHashMap<>(handled.payload());
            payload.putIfAbsent("requestId", request.requestId());
            Response normalized = new Response(handled.statusCode(), handled.message(), payload);
            byte[] responsePayload = encodeGrpcData(ProtoCodec.encodeResponse(normalized));

            Http2Wire.writeFrame(output, h.streamId(), Http2Wire.FRAME_HEADERS, Http2Wire.encodeHeaders(Map.of("content-type", CONTENT_TYPE)));
            Http2Wire.writeFrame(output, h.streamId(), Http2Wire.FRAME_DATA, responsePayload);
            Http2Wire.writeFrame(output, h.streamId(), Http2Wire.FRAME_TRAILERS, Http2Wire.encodeHeaders(Map.of("grpc-status", "0")));
            output.flush();
            logExchange("server", headers.getOrDefault(":path", "/unknown"), h.streamId(), responsePayload.length, OK, start);
        } catch (Exception exception) {
            respondBestEffort(clientSocket, 1, 13, exception.getMessage());
        } finally {
            track(remote, -1);
        }
    }

    private void respondBestEffort(Socket socket, int streamId, int status, String message) {
        try (BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream())) {
            writeError(output, streamId, status, message);
        } catch (IOException ignored) {
        }
    }

    private static void writeError(BufferedOutputStream output, int streamId, int grpcStatus, String grpcMessage) throws IOException {
        Http2Wire.writeFrame(output, streamId, Http2Wire.FRAME_HEADERS, Http2Wire.encodeHeaders(Map.of("content-type", CONTENT_TYPE)));
        Http2Wire.writeFrame(output, streamId, Http2Wire.FRAME_DATA, encodeGrpcData(new byte[0]));
        Http2Wire.writeFrame(output, streamId, Http2Wire.FRAME_TRAILERS,
                Http2Wire.encodeHeaders(Map.of("grpc-status", String.valueOf(grpcStatus), "grpc-message", grpcMessage == null ? "" : grpcMessage)));
        output.flush();
    }

    private static boolean expected(int streamId, Http2Wire.Frame frame, byte type) {
        return frame.streamId() == streamId && frame.type() == type;
    }

    private static void ensure(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void validateHeaders(Map<String, String> headers) {
        if (!"POST".equals(headers.get(":method")) || headers.get(":path") == null || headers.get(":path").isBlank()) {
            throw new IllegalArgumentException("invalid gRPC pseudo-headers");
        }
        if (!CONTENT_TYPE.equals(headers.get("content-type")) || !TE.equals(headers.get("te"))) {
            throw new IllegalArgumentException("invalid gRPC headers");
        }
    }

    private static byte[] encodeGrpcData(byte[] message) {
        byte[] data = new byte[5 + message.length];
        data[0] = 0;
        byte[] len = ByteBuffer.allocate(4).putInt(message.length).array();
        System.arraycopy(len, 0, data, 1, 4);
        System.arraycopy(message, 0, data, 5, message.length);
        return data;
    }

    private static byte[] decodeGrpcData(byte[] bytes) {
        if (bytes.length < 5 || bytes[0] != 0) {
            throw new IllegalArgumentException("invalid gRPC message frame");
        }
        int length = ByteBuffer.wrap(bytes, 1, 4).getInt();
        if (length < 0 || bytes.length != 5 + length) {
            throw new IllegalArgumentException("message length mismatch");
        }
        byte[] payload = new byte[length];
        System.arraycopy(bytes, 5, payload, 0, length);
        return payload;
    }

    private static String resolvePath(String action) {
        if (action == null || action.isBlank()) {
            return "/pdist.Service/Unknown";
        }
        String normalized = action.trim().toLowerCase(Locale.ROOT).replace(" ", ".");
        return normalized.startsWith("/") ? normalized : "/pdist.Service/" + normalized;
    }

    private static int parseStatus(String raw) {
        try {
            return raw == null ? 13 : Integer.parseInt(raw);
        } catch (NumberFormatException exception) {
            return 13;
        }
    }

    private void track(String remote, int delta) {
        activeStreamsByConnection.compute(remote, (key, value) -> {
            AtomicInteger count = value == null ? new AtomicInteger() : value;
            int current = count.addAndGet(delta);
            return current <= 0 ? null : count;
        });
    }

    private int activeStreams() {
        return activeStreamsByConnection.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    private void logExchange(String side, String path, int streamId, int messageSize, int grpcStatus, Instant start) {
        long elapsed = Duration.between(start, Instant.now()).toMillis();
        System.out.printf("transport=grpc side=%s path=%s streamId=%d messageBytes=%d durationMs=%d grpcStatus=%d activeStreams=%d%n",
                side, path, streamId, messageSize, elapsed, grpcStatus, activeStreams());
    }

    private static Response grpcTransportError(int status, String message, String requestId) {
        return new Response(502, message == null ? "gRPC transport error" : message, Map.of(
                "requestId", requestId,
                "grpc-status", status,
                "grpc-message", message == null ? "" : message
        ));
    }
}
