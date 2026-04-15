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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public final class HttpTransport implements TransportLayer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() { };
    private final TcpTransport tcpTransport = new TcpTransport();

    @Override
    public void startServer(int port, RequestHandler handler) {
        tcpTransport.startSocketServer(port, "http", socket -> handleConnection(socket, handler));
    }

    @Override
    public Response send(Request request, Instance target) {
        HttpTarget actionTarget = parseActionTarget(request.action());
        try {
            return tcpTransport.executeClient(target, socket -> {
                BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());
                BufferedInputStream input = new BufferedInputStream(socket.getInputStream());

                byte[] body = OBJECT_MAPPER.writeValueAsBytes(request.payload());
                writeHttpRequest(output, actionTarget.method(), actionTarget.path(), body);
                output.flush();

                HttpMessage responseMessage = readHttpMessage(input);
                int statusCode = parseStatusCode(responseMessage.startLine());
                Map<String, Object> payload = parseBodyMap(responseMessage.body());
                String message = extractMessage(payload, statusCode);
                return new Response(statusCode, message, payload);
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
                HttpMessage requestMessage = readHttpMessage(input);
                ParsedStartLine parsed = parseStartLine(requestMessage.startLine());
                if (!isSupportedMethod(parsed.method())) {
                    writeHttpResponse(output, 405, "Method Not Allowed", Map.of("message", "Method not allowed"));
                    output.flush();
                    logRequest(parsed.method(), parsed.path(), 405, start);
                    return;
                }

                Map<String, Object> payload = parseBodyMap(requestMessage.body());
                String requestId = resolveRequestId(payload);
                Request request = new Request(requestId, parsed.method() + " " + parsed.path(), payload);
                Response response = handler.handle(request);
                Map<String, Object> responsePayload = new LinkedHashMap<>(response.payload());
                responsePayload.putIfAbsent("requestId", requestId);
                responsePayload.putIfAbsent("message", response.message());
                writeHttpResponse(output, response.statusCode(), httpReasonPhrase(response.statusCode()), responsePayload);
                output.flush();
                logRequest(parsed.method(), parsed.path(), response.statusCode(), start);
            } catch (HttpParseException exception) {
                Map<String, Object> payload = Map.of("message", exception.getMessage());
                writeHttpResponse(output, 400, "Bad Request", payload);
                output.flush();
                System.out.printf("transport=http event=bad-request remote=%s error=\"%s\"%n", remote, exception.getMessage());
            } catch (SocketTimeoutException exception) {
                Map<String, Object> payload = Map.of("message", exception.getMessage());
                writeHttpResponse(output, 408, "Request Timeout", payload);
                output.flush();
                System.out.printf("transport=http event=server-error remote=%s code=%d error=\"%s\"%n",
                        remote, 408, exception.getMessage());
            } catch (Exception exception) {
                Map<String, Object> payload = Map.of("message", exception.getMessage());
                writeHttpResponse(output, 500, "Internal Server Error", payload);
                output.flush();
                System.out.printf("transport=http event=server-error remote=%s code=%d error=\"%s\"%n",
                        remote, 500, exception.getMessage());
            }
        } catch (IOException exception) {
            System.out.printf("transport=http event=io-failure remote=%s error=\"%s\"%n", remote, exception.getMessage());
        }
    }

    private static void writeHttpRequest(BufferedOutputStream output, String method, String path, byte[] body)
            throws IOException {
        String startLine = method + " " + path + " HTTP/1.1\r\n";
        output.write(startLine.getBytes(StandardCharsets.UTF_8));
        output.write(("Host: 127.0.0.1\r\n").getBytes(StandardCharsets.UTF_8));
        output.write(("Content-Type: application/json\r\n").getBytes(StandardCharsets.UTF_8));
        output.write(("Content-Length: " + body.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        output.write(("Connection: close\r\n").getBytes(StandardCharsets.UTF_8));
        output.write("\r\n".getBytes(StandardCharsets.UTF_8));
        output.write(body);
    }

    private static void writeHttpResponse(BufferedOutputStream output, int statusCode, String reasonPhrase, Map<String, Object> payload)
            throws IOException {
        byte[] body = OBJECT_MAPPER.writeValueAsBytes(payload);
        output.write(("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n").getBytes(StandardCharsets.UTF_8));
        output.write(("Content-Type: application/json\r\n").getBytes(StandardCharsets.UTF_8));
        output.write(("Content-Length: " + body.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        output.write(("Connection: close\r\n").getBytes(StandardCharsets.UTF_8));
        output.write("\r\n".getBytes(StandardCharsets.UTF_8));
        output.write(body);
    }

    private static HttpMessage readHttpMessage(BufferedInputStream input) throws IOException {
        String startLine = readAsciiLine(input);
        if (startLine.isBlank()) {
            throw new HttpParseException("Missing HTTP start line");
        }
        Map<String, String> headers = new LinkedHashMap<>();
        while (true) {
            String headerLine = readAsciiLine(input);
            if (headerLine.isEmpty()) {
                break;
            }
            int separator = headerLine.indexOf(':');
            if (separator <= 0) {
                throw new HttpParseException("Malformed header: " + headerLine);
            }
            String name = headerLine.substring(0, separator).trim().toLowerCase(Locale.ROOT);
            String value = headerLine.substring(separator + 1).trim();
            headers.put(name, value);
        }

        int contentLength = 0;
        if (headers.containsKey("content-length")) {
            try {
                contentLength = Integer.parseInt(headers.get("content-length"));
            } catch (NumberFormatException exception) {
                throw new HttpParseException("Invalid Content-Length");
            }
        }

        byte[] body = readFixedBytes(input, contentLength);
        return new HttpMessage(startLine, headers, body);
    }

    private static byte[] readFixedBytes(BufferedInputStream input, int contentLength) throws IOException {
        if (contentLength == 0) {
            return new byte[0];
        }
        byte[] body = new byte[contentLength];
        int totalRead = 0;
        while (totalRead < contentLength) {
            int read = input.read(body, totalRead, contentLength - totalRead);
            if (read < 0) {
                throw new HttpParseException("Unexpected end of stream while reading body");
            }
            totalRead += read;
        }
        return body;
    }

    private static String readAsciiLine(BufferedInputStream input) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        boolean seenCarriageReturn = false;
        while (true) {
            int current = input.read();
            if (current == -1) {
                if (buffer.size() == 0) {
                    return "";
                }
                break;
            }
            if (current == '\n' && seenCarriageReturn) {
                break;
            }
            if (seenCarriageReturn) {
                buffer.write('\r');
                seenCarriageReturn = false;
            }
            if (current == '\r') {
                seenCarriageReturn = true;
                continue;
            }
            buffer.write(current);
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    private static ParsedStartLine parseStartLine(String startLine) {
        String[] parts = startLine.split(" ");
        if (parts.length != 3) {
            throw new HttpParseException("Malformed start line");
        }
        if (!parts[2].startsWith("HTTP/")) {
            throw new HttpParseException("Unsupported HTTP version");
        }
        return new ParsedStartLine(parts[0].toUpperCase(Locale.ROOT), parts[1]);
    }

    private static boolean isSupportedMethod(String method) {
        return "GET".equals(method) || "POST".equals(method);
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

    private static int parseStatusCode(String statusLine) {
        String[] parts = statusLine.split(" ");
        if (parts.length < 2) {
            throw new HttpParseException("Malformed response status line");
        }
        return Integer.parseInt(parts[1]);
    }

    private static Map<String, Object> parseBodyMap(byte[] body) throws IOException {
        if (body.length == 0) {
            return new LinkedHashMap<>();
        }
        return OBJECT_MAPPER.readValue(body, MAP_TYPE);
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
        if (message != null) {
            return message.toString();
        }
        return "HTTP " + statusCode;
    }

    private static String httpReasonPhrase(int statusCode) {
        return switch (statusCode) {
            case 200 -> "OK";
            case 201 -> "Created";
            case 400 -> "Bad Request";
            case 404 -> "Not Found";
            case 405 -> "Method Not Allowed";
            case 408 -> "Request Timeout";
            case 500 -> "Internal Server Error";
            case 502 -> "Bad Gateway";
            case 504 -> "Gateway Timeout";
            default -> "Custom";
        };
    }

    private static void logRequest(String method, String path, int status, Instant start) {
        long elapsed = Duration.between(start, Instant.now()).toMillis();
        System.out.printf("transport=http event=request method=%s path=%s status=%d durationMs=%d%n",
                method, path, status, elapsed);
    }

    private static Response transportError(int statusCode, String message, String requestId) {
        return new Response(statusCode, message, Map.of("requestId", requestId));
    }

    private record HttpMessage(String startLine, Map<String, String> headers, byte[] body) {
    }

    private record ParsedStartLine(String method, String path) {
    }

    private record HttpTarget(String method, String path) {
    }

    private static final class HttpParseException extends RuntimeException {
        private HttpParseException(String message) {
            super(message);
        }
    }
}
