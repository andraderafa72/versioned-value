package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.http.BankingHttpRoutes;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public final class HttpTransport implements TransportLayer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final int READ_TIMEOUT_MILLIS = 5_000;

    @Override
    public void startServer(int port, RequestHandler handler) {
        Thread acceptThread = new Thread(() -> runServer(port, handler), "http-server-" + port);
        acceptThread.setDaemon(false);
        acceptThread.start();
    }

    private void runServer(int port, RequestHandler handler) {
        try (ServerSocket serverSocket = new ServerSocket(port, 300)) {
            System.out.printf("transport=http event=server-started port=%d%n", port);
            while (true) {
                Socket client = serverSocket.accept();
                Thread worker = new Thread(
                        () -> handleConnection(client, handler),
                        "http-conn-" + client.getPort()
                );
                worker.setDaemon(false);
                worker.start();
            }
        } catch (IOException e) {
            System.err.printf("transport=http event=server-failure port=%d error=\"%s\"%n", port, e.getMessage());
        }
    }

    private void handleConnection(Socket socket, RequestHandler handler) {
        try (socket;
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             OutputStream out = socket.getOutputStream()) {

            String requestLine = reader.readLine();
            if (requestLine == null || requestLine.isBlank()) {
                return;
            }
            String[] parts = requestLine.split(" ", 3);
            if (parts.length < 2) {
                return;
            }
            String method = parts[0].toUpperCase(Locale.ROOT);
            String path = parts[1];

            int contentLength = 0;
            String requestId = null;
            String line;
            while ((line = reader.readLine()) != null && !line.isEmpty()) {
                String lower = line.toLowerCase(Locale.ROOT);
                if (lower.startsWith("content-length:")) {
                    contentLength = Integer.parseInt(line.substring(15).trim());
                } else if (lower.startsWith("x-request-id:")) {
                    requestId = line.substring(13).trim();
                }
            }

            String body = "";
            if (contentLength > 0) {
                char[] buf = new char[contentLength];
                int totalRead = 0;
                while (totalRead < contentLength) {
                    int n = reader.read(buf, totalRead, contentLength - totalRead);
                    if (n < 0) break;
                    totalRead += n;
                }
                body = new String(buf, 0, totalRead);
            }

            Map<String, Object> payload;
            try {
                payload = body.isEmpty() ? new LinkedHashMap<>() : OBJECT_MAPPER.readValue(body, MAP_TYPE);
            } catch (IOException e) {
                sendHttpResponse(out, 400, "Bad Request", Map.of("message", "Invalid JSON body"));
                return;
            }

            if (requestId == null || requestId.isBlank()) {
                Object fromBody = payload.get("requestId");
                requestId = (fromBody != null && !fromBody.toString().isBlank())
                        ? fromBody.toString()
                        : UUID.randomUUID().toString();
            }

            Response response;
            try {
                response = handler.handle(new Request(requestId, method + " " + path, payload));
                if (response == null) {
                    response = transportError(500, "Handler returned null response", requestId);
                }
            } catch (RuntimeException e) {
                response = transportError(500, "Handler failure: " + e.getMessage(), requestId);
            }

            Map<String, Object> responsePayload = new LinkedHashMap<>();
            if (response.payload() != null) responsePayload.putAll(response.payload());
            responsePayload.putIfAbsent("requestId", requestId);
            responsePayload.putIfAbsent("message", response.message());
            sendHttpResponse(out, response.statusCode(), statusText(response.statusCode()), responsePayload);

        } catch (IOException e) {
            if (isBrokenPipe(e)) {
                System.out.printf("transport=http event=client-disconnected%n");
            } else {
                System.err.printf("transport=http event=io-failure error=\"%s\"%n", e.getMessage());
            }
        }
    }

    private static boolean isBrokenPipe(IOException e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("Broken pipe") || msg.contains("Pipe quebrado")
                || msg.contains("Connection reset") || msg.contains("Software caused connection abort"));
    }

    private static void sendHttpResponse(OutputStream out, int statusCode, String statusText,
                                         Map<String, Object> payload) throws IOException {
        byte[] body = OBJECT_MAPPER.writeValueAsBytes(payload);
        PrintWriter header = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), false);
        header.printf("HTTP/1.1 %d %s\r\n", statusCode, statusText);
        header.printf("Server: versioned-value\r\n");
        header.printf("Content-Type: application/json\r\n");
        header.printf("Content-Length: %d\r\n", body.length);
        header.printf("Connection: close\r\n");
        header.printf("\r\n");
        header.flush();
        out.write(body);
        out.flush();
    }

    @Override
    public Response send(Request request, Instance target) {
        try (Socket socket = new Socket(target.host(), target.port())) {
            socket.setSoTimeout(READ_TIMEOUT_MILLIS);

            String method;
            String path;
            byte[] body;

            if (request.service() != null && request.action() != null && !request.action().contains(" ")) {
                BankingHttpRoutes.OutboundHttp outbound = BankingHttpRoutes.toOutboundRequest(request);
                method = outbound.method();
                path = outbound.path();
                body = OBJECT_MAPPER.writeValueAsBytes(outbound.jsonBody());
            } else {
                String[] mp = parseMethodPath(request.action());
                method = mp[0];
                path = mp[1];
                Map<String, Object> p = request.payload() == null ? Map.of() : request.payload();
                body = OBJECT_MAPPER.writeValueAsBytes(p);
            }

            OutputStream out = socket.getOutputStream();
            PrintWriter header = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), false);
            header.printf("%s %s HTTP/1.1\r\n", method, path);
            header.printf("Host: %s:%d\r\n", target.host(), target.port());
            header.printf("Content-Type: application/json\r\n");
            header.printf("Content-Length: %d\r\n", body.length);
            header.printf("X-Request-Id: %s\r\n", request.requestId());
            header.printf("Connection: close\r\n");
            header.printf("\r\n");
            header.flush();
            out.write(body);
            out.flush();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String statusLine = reader.readLine();
            if (statusLine == null) {
                return transportError(502, "Empty response from server", request.requestId());
            }
            String[] statusParts = statusLine.split(" ", 3);
            int statusCode = Integer.parseInt(statusParts[1]);

            int responseContentLength = 0;
            String respLine;
            while ((respLine = reader.readLine()) != null && !respLine.isEmpty()) {
                if (respLine.toLowerCase(Locale.ROOT).startsWith("content-length:")) {
                    responseContentLength = Integer.parseInt(respLine.substring(15).trim());
                }
            }

            String responseBody = "";
            if (responseContentLength > 0) {
                char[] buf = new char[responseContentLength];
                int totalRead = 0;
                while (totalRead < responseContentLength) {
                    int n = reader.read(buf, totalRead, responseContentLength - totalRead);
                    if (n < 0) break;
                    totalRead += n;
                }
                responseBody = new String(buf, 0, totalRead);
            }

            Map<String, Object> responsePayload = responseBody.isEmpty()
                    ? new LinkedHashMap<>()
                    : OBJECT_MAPPER.readValue(responseBody, MAP_TYPE);
            Object msg = responsePayload.get("message");
            String message = msg != null ? msg.toString() : "HTTP " + statusCode;
            return new Response(statusCode, message, responsePayload);

        } catch (SocketTimeoutException e) {
            return transportError(504, "Transport timeout", request.requestId());
        } catch (IOException e) {
            return transportError(502, "Transport I/O failure: " + e.getMessage(), request.requestId());
        }
    }

    private static String[] parseMethodPath(String action) {
        if (action == null || action.isBlank()) {
            return new String[]{"POST", "/"};
        }
        String[] parts = action.trim().split(" ", 2);
        if (parts.length == 2) {
            return new String[]{parts[0].toUpperCase(Locale.ROOT), parts[1]};
        }
        return new String[]{"POST", "/" + action.trim().replace(' ', '-')};
    }

    private static String statusText(int code) {
        return switch (code) {
            case 200 -> "OK";
            case 201 -> "Created";
            case 400 -> "Bad Request";
            case 403 -> "Forbidden";
            case 404 -> "Not Found";
            case 405 -> "Method Not Allowed";
            case 409 -> "Conflict";
            case 500 -> "Internal Server Error";
            case 502 -> "Bad Gateway";
            case 503 -> "Service Unavailable";
            case 504 -> "Gateway Timeout";
            default -> "Unknown";
        };
    }

    private static Response transportError(int statusCode, String message, String requestId) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("requestId", requestId);
        payload.put("message", message);
        return new Response(statusCode, message, payload);
    }
}
