package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.logging.EventLog;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class TcpTransport implements TransportLayer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int CONNECT_TIMEOUT_MILLIS = 2_000;
    private static final int READ_TIMEOUT_MILLIS = 2_000;

    @FunctionalInterface
    interface ClientSocketOperation<T> {
        T execute(Socket socket) throws IOException;
    }

    @FunctionalInterface
    interface ServerSocketOperation {
        void execute(Socket socket) throws IOException;
    }

    @Override
    public void startServer(int port, RequestHandler handler) {
        startSocketServer(port, "tcp", socket -> handleConnection(socket, handler));
    }

    @Override
    public Response send(Request request, Instance target) {
        try {
            return executeClient(target, socket -> {
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                 BufferedReader reader = new BufferedReader(
                         new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                writer.write(OBJECT_MAPPER.writeValueAsString(request));
                writer.newLine();
                writer.flush();

                String rawResponse = reader.readLine();
                if (rawResponse == null) {
                    return transportError(502, "Connection closed without response", request.requestId());
                }
                return OBJECT_MAPPER.readValue(rawResponse, Response.class);
            }
            });
        } catch (SocketTimeoutException exception) {
            return transportError(504, "Transport timeout", request.requestId());
        } catch (JsonProcessingException exception) {
            return transportError(400, "Malformed transport payload", request.requestId());
        } catch (IOException exception) {
            return transportError(502, "Transport I/O failure: " + exception.getMessage(), request.requestId());
        }
    }

    <T> T executeClient(Instance target, ClientSocketOperation<T> operation) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(target.host(), target.port()), CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(READ_TIMEOUT_MILLIS);
            return operation.execute(socket);
        }
    }

    void startSocketServer(int port, String transportName, ServerSocketOperation operation) {
        Thread acceptThread = new Thread(() -> runServer(port, transportName, operation), transportName + "-server-" + port);
        // Keep server thread non-daemon so service process remains alive.
        acceptThread.setDaemon(false);
        acceptThread.start();
    }

    private void runServer(int port, String transportName, ServerSocketOperation operation) {
        ExecutorService workerPool = Executors.newFixedThreadPool(TransportServerDefaults.WORKER_POOL_SIZE);
        try (ServerSocket serverSocket = new ServerSocket(port, TransportServerDefaults.ACCEPT_BACKLOG)) {
            System.out.printf("transport=%s event=server-started port=%d%n", transportName, port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                workerPool.execute(() -> {
                    try {
                        operation.execute(clientSocket);
                    } catch (IOException exception) {
                        String remote = clientSocket.getRemoteSocketAddress().toString();
                        EventLog.printlnWithStackTrace(
                                String.format(
                                        "transport=%s event=io-failure remote=%s error=\"%s\"",
                                        transportName,
                                        remote,
                                        exception.getMessage()
                                ),
                                exception
                        );
                    }
                });
            }
        } catch (IOException exception) {
            EventLog.printlnWithStackTrace(
                    String.format(
                            "transport=%s event=server-failure port=%d error=\"%s\"",
                            transportName,
                            port,
                            exception.getMessage()
                    ),
                    exception
            );
        } finally {
            workerPool.shutdown();
        }
    }

    private void handleConnection(Socket clientSocket, RequestHandler handler) {
        String remote = clientSocket.getRemoteSocketAddress().toString();
        System.out.printf("transport=tcp event=connection-open remote=%s%n", remote);

        try (Socket socket = clientSocket;
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(
                     new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
            String rawRequest = reader.readLine();
            if (rawRequest == null) {
                Response error = transportError(400, "Empty request", UUID.randomUUID().toString());
                writeResponse(writer, error);
                System.out.printf("transport=tcp event=io-failure remote=%s error=\"empty request\"%n", remote);
                return;
            }

            Request request = null;
            try {
                request = OBJECT_MAPPER.readValue(rawRequest, Request.class);
            } catch (IOException parseException) {
                String requestId = extractRequestId(rawRequest);
                Response parseError = transportError(400, "Malformed transport payload", requestId);
                writeResponse(writer, parseError);
                EventLog.printlnWithStackTrace(
                        String.format(
                                "transport=tcp event=io-failure remote=%s error=\"%s\"",
                                remote,
                                parseException.getMessage()
                        ),
                        parseException
                );
                return;
            }

            String requestId = resolveRequestId(request);
            Response response;
            try {
                response = handler.handle(request);
                if (response == null) {
                    response = transportError(500, "Handler returned null response", requestId);
                }
            } catch (RuntimeException runtimeException) {
                EventLog.printlnWithStackTrace(
                        String.format(
                                "transport=tcp event=handler-failure remote=%s requestId=%s error=\"%s\"",
                                remote,
                                requestId,
                                runtimeException.getMessage()
                        ),
                        runtimeException
                );
                response = transportError(500, "Handler failure: " + runtimeException.getMessage(), requestId);
            }

            writeResponse(writer, response);
        } catch (IOException exception) {
            if (isBrokenPipe(exception)) {
                System.out.printf("transport=tcp event=client-disconnected remote=%s%n", remote);
            } else {
                EventLog.printlnWithStackTrace(
                        String.format("transport=tcp event=io-failure remote=%s error=\"%s\"", remote, exception.getMessage()),
                        exception
                );
            }
        } finally {
            System.out.printf("transport=tcp event=connection-close remote=%s%n", remote);
        }
    }

    private static boolean isBrokenPipe(IOException e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("Broken pipe") || msg.contains("Pipe quebrado")
                || msg.contains("Connection reset") || msg.contains("Software caused connection abort"));
    }

    private static void writeResponse(BufferedWriter writer, Response response) throws IOException {
        writer.write(OBJECT_MAPPER.writeValueAsString(response));
        writer.newLine();
        writer.flush();
    }

    private static String resolveRequestId(Request request) {
        if (request == null || request.requestId() == null || request.requestId().isBlank()) {
            return UUID.randomUUID().toString();
        }
        return request.requestId();
    }

    private static String extractRequestId(String rawRequest) {
        try {
            Map<String, Object> payload = OBJECT_MAPPER.readValue(rawRequest, Map.class);
            Object requestId = payload.get("requestId");
            if (requestId != null && !requestId.toString().isBlank()) {
                return requestId.toString();
            }
        } catch (IOException ignored) {
            // Return generated fallback when request body cannot be parsed.
        }
        return UUID.randomUUID().toString();
    }

    private static Response transportError(int statusCode, String message, String requestId) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("requestId", requestId);
        payload.put("message", message);
        return new Response(statusCode, message, payload);
    }
}
