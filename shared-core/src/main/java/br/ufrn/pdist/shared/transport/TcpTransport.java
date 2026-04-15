package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.Response;
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
import java.util.Map;

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
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    private void runServer(int port, String transportName, ServerSocketOperation operation) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.printf("transport=%s event=server-started port=%d%n", transportName, port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                Thread worker = new Thread(
                        () -> {
                            try {
                                clientSocket.setSoTimeout(READ_TIMEOUT_MILLIS);
                                operation.execute(clientSocket);
                            } catch (IOException exception) {
                                String remote = clientSocket.getRemoteSocketAddress().toString();
                                System.out.printf("transport=%s event=io-failure remote=%s error=\"%s\"%n",
                                        transportName, remote, exception.getMessage());
                            }
                        },
                        transportName + "-connection-" + clientSocket.getPort()
                );
                worker.setDaemon(true);
                worker.start();
            }
        } catch (IOException exception) {
            System.out.printf("transport=%s event=server-failure port=%d error=\"%s\"%n",
                    transportName, port, exception.getMessage());
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
                System.out.printf("transport=tcp event=io-failure remote=%s error=\"empty request\"%n", remote);
                return;
            }

            Request request = OBJECT_MAPPER.readValue(rawRequest, Request.class);
            Response response = handler.handle(request);

            writer.write(OBJECT_MAPPER.writeValueAsString(response));
            writer.newLine();
            writer.flush();
        } catch (IOException exception) {
            System.out.printf("transport=tcp event=io-failure remote=%s error=\"%s\"%n", remote, exception.getMessage());
        } finally {
            System.out.printf("transport=tcp event=connection-close remote=%s%n", remote);
        }
    }

    private static Response transportError(int statusCode, String message, String requestId) {
        return new Response(statusCode, message, Map.of("requestId", requestId));
    }
}
