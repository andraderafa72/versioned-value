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

    @Override
    public void startServer(int port, RequestHandler handler) {
        Thread acceptThread = new Thread(() -> runServer(port, handler), "tcp-server-" + port);
        acceptThread.start();
    }

    @Override
    public Response send(Request request, Instance target) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(target.host(), target.port()), CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(READ_TIMEOUT_MILLIS);

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
        } catch (SocketTimeoutException exception) {
            return transportError(504, "Transport timeout", request.requestId());
        } catch (JsonProcessingException exception) {
            return transportError(400, "Malformed transport payload", request.requestId());
        } catch (IOException exception) {
            return transportError(502, "Transport I/O failure: " + exception.getMessage(), request.requestId());
        }
    }

    private void runServer(int port, RequestHandler handler) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.printf("transport=tcp event=server-started port=%d%n", port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                Thread worker = new Thread(
                        () -> handleConnection(clientSocket, handler),
                        "tcp-connection-" + clientSocket.getPort()
                );
                worker.setDaemon(true);
                worker.start();
            }
        } catch (IOException exception) {
            System.out.printf("transport=tcp event=server-failure port=%d error=\"%s\"%n", port, exception.getMessage());
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
