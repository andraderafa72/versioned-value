package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpTransportTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void shouldRoundTripPostRequestAndResponse() throws Exception {
        int port = findFreePort();
        HttpTransport transport = new HttpTransport();
        transport.startServer(port, request ->
                new Response(200, "ok", Map.of("echoAction", request.action(), "message", "ok")));

        Thread.sleep(150);

        Request request = new Request("h1", "POST /accounts", Map.of("requestId", "h1", "amount", 50));
        Instance target = new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port);

        Response response = transport.send(request, target);

        assertEquals(200, response.statusCode());
        assertEquals("ok", response.message());
        assertEquals("POST /accounts", response.payload().get("echoAction"));
        assertEquals("h1", response.payload().get("requestId"));
    }

    @Test
    void shouldAcceptExternalRawHttpGetRequest() throws Exception {
        int port = findFreePort();
        HttpTransport transport = new HttpTransport();
        transport.startServer(port, request ->
                new Response(200, "ok", Map.of("action", request.action(), "message", "ok")));

        Thread.sleep(150);

        try (Socket socket = new Socket("127.0.0.1", port);
             Writer writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            writer.write("GET /health HTTP/1.1\r\n");
            writer.write("Host: localhost\r\n");
            writer.write("Connection: close\r\n");
            writer.write("\r\n");
            writer.flush();

            String statusLine = reader.readLine();
            assertTrue(statusLine.startsWith("HTTP/1.1 200"));

            String line;
            int contentLength = 0;
            while (!(line = reader.readLine()).isEmpty()) {
                if (line.startsWith("Content-Length:")) {
                    contentLength = Integer.parseInt(line.substring("Content-Length:".length()).trim());
                }
            }

            char[] bodyChars = new char[contentLength];
            int read = reader.read(bodyChars);
            String body = new String(bodyChars, 0, read);
            Map<String, Object> payload = OBJECT_MAPPER.readValue(body, Map.class);
            assertEquals("GET /health", payload.get("action"));
        }
    }

    @Test
    void shouldReturnControlledErrorForInvalidRequest() throws Exception {
        int port = findFreePort();
        HttpTransport transport = new HttpTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("message", "ok")));

        Thread.sleep(150);

        try (Socket socket = new Socket("127.0.0.1", port);
             Writer writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            writer.write("NOT A VALID REQUEST\r\n");
            writer.write("\r\n");
            writer.flush();

            String statusLine = reader.readLine();
            assertTrue(statusLine.startsWith("HTTP/1.1 400"));
        }
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
