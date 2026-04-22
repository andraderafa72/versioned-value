package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HttpTransportTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void shouldRoundTripPostRequestAndResponse() throws Exception {
        int port = findFreePort();
        HttpTransport transport = new HttpTransport();
        transport.startServer(port, request -> new Response(200, "ok",
                Map.of("echoAction", request.action(), "message", "ok")));
        Thread.sleep(200);

        Response response = transport.send(
                new Request("h1", "POST /accounts", Map.of("requestId", "h1", "amount", 50)),
                new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port)
        );

        assertEquals(200, response.statusCode());
        assertEquals("POST /accounts", response.payload().get("echoAction"));
        assertEquals("h1", response.payload().get("requestId"));
    }

    @Test
    void shouldRoundTripRawGetRequest() throws Exception {
        int port = findFreePort();
        HttpTransport transport = new HttpTransport();
        transport.startServer(port, request -> new Response(200, "ok",
                Map.of("action", request.action(), "message", "ok")));
        Thread.sleep(200);

        try (Socket socket = new Socket("127.0.0.1", port);
             OutputStream out = socket.getOutputStream();
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

            PrintWriter writer = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), false);
            writer.printf("GET /health HTTP/1.1\r\n");
            writer.printf("Host: 127.0.0.1:%d\r\n", port);
            writer.printf("Content-Length: 0\r\n");
            writer.printf("Connection: close\r\n");
            writer.printf("\r\n");
            writer.flush();

            String statusLine = reader.readLine();
            assertEquals("HTTP/1.1 200 OK", statusLine);

            int contentLength = 0;
            String line;
            while ((line = reader.readLine()) != null && !line.isEmpty()) {
                if (line.toLowerCase().startsWith("content-length:")) {
                    contentLength = Integer.parseInt(line.substring(15).trim());
                }
            }
            char[] buf = new char[contentLength];
            reader.read(buf, 0, contentLength);
            Map<?, ?> body = OBJECT_MAPPER.readValue(new String(buf), Map.class);
            assertEquals("GET /health", body.get("action"));
        }
    }

    @Test
    void shouldReturn400OnInvalidJsonBody() throws Exception {
        int port = findFreePort();
        HttpTransport transport = new HttpTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("message", "ok")));
        Thread.sleep(200);

        byte[] badBody = "not-json".getBytes(StandardCharsets.UTF_8);
        try (Socket socket = new Socket("127.0.0.1", port);
             OutputStream out = socket.getOutputStream();
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

            PrintWriter writer = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), false);
            writer.printf("POST /accounts HTTP/1.1\r\n");
            writer.printf("Host: 127.0.0.1:%d\r\n", port);
            writer.printf("Content-Type: application/json\r\n");
            writer.printf("Content-Length: %d\r\n", badBody.length);
            writer.printf("Connection: close\r\n");
            writer.printf("\r\n");
            writer.flush();
            out.write(badBody);
            out.flush();

            String statusLine = reader.readLine();
            assertEquals("HTTP/1.1 400 Bad Request", statusLine);
        }
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
