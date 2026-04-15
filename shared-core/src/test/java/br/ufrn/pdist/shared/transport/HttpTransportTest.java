package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HttpTransportTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void shouldRoundTripPostRequestAndResponse() throws Exception {
        int port = findFreePort();
        HttpTransport transport = new HttpTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("echoAction", request.action(), "message", "ok")));
        Thread.sleep(150);

        Response response = transport.send(
                new Request("h1", "POST /accounts", Map.of("requestId", "h1", "amount", 50)),
                new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port)
        );

        assertEquals(200, response.statusCode());
        assertEquals("POST /accounts", response.payload().get("echoAction"));
        assertEquals("h1", response.payload().get("requestId"));
    }

    @Test
    void shouldAcceptRawHttp2FramesFromExternalClient() throws Exception {
        int port = findFreePort();
        HttpTransport transport = new HttpTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("action", request.action(), "message", "ok")));
        Thread.sleep(150);

        try (Socket socket = new Socket("127.0.0.1", port);
             BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());
             BufferedInputStream input = new BufferedInputStream(socket.getInputStream())) {
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(":method", "GET");
            headers.put(":path", "/health");
            headers.put("content-type", "application/json");

            Http2Wire.writeFrame(output, 1, Http2Wire.FRAME_HEADERS, Http2Wire.encodeHeaders(headers));
            Http2Wire.writeFrame(output, 1, Http2Wire.FRAME_DATA, "{}".getBytes());
            output.flush();

            Http2Wire.Frame h = Http2Wire.readFrame(input);
            Http2Wire.Frame d = Http2Wire.readFrame(input);
            assertEquals("200", Http2Wire.decodeHeaders(h.payload()).get(":status"));
            assertEquals("GET /health", OBJECT_MAPPER.readValue(d.payload(), Map.class).get("action"));
        }
    }

    @Test
    void shouldReturnControlledErrorForInvalidFrameSequence() throws Exception {
        int port = findFreePort();
        HttpTransport transport = new HttpTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("message", "ok")));
        Thread.sleep(150);

        try (Socket socket = new Socket("127.0.0.1", port);
             BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());
             BufferedInputStream input = new BufferedInputStream(socket.getInputStream())) {
            Http2Wire.writeFrame(output, 1, Http2Wire.FRAME_DATA, "invalid".getBytes());
            output.flush();
            Http2Wire.Frame h = Http2Wire.readFrame(input);
            assertEquals("400", Http2Wire.decodeHeaders(h.payload()).get(":status"));
        }
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
