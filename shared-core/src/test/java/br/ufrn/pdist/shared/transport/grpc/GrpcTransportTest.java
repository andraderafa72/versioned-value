package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcTransportTest {
    @Test
    void shouldRoundTripUnaryGrpcCall() throws Exception {
        int port = findFreePort();
        GrpcTransport transport = new GrpcTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("echoAction", request.action(), "requestId", request.requestId())));
        Thread.sleep(150);

        Response response = transport.send(
                new Request("g1", ServiceName.ACCOUNT, "POST /accounts", Map.of("amount", 50)),
                new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port)
        );

        assertEquals(200, response.statusCode());
        assertEquals("POST /accounts", response.payload().get("echoAction"));
        assertEquals("g1", response.payload().get("requestId"));
    }

    @Test
    void shouldHandleMultipleSimultaneousGrpcStreams() throws Exception {
        int port = findFreePort();
        GrpcTransport transport = new GrpcTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("requestId", request.requestId())));
        Thread.sleep(150);

        int total = 20;
        CountDownLatch done = new CountDownLatch(total);
        AtomicInteger success = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Instance target = new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port);

        for (int i = 0; i < total; i++) {
            int idx = i;
            executor.submit(() -> {
                try {
                    Response response = transport.send(new Request("g-" + idx, ServiceName.ACCOUNT, "POST /accounts", Map.of("n", idx)), target);
                    if (response.statusCode() == 200) {
                        success.incrementAndGet();
                    }
                } finally {
                    done.countDown();
                }
            });
        }

        assertTrue(done.await(6, TimeUnit.SECONDS));
        executor.shutdownNow();
        assertEquals(total, success.get());
    }

    @Test
    void shouldIncludeGrpcTrailersInResponseFrames() throws Exception {
        int port = findFreePort();
        GrpcTransport transport = new GrpcTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("requestId", request.requestId())));
        Thread.sleep(150);

        try (Socket socket = new Socket("127.0.0.1", port);
             BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());
             BufferedInputStream input = new BufferedInputStream(socket.getInputStream())) {
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(":method", "POST");
            headers.put(":path", "/pdist.Service/post./accounts");
            headers.put(":scheme", "http");
            headers.put("content-type", "application/grpc");
            headers.put("te", "trailers");

            byte[] message = ProtoCodec.encodeRequest(new Request("raw1", ServiceName.ACCOUNT, "POST /accounts", Map.of()));
            byte[] grpcBody = new byte[5 + message.length];
            grpcBody[0] = 0;
            System.arraycopy(ByteBuffer.allocate(4).putInt(message.length).array(), 0, grpcBody, 1, 4);
            System.arraycopy(message, 0, grpcBody, 5, message.length);

            Http2Wire.writeFrame(output, 1, Http2Wire.FRAME_HEADERS, Http2Wire.encodeHeaders(headers));
            Http2Wire.writeFrame(output, 1, Http2Wire.FRAME_DATA, grpcBody);
            output.flush();

            Http2Wire.readFrame(input);
            Http2Wire.readFrame(input);
            Http2Wire.Frame trailers = Http2Wire.readFrame(input);
            assertEquals(Http2Wire.FRAME_TRAILERS, trailers.type());
            assertEquals("0", Http2Wire.decodeHeaders(trailers.payload()).get("grpc-status"));
        }
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
