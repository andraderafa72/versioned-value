package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.net.ServerSocket;
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
    void shouldReturnTransportErrorWhenServerUnavailable() throws Exception {
        int port = findFreePort();
        GrpcTransport transport = new GrpcTransport();
        Response response = transport.send(
                new Request("g-error", ServiceName.ACCOUNT, "POST /accounts", Map.of()),
                new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port)
        );

        assertEquals(502, response.statusCode());
        assertTrue(response.payload().containsKey("grpc-status"));
    }

    @Test
    void shouldReturnStructuredErrorPayloadWhenHandlerThrows() throws Exception {
        int port = findFreePort();
        GrpcTransport transport = new GrpcTransport();
        transport.startServer(port, request -> {
            throw new IllegalStateException("boom");
        });
        Thread.sleep(150);

        Response response = transport.send(
                new Request("g2", ServiceName.ACCOUNT, "POST /accounts", Map.of()),
                new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port)
        );

        assertEquals(500, response.statusCode());
        assertEquals("g2", response.payload().get("requestId"));
        assertTrue(response.message().contains("Handler failure"));
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
