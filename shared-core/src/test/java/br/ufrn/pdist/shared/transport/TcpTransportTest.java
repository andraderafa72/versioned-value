package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TcpTransportTest {

    @Test
    void shouldRoundTripARequestAndResponse() throws Exception {
        int port = findFreePort();
        TcpTransport transport = new TcpTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("echo", request.action())));

        Thread.sleep(150);

        Request request = new Request("r1", "ping", Map.of("k", "v"));
        Instance target = new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port);

        Response response = transport.send(request, target);

        assertEquals(200, response.statusCode());
        assertEquals("ok", response.message());
        assertEquals("ping", response.payload().get("echo"));
    }

    @Test
    void shouldHandleSimultaneousClientsWithoutDeadlock() throws Exception {
        int port = findFreePort();
        TcpTransport transport = new TcpTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("requestId", request.requestId())));

        Thread.sleep(150);

        int totalRequests = 20;
        CountDownLatch done = new CountDownLatch(totalRequests);
        AtomicInteger success = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Instance target = new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port);

        for (int i = 0; i < totalRequests; i++) {
            int idx = i;
            executor.submit(() -> {
                try {
                    Response response = transport.send(new Request("r" + idx, "ping", Map.of()), target);
                    if (response.statusCode() == 200) {
                        success.incrementAndGet();
                    }
                } finally {
                    done.countDown();
                }
            });
        }

        boolean completed = done.await(5, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue(completed);
        assertEquals(totalRequests, success.get());
    }

    @Test
    void shouldReturnTimeoutErrorWhenResponseIsPendingTooLong() throws Exception {
        int port = findFreePort();
        Thread slowServer = new Thread(() -> runSlowServer(port), "slow-server");
        slowServer.setDaemon(true);
        slowServer.start();

        Thread.sleep(150);

        TcpTransport transport = new TcpTransport();
        Request request = new Request("r-timeout", "slow", Map.of());
        Instance target = new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port);

        Response response = transport.send(request, target);

        assertEquals(504, response.statusCode());
        assertEquals("r-timeout", response.payload().get("requestId"));
    }

    private static void runSlowServer(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            try (Socket socket = serverSocket.accept();
                 BufferedReader reader = new BufferedReader(
                         new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                 Writer writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8)) {
                reader.readLine();
                Thread.sleep(2_500);
                writer.write("{\"statusCode\":200,\"message\":\"late\",\"payload\":{}}\n");
                writer.flush();
            }
        } catch (Exception ignored) {
            // Test helper server lifecycle is intentionally best-effort.
        }
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
