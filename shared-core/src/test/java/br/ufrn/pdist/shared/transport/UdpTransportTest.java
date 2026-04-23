package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UdpTransportTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void shouldRoundTripARequestAndResponse() throws Exception {
        int port = findFreeUdpPort();
        UdpTransport transport = new UdpTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("requestId", request.requestId(), "echo", request.action())));

        Thread.sleep(150);

        Request request = new Request("u1", "ping", Map.of("timeout", 300, "retry", 2));
        Instance target = new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port);
        Response response = transport.send(request, target);

        assertEquals(200, response.statusCode());
        assertEquals("ok", response.message());
        assertEquals("u1", response.payload().get("requestId"));
        assertEquals("ping", response.payload().get("echo"));
    }

    @Test
    void shouldRecoverFromSinglePacketLossUsingRetry() throws Exception {
        int port = findFreeUdpPort();
        CountDownLatch serverReady = new CountDownLatch(1);
        AtomicBoolean firstPacketDropped = new AtomicBoolean(false);
        Thread serverThread = new Thread(() -> runDropFirstPacketServer(port, serverReady, firstPacketDropped), "udp-drop-first");
        serverThread.setDaemon(true);
        serverThread.start();
        assertTrue(serverReady.await(1, TimeUnit.SECONDS));

        UdpTransport transport = new UdpTransport();
        Request request = new Request("u2", "retry-me", Map.of("timeout", 200, "retry", 3));
        Instance target = new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port);
        Response response = transport.send(request, target);

        assertEquals(200, response.statusCode());
        assertEquals("recovered", response.message());
        assertTrue(firstPacketDropped.get());
        assertEquals("u2", response.payload().get("requestId"));
    }

    @Test
    void shouldReturnTimeoutAfterRetryLimitWithoutResponse() throws Exception {
        int port = findFreeUdpPort();
        CountDownLatch serverReady = new CountDownLatch(1);
        Thread blackholeServer = new Thread(() -> runBlackholeServer(port, serverReady), "udp-blackhole");
        blackholeServer.setDaemon(true);
        blackholeServer.start();
        assertTrue(serverReady.await(1, TimeUnit.SECONDS));

        UdpTransport transport = new UdpTransport();
        Request request = new Request("u3", "never-respond", Map.of("timeout", 120, "retry", 2));
        Instance target = new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port);
        Response response = transport.send(request, target);

        assertEquals(504, response.statusCode());
        assertEquals("u3", response.payload().get("requestId"));
    }

    @Test
    void shouldDiscardDuplicateOrLateResponseAndKeepCorrelatedOne() throws Exception {
        int port = findFreeUdpPort();
        CountDownLatch serverReady = new CountDownLatch(1);
        Thread duplicateServer = new Thread(() -> runDuplicateResponseServer(port, serverReady), "udp-duplicate-response");
        duplicateServer.setDaemon(true);
        duplicateServer.start();
        assertTrue(serverReady.await(1, TimeUnit.SECONDS));

        UdpTransport transport = new UdpTransport();
        Request request = new Request("u4", "expect-second", Map.of("timeout", 300, "retry", 2));
        Instance target = new Instance("i1", ServiceName.ACCOUNT, "127.0.0.1", port);
        Response response = transport.send(request, target);

        assertEquals(200, response.statusCode());
        assertEquals("final-response", response.message());
        assertEquals("u4", response.payload().get("requestId"));
    }

    @Test
    void shouldAcceptTrailingWhitespaceAndUnknownRootFieldsOnUdpJson() throws Exception {
        int port = findFreeUdpPort();
        UdpTransport transport = new UdpTransport();
        transport.startServer(port, request -> new Response(
                200,
                "ok",
                Map.of("requestId", request.requestId(), "service", request.service().name())
        ));
        Thread.sleep(150);

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(2_000);
            String json = "{\"requestId\":\"udp-edge\",\"service\":\"ACCOUNT\",\"action\":\"ping\",\"payload\":{},\"clientMeta\":{\"tool\":\"jmeter\"}}\n\r ";
            byte[] rawRequest = json.getBytes(StandardCharsets.UTF_8);
            DatagramPacket requestPacket = new DatagramPacket(
                    rawRequest,
                    rawRequest.length,
                    InetAddress.getByName("127.0.0.1"),
                    port
            );
            socket.send(requestPacket);

            DatagramPacket responsePacket = new DatagramPacket(new byte[16 * 1024], 16 * 1024);
            socket.receive(responsePacket);
            Response response = OBJECT_MAPPER.readValue(
                    new String(
                            responsePacket.getData(),
                            responsePacket.getOffset(),
                            responsePacket.getLength(),
                            StandardCharsets.UTF_8
                    ),
                    Response.class
            );
            assertEquals(200, response.statusCode());
            assertEquals("udp-edge", response.payload().get("requestId"));
            assertEquals("ACCOUNT", response.payload().get("service"));
        }
    }

    @Test
    void shouldReturnErrorDatagramWhenIncomingPayloadIsMalformed() throws Exception {
        int port = findFreeUdpPort();
        UdpTransport transport = new UdpTransport();
        transport.startServer(port, request -> new Response(200, "ok", Map.of("requestId", request.requestId())));
        Thread.sleep(150);

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(1_000);
            byte[] rawRequest = "{\"requestId\":\"u-bad\"".getBytes(StandardCharsets.UTF_8);
            DatagramPacket requestPacket = new DatagramPacket(
                    rawRequest,
                    rawRequest.length,
                    InetAddress.getByName("127.0.0.1"),
                    port
            );
            socket.send(requestPacket);

            DatagramPacket responsePacket = new DatagramPacket(new byte[16 * 1024], 16 * 1024);
            socket.receive(responsePacket);
            Response response = OBJECT_MAPPER.readValue(
                    new String(
                            responsePacket.getData(),
                            responsePacket.getOffset(),
                            responsePacket.getLength(),
                            StandardCharsets.UTF_8
                    ),
                    Response.class
            );
            assertEquals(400, response.statusCode());
            assertTrue(response.payload().containsKey("requestId"));
            assertTrue(response.payload().containsKey("message"));
        }
    }

    private static void runDropFirstPacketServer(int port, CountDownLatch serverReady, AtomicBoolean firstPacketDropped) {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            serverReady.countDown();
            byte[] buffer = new byte[16 * 1024];
            while (true) {
                DatagramPacket requestPacket = new DatagramPacket(buffer, buffer.length);
                socket.receive(requestPacket);
                Request request = OBJECT_MAPPER.readValue(
                        new String(requestPacket.getData(), requestPacket.getOffset(), requestPacket.getLength(), StandardCharsets.UTF_8),
                        Request.class
                );
                if (firstPacketDropped.compareAndSet(false, true)) {
                    continue;
                }
                Response response = new Response(200, "recovered", Map.of("requestId", request.requestId()));
                sendResponse(socket, requestPacket, response);
                return;
            }
        } catch (Exception ignored) {
            // Best-effort test helper.
        }
    }

    private static void runBlackholeServer(int port, CountDownLatch serverReady) {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            serverReady.countDown();
            byte[] buffer = new byte[16 * 1024];
            long end = System.currentTimeMillis() + 1_500;
            while (System.currentTimeMillis() < end) {
                DatagramPacket requestPacket = new DatagramPacket(buffer, buffer.length);
                socket.receive(requestPacket);
            }
        } catch (Exception ignored) {
            // Best-effort test helper.
        }
    }

    private static void runDuplicateResponseServer(int port, CountDownLatch serverReady) {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            serverReady.countDown();
            byte[] buffer = new byte[16 * 1024];
            DatagramPacket requestPacket = new DatagramPacket(buffer, buffer.length);
            socket.receive(requestPacket);
            Request request = OBJECT_MAPPER.readValue(
                    new String(requestPacket.getData(), requestPacket.getOffset(), requestPacket.getLength(), StandardCharsets.UTF_8),
                    Request.class
            );

            Response wrong = new Response(200, "wrong-correlation", Map.of("requestId", "other-request"));
            sendResponse(socket, requestPacket, wrong);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("requestId", request.requestId());
            Response correct = new Response(200, "final-response", payload);
            sendResponse(socket, requestPacket, correct);
        } catch (Exception ignored) {
            // Best-effort test helper.
        }
    }

    private static void sendResponse(DatagramSocket socket, DatagramPacket sourcePacket, Response response) throws Exception {
        byte[] payload = OBJECT_MAPPER.writeValueAsBytes(response);
        DatagramPacket responsePacket = new DatagramPacket(
                payload,
                payload.length,
                sourcePacket.getAddress(),
                sourcePacket.getPort()
        );
        socket.send(responsePacket);
    }

    private static int findFreeUdpPort() throws Exception {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
