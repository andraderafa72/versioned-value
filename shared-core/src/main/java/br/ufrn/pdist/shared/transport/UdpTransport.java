package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.logging.EventLog;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class UdpTransport implements TransportLayer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int MAX_PACKET_BYTES = 16 * 1024;
    private static final int DEFAULT_TIMEOUT_MILLIS = 500;
    private static final int DEFAULT_RETRY_ATTEMPTS = 2;
    private static final String REQUEST_ID_KEY = "requestId";
    private static final String TIMEOUT_KEY = "timeout";
    private static final String RETRY_KEY = "retry";
    private final Map<String, Long> pendingRequests = new ConcurrentHashMap<>();

    @Override
    public void startServer(int port, RequestHandler handler) {
        Thread serverThread = new Thread(() -> runServer(port, handler), "udp-server-" + port);
        serverThread.setDaemon(true);
        serverThread.start();
    }

    @Override
    public Response send(Request request, Instance target) {
        Request normalizedRequest = normalizeRequest(request);
        String requestId = normalizedRequest.requestId();
        int timeoutMillis = readPositiveInt(normalizedRequest.payload().get(TIMEOUT_KEY), DEFAULT_TIMEOUT_MILLIS);
        int retryAttempts = readPositiveInt(normalizedRequest.payload().get(RETRY_KEY), DEFAULT_RETRY_ATTEMPTS);
        pendingRequests.put(requestId, System.currentTimeMillis());
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(timeoutMillis);
            InetAddress address = InetAddress.getByName(target.host());
            byte[] outbound = OBJECT_MAPPER.writeValueAsBytes(normalizedRequest);

            for (int attempt = 1; attempt <= retryAttempts; attempt++) {
                System.out.printf(
                        "transport=udp event=request-attempt requestId=%s attempt=%d maxAttempts=%d timeoutMs=%d host=%s port=%d%n",
                        requestId,
                        attempt,
                        retryAttempts,
                        timeoutMillis,
                        target.host(),
                        target.port()
                );
                DatagramPacket packet = new DatagramPacket(outbound, outbound.length, address, target.port());
                socket.send(packet);

                try {
                    Response response = awaitCorrelatedResponse(socket, requestId);
                    if (response != null) {
                        pendingRequests.remove(requestId);
                        return response;
                    }
                } catch (SocketTimeoutException timeoutException) {
                    System.out.printf(
                            "transport=udp event=request-timeout requestId=%s attempt=%d maxAttempts=%d timeoutMs=%d%n",
                            requestId,
                            attempt,
                            retryAttempts,
                            timeoutMillis
                    );
                }
            }
            pendingRequests.remove(requestId);
            return transportError(504, "Transport timeout after retry limit", requestId);
        } catch (JsonProcessingException jsonProcessingException) {
            pendingRequests.remove(requestId);
            return transportError(400, "Malformed transport payload", requestId);
        } catch (IOException ioException) {
            pendingRequests.remove(requestId);
            return transportError(502, "Transport I/O failure: " + ioException.getMessage(), requestId);
        }
    }

    private Response awaitCorrelatedResponse(DatagramSocket socket, String requestId) throws IOException {
        while (true) {
            DatagramPacket responsePacket = new DatagramPacket(new byte[MAX_PACKET_BYTES], MAX_PACKET_BYTES);
            socket.receive(responsePacket);
            Response response = decodeResponse(responsePacket);
            String responseRequestId = extractResponseRequestId(response, requestId);

            if (!requestId.equals(responseRequestId)) {
                boolean stillPending = pendingRequests.containsKey(responseRequestId);
                System.out.printf(
                        "transport=udp event=response-discarded requestId=%s reason=%s%n",
                        responseRequestId,
                        stillPending ? "for-other-pending-request" : "duplicate-or-late"
                );
                continue;
            }

            if (!pendingRequests.containsKey(requestId)) {
                System.out.printf(
                        "transport=udp event=response-discarded requestId=%s reason=already-completed%n",
                        requestId
                );
                continue;
            }
            return response;
        }
    }

    private void runServer(int port, RequestHandler handler) {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            System.out.printf("transport=udp event=server-started port=%d%n", port);
            while (true) {
                DatagramPacket packet = new DatagramPacket(new byte[MAX_PACKET_BYTES], MAX_PACKET_BYTES);
                socket.receive(packet);
                handleIncomingPacket(socket, packet, handler);
            }
        } catch (IOException exception) {
            EventLog.printlnWithStackTrace(
                    String.format(
                            "transport=udp event=server-failure port=%d error=\"%s\"",
                            port,
                            exception.getMessage()
                    ),
                    exception
            );
        }
    }

    private void handleIncomingPacket(DatagramSocket socket, DatagramPacket packet, RequestHandler handler) {
        String requestId = UUID.randomUUID().toString();
        try {
            String rawRequest = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);
            requestId = extractRequestId(rawRequest);
            Request request = OBJECT_MAPPER.readValue(rawRequest, Request.class);
            Request normalized = normalizeRequest(request);
            requestId = normalized.requestId();
            Response handled = handler.handle(normalized);
            if (handled == null) {
                handled = transportError(500, "Handler returned null response", requestId);
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            if (handled.payload() != null) {
                payload.putAll(handled.payload());
            }
            payload.putIfAbsent(REQUEST_ID_KEY, normalized.requestId());
            Response response = new Response(handled.statusCode(), handled.message(), payload);
            byte[] responsePayload = OBJECT_MAPPER.writeValueAsBytes(response);
            DatagramPacket responsePacket = new DatagramPacket(
                    responsePayload,
                    responsePayload.length,
                    packet.getAddress(),
                    packet.getPort()
            );
            socket.send(responsePacket);
            System.out.printf(
                    "transport=udp event=response-sent requestId=%s remote=%s:%d%n",
                    normalized.requestId(),
                    packet.getAddress().getHostAddress(),
                    packet.getPort()
            );
        } catch (Exception exception) {
            try {
                Response error = transportError(400, exception.getMessage() == null ? "Bad request" : exception.getMessage(), requestId);
                byte[] responsePayload = OBJECT_MAPPER.writeValueAsBytes(error);
                DatagramPacket responsePacket = new DatagramPacket(
                        responsePayload,
                        responsePayload.length,
                        packet.getAddress(),
                        packet.getPort()
                );
                socket.send(responsePacket);
                EventLog.printlnWithStackTrace(
                        String.format(
                                "transport=udp event=bad-request requestId=%s remote=%s:%d error=\"%s\"",
                                requestId,
                                packet.getAddress().getHostAddress(),
                                packet.getPort(),
                                exception.getMessage()
                        ),
                        exception
                );
            } catch (IOException ioException) {
                EventLog.printlnWithStackTrace(
                        String.format(
                                "transport=udp event=bad-request-response-failed requestId=%s error=\"%s\"",
                                requestId,
                                ioException.getMessage()
                        ),
                        ioException
                );
            }
        }
    }

    private static Request normalizeRequest(Request request) {
        Map<String, Object> payload = new LinkedHashMap<>();
        if (request.payload() != null) {
            payload.putAll(request.payload());
        }
        String requestId = request.requestId();
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString();
        }
        payload.putIfAbsent(REQUEST_ID_KEY, requestId);
        payload.putIfAbsent(TIMEOUT_KEY, DEFAULT_TIMEOUT_MILLIS);
        payload.putIfAbsent(RETRY_KEY, DEFAULT_RETRY_ATTEMPTS);
        return new Request(requestId, request.service(), request.action(), payload);
    }

    private static int readPositiveInt(Object value, int defaultValue) {
        if (value instanceof Number number) {
            int parsed = number.intValue();
            return parsed > 0 ? parsed : defaultValue;
        }
        if (value instanceof String raw) {
            try {
                int parsed = Integer.parseInt(raw);
                return parsed > 0 ? parsed : defaultValue;
            } catch (NumberFormatException ignored) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    private static Response decodeResponse(DatagramPacket packet) throws IOException {
        String raw = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);
        return OBJECT_MAPPER.readValue(raw, Response.class);
    }

    private static String extractResponseRequestId(Response response, String fallbackRequestId) {
        if (response == null || response.payload() == null) {
            return fallbackRequestId;
        }
        Object requestId = response.payload().get(REQUEST_ID_KEY);
        if (requestId == null || requestId.toString().isBlank()) {
            return fallbackRequestId;
        }
        return requestId.toString();
    }

    private static String extractRequestId(String rawRequest) {
        try {
            Map<String, Object> payload = OBJECT_MAPPER.readValue(rawRequest, Map.class);
            Object requestId = payload.get(REQUEST_ID_KEY);
            if (requestId != null && !requestId.toString().isBlank()) {
                return requestId.toString();
            }
        } catch (IOException ignored) {
            // Fallback generated by caller.
        }
        return UUID.randomUUID().toString();
    }

    private static Response transportError(int statusCode, String message, String requestId) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put(REQUEST_ID_KEY, requestId);
        payload.put("message", message);
        return new Response(statusCode, message, payload);
    }
}
