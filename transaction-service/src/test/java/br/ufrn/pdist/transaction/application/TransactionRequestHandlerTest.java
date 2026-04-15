package br.ufrn.pdist.transaction.application;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.transport.TransportLayer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransactionRequestHandlerTest {

    @Test
    void shouldDepositAndWithdrawByForwardingToAccountThroughGateway() {
        StubTransport transport = new StubTransport();
        TransactionRequestHandler handler = new TransactionRequestHandler(transport, gatewayInstance());

        Response deposit = handler.handle(new Request(
                "req-1",
                ServiceName.TRANSACTION,
                "deposit",
                gatewayPayload(Map.of("accountId", "acc-1", "amount", "15.00"))
        ));
        Response withdraw = handler.handle(new Request(
                "req-2",
                ServiceName.TRANSACTION,
                "withdraw",
                gatewayPayload(Map.of("accountId", "acc-1", "amount", "5.00"))
        ));

        assertEquals(200, deposit.statusCode());
        assertEquals(200, withdraw.statusCode());
        assertEquals("COMPLETED", deposit.payload().get("transactionStatus"));
        assertEquals("COMPLETED", withdraw.payload().get("transactionStatus"));
        assertEquals(2, transport.requests.size());
        assertEquals(ServiceName.ACCOUNT, transport.requests.get(0).service());
        assertEquals("credit", transport.requests.get(0).action());
        assertEquals("debit", transport.requests.get(1).action());
        assertEquals(gatewayInstance(), transport.targets.get(0));
        assertEquals(gatewayInstance(), transport.targets.get(1));
    }

    @Test
    void shouldReturnIdempotentResponseForSameRequestId() {
        StubTransport transport = new StubTransport();
        TransactionRequestHandler handler = new TransactionRequestHandler(transport, gatewayInstance());
        Request request = new Request(
                "dup-1",
                ServiceName.TRANSACTION,
                "deposit",
                gatewayPayload(Map.of("accountId", "acc-1", "amount", "9.00"))
        );

        Response first = handler.handle(request);
        Response second = handler.handle(request);

        assertEquals(first.statusCode(), second.statusCode());
        assertEquals(first.message(), second.message());
        assertEquals(first.payload(), second.payload());
        assertEquals("COMPLETED", first.payload().get("transactionStatus"));
        assertEquals(1, transport.requests.size());
    }

    @Test
    void shouldCompensateAndFailWhenTransferCreditFails() {
        StubTransport transport = new StubTransport();
        transport.responses = List.of(
                new Response(200, "balance debited", Map.of("balance", "70.00")),
                new Response(502, "service unavailable", Map.of("message", "service unavailable")),
                new Response(200, "balance credited", Map.of("balance", "100.00"))
        );
        TransactionRequestHandler handler = new TransactionRequestHandler(transport, gatewayInstance());

        Response transfer = handler.handle(new Request(
                "req-transfer-1",
                ServiceName.TRANSACTION,
                "transfer",
                gatewayPayload(Map.of(
                        "fromAccountId", "acc-1",
                        "toAccountId", "acc-2",
                        "amount", "30.00"
                ))
        ));

        assertEquals(502, transfer.statusCode());
        assertEquals("FAILED", transfer.payload().get("transactionStatus"));
        assertEquals(3, transport.requests.size());
        assertEquals("debit", transport.requests.get(0).action());
        assertEquals("credit", transport.requests.get(1).action());
        assertEquals("credit", transport.requests.get(2).action());
        assertEquals("acc-1", transport.requests.get(2).payload().get("accountId"));
    }

    @Test
    void shouldRejectRequestsWithoutGatewayForwarding() {
        StubTransport transport = new StubTransport();
        TransactionRequestHandler handler = new TransactionRequestHandler(transport, gatewayInstance());

        Response response = handler.handle(new Request(
                "req-3",
                ServiceName.TRANSACTION,
                "deposit",
                Map.of("accountId", "acc-1", "amount", "20.00")
        ));

        assertEquals(403, response.statusCode());
        assertEquals("FAILED", response.payload().get("transactionStatus"));
        assertEquals(0, transport.requests.size());
    }

    @Test
    void shouldProcessSameAccountRequestsSequentially() throws Exception {
        StubTransport transport = new StubTransport();
        transport.delayMs = 100;
        TransactionRequestHandler handler = new TransactionRequestHandler(transport, gatewayInstance());
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch doneSignal = new CountDownLatch(2);

        Runnable firstRequest = () -> {
            awaitLatch(startSignal);
            handler.handle(new Request(
                    "req-concurrent-1",
                    ServiceName.TRANSACTION,
                    "deposit",
                    gatewayPayload(Map.of("accountId", "acc-serial", "amount", "10.00"))
            ));
            doneSignal.countDown();
        };
        Runnable secondRequest = () -> {
            awaitLatch(startSignal);
            handler.handle(new Request(
                    "req-concurrent-2",
                    ServiceName.TRANSACTION,
                    "withdraw",
                    gatewayPayload(Map.of("accountId", "acc-serial", "amount", "4.00"))
            ));
            doneSignal.countDown();
        };

        Thread threadOne = new Thread(firstRequest);
        Thread threadTwo = new Thread(secondRequest);
        threadOne.start();
        threadTwo.start();
        startSignal.countDown();

        boolean completed = doneSignal.await(2, TimeUnit.SECONDS);
        assertEquals(true, completed);
        assertEquals(1, transport.maxParallelSends.get());
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
    }

    private static Instance gatewayInstance() {
        return new Instance("gateway-1", ServiceName.GATEWAY, "127.0.0.1", 8080);
    }

    private static Map<String, Object> gatewayPayload(Map<String, Object> body) {
        Map<String, Object> payload = new java.util.LinkedHashMap<>(body);
        payload.put("gatewayForwarded", true);
        payload.put("sourceService", ServiceName.GATEWAY.name());
        return Map.copyOf(payload);
    }

    private static final class StubTransport implements TransportLayer {
        private final List<Request> requests = new ArrayList<>();
        private final List<Instance> targets = new ArrayList<>();
        private List<Response> responses = List.of(
                new Response(200, "ok", Map.of("balance", "100.00"))
        );
        private int responseIndex = 0;
        private long delayMs = 0;
        private final AtomicInteger activeSends = new AtomicInteger(0);
        private final AtomicInteger maxParallelSends = new AtomicInteger(0);

        @Override
        public void startServer(int port, RequestHandler handler) {
            // No-op for unit tests.
        }

        @Override
        public Response send(Request request, Instance target) {
            requests.add(request);
            targets.add(target);
            int current = activeSends.incrementAndGet();
            maxParallelSends.accumulateAndGet(current, Math::max);
            try {
                sleep(delayMs);
                if (responses.isEmpty()) {
                    return new Response(500, "missing stub response", Map.of("message", "missing stub response"));
                }
                int currentIndex = Math.min(responseIndex, responses.size() - 1);
                responseIndex++;
                return responses.get(currentIndex);
            } finally {
                activeSends.decrementAndGet();
            }
        }

        private void sleep(long millis) {
            if (millis <= 0) {
                return;
            }
            try {
                Thread.sleep(millis);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(exception);
            }
        }
    }
}
