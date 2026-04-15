package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.transport.TransportLayer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayRouterTest {

    @Test
    void shouldRouteRequestToSelectedServiceInstance() {
        StubTransport transport = new StubTransport();
        GatewayServiceRegistry serviceRegistry = new GatewayServiceRegistry(
                Map.of(ServiceName.ACCOUNT, List.of(new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081)))
        );
        GatewayRouter router = new GatewayRouter(
                transport,
                serviceRegistry
        );

        Request request = new Request("r1", ServiceName.ACCOUNT, "POST /accounts", Map.of("amount", 100));
        Response response = router.route(request);

        assertEquals(200, response.statusCode());
        assertEquals("forwarded", response.message());
        assertEquals(ServiceName.ACCOUNT, transport.lastTarget.serviceName());
        assertEquals("r1", transport.lastRequest.requestId());
        assertEquals(ServiceName.ACCOUNT, transport.lastRequest.service());
        assertTrue(Boolean.TRUE.equals(transport.lastRequest.payload().get("gatewayForwarded")));
        assertEquals("GATEWAY", transport.lastRequest.payload().get("sourceService"));
    }

    @Test
    void shouldReturnDeterministicErrorForUnknownService() {
        StubTransport transport = new StubTransport();
        GatewayRouter router = new GatewayRouter(transport, new GatewayServiceRegistry(Map.of()));

        Request request = new Request("r2", ServiceName.TRANSACTION, "POST /transactions", Map.of());
        Response response = router.route(request);

        assertEquals(404, response.statusCode());
        assertEquals("service not found", response.message());
        assertEquals("r2", response.payload().get("requestId"));
        assertEquals("TRANSACTION", response.payload().get("service"));
    }

    @Test
    void shouldReturnServiceUnavailableWhenDownstreamFails() {
        StubTransport transport = new StubTransport();
        transport.nextResponse = new Response(504, "Transport timeout", Map.of("requestId", "r3"));
        GatewayServiceRegistry serviceRegistry = new GatewayServiceRegistry(
                Map.of(ServiceName.TRANSACTION, List.of(new Instance("tx-1", ServiceName.TRANSACTION, "127.0.0.1", 8082)))
        );
        GatewayRouter router = new GatewayRouter(
                transport,
                serviceRegistry,
                policy(1, 0L)
        );

        Request request = new Request("r3", ServiceName.TRANSACTION, "POST /transactions", Map.of());
        Response response = router.route(request);

        assertEquals(503, response.statusCode());
        assertEquals("service unavailable", response.message());
        assertEquals("r3", response.payload().get("requestId"));
        assertEquals("TRANSACTION", response.payload().get("service"));
    }

    @Test
    void shouldRouteConsecutiveRequestsUsingRoundRobinAcrossHealthyInstances() {
        StubTransport transport = new StubTransport();
        GatewayServiceRegistry serviceRegistry = new GatewayServiceRegistry(
                Map.of(
                        ServiceName.ACCOUNT,
                        List.of(
                                new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081),
                                new Instance("account-2", ServiceName.ACCOUNT, "127.0.0.1", 8082)
                        )
                )
        );
        GatewayRouter router = new GatewayRouter(transport, serviceRegistry);

        router.route(new Request("r4", ServiceName.ACCOUNT, "GET /accounts/a", Map.of()));
        String firstTargetId = transport.lastTarget.instanceId();

        router.route(new Request("r5", ServiceName.ACCOUNT, "GET /accounts/b", Map.of()));
        String secondTargetId = transport.lastTarget.instanceId();

        assertTrue(!firstTargetId.equals(secondTargetId));
    }

    @Test
    void shouldRetryTransientFailureUsingFailoverInstance() {
        StubTransport transport = new StubTransport();
        GatewayServiceRegistry serviceRegistry = new GatewayServiceRegistry(
                Map.of(
                        ServiceName.ACCOUNT,
                        List.of(
                                new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081),
                                new Instance("account-2", ServiceName.ACCOUNT, "127.0.0.1", 8082)
                        )
                )
        );
        transport.enqueueResponse("account-1", new Response(504, "Transport timeout", Map.of("requestId", "r6")));
        transport.enqueueResponse("account-2", new Response(200, "forwarded", Map.of("ok", true)));
        GatewayRouter router = new GatewayRouter(transport, serviceRegistry, policy(2, 0L));

        Response response = router.route(new Request("r6", ServiceName.ACCOUNT, "GET /accounts/a", Map.of()));

        assertEquals(200, response.statusCode());
        assertEquals(2, transport.targetsHistory.size());
        assertEquals("account-1", transport.targetsHistory.get(0).instanceId());
        assertEquals("account-2", transport.targetsHistory.get(1).instanceId());
    }

    @Test
    void shouldNotRetryBusiness4xxErrors() {
        StubTransport transport = new StubTransport();
        GatewayServiceRegistry serviceRegistry = new GatewayServiceRegistry(
                Map.of(
                        ServiceName.TRANSACTION,
                        List.of(
                                new Instance("tx-1", ServiceName.TRANSACTION, "127.0.0.1", 8083),
                                new Instance("tx-2", ServiceName.TRANSACTION, "127.0.0.1", 8084)
                        )
                )
        );
        transport.enqueueResponse("tx-1", new Response(422, "business rule violation", Map.of("requestId", "r7")));
        GatewayRouter router = new GatewayRouter(transport, serviceRegistry, policy(3, 0L));

        Response response = router.route(new Request("r7", ServiceName.TRANSACTION, "POST /transactions", Map.of()));

        assertEquals(422, response.statusCode());
        assertEquals("business rule violation", response.message());
        assertEquals(1, transport.targetsHistory.size());
    }

    @Test
    void shouldReturnDeterministicErrorAfterRetryExhaustion() {
        StubTransport transport = new StubTransport();
        GatewayServiceRegistry serviceRegistry = new GatewayServiceRegistry(
                Map.of(
                        ServiceName.ACCOUNT,
                        List.of(
                                new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081),
                                new Instance("account-2", ServiceName.ACCOUNT, "127.0.0.1", 8082)
                        )
                )
        );
        transport.enqueueResponse("account-1", new Response(504, "Transport timeout", Map.of("requestId", "r8")));
        transport.enqueueResponse("account-2", new Response(502, "Transport I/O failure", Map.of("requestId", "r8")));
        GatewayRouter router = new GatewayRouter(transport, serviceRegistry, policy(2, 0L));

        Response response = router.route(new Request("r8", ServiceName.ACCOUNT, "GET /accounts/a", Map.of()));

        assertEquals(503, response.statusCode());
        assertEquals("service unavailable", response.message());
        assertEquals("r8", response.payload().get("requestId"));
        assertEquals("ACCOUNT", response.payload().get("service"));
        assertEquals(2, transport.targetsHistory.size());
    }

    private static GatewayRetryPolicy policy(int maxAttempts, long backoffMillis) {
        return GatewayRetryPolicy.fromArgs(
                new String[]{
                        "--gateway.retry.max-attempts=" + maxAttempts,
                        "--gateway.retry.backoff-ms=" + backoffMillis
                }
        );
    }

    private static final class StubTransport implements TransportLayer {
        private Request lastRequest;
        private Instance lastTarget;
        private Response nextResponse = new Response(200, "forwarded", Map.of("ok", true));
        private final Map<String, List<Response>> scriptedResponses = new HashMap<>();
        private final List<Instance> targetsHistory = new ArrayList<>();

        @Override
        public void startServer(int port, br.ufrn.pdist.shared.contracts.RequestHandler handler) {
            // No-op for unit tests.
        }

        @Override
        public Response send(Request request, Instance target) {
            this.lastRequest = request;
            this.lastTarget = target;
            this.targetsHistory.add(target);
            List<Response> scripted = scriptedResponses.get(target.instanceId());
            if (scripted != null && !scripted.isEmpty()) {
                return scripted.remove(0);
            }
            return nextResponse;
        }

        private void enqueueResponse(String instanceId, Response response) {
            scriptedResponses.computeIfAbsent(instanceId, ignored -> new ArrayList<>()).add(response);
        }
    }
}
