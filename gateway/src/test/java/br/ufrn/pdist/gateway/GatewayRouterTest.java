package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.transport.TransportLayer;
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
                serviceRegistry
        );

        Request request = new Request("r3", ServiceName.TRANSACTION, "POST /transactions", Map.of());
        Response response = router.route(request);

        assertEquals(503, response.statusCode());
        assertEquals("service unavailable", response.message());
        assertEquals("r3", response.payload().get("requestId"));
        assertEquals("TRANSACTION", response.payload().get("service"));
    }

    private static final class StubTransport implements TransportLayer {
        private Request lastRequest;
        private Instance lastTarget;
        private Response nextResponse = new Response(200, "forwarded", Map.of("ok", true));

        @Override
        public void startServer(int port, br.ufrn.pdist.shared.contracts.RequestHandler handler) {
            // No-op for unit tests.
        }

        @Override
        public Response send(Request request, Instance target) {
            this.lastRequest = request;
            this.lastTarget = target;
            return nextResponse;
        }
    }
}
