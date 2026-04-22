package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.http.BankingHttpRoutes;
import br.ufrn.pdist.shared.transport.TransportLayer;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GatewayHttpInboundHandlerTest {

    @Test
    void shouldRouteHttpMappedDepositToTransaction() {
        StubTransport transport = new StubTransport();
        GatewayServiceRegistry registry = new GatewayServiceRegistry(
                Map.of(ServiceName.TRANSACTION, List.of(new Instance("tx-1", ServiceName.TRANSACTION, "127.0.0.1", 8083)))
        );
        GatewayRouter router = new GatewayRouter(transport, registry, GatewayRetryPolicy.defaults());
        Request http = new Request(
                "http-1",
                "POST " + BankingHttpRoutes.API_PREFIX + "/transactions/deposit",
                Map.of("accountId", "a1", "amount", 1)
        );
        Response response = GatewayHttpInboundHandler.handle(router, http);
        assertEquals(200, response.statusCode());
        assertNotNull(transport.lastForwarded);
        assertEquals(ServiceName.TRANSACTION, transport.lastForwarded.service());
        assertEquals("deposit", transport.lastForwarded.action());
    }

    private static final class StubTransport implements TransportLayer {
        Request lastForwarded;

        @Override
        public void startServer(int port, br.ufrn.pdist.shared.contracts.RequestHandler handler) {
        }

        @Override
        public Response send(Request request, Instance target) {
            this.lastForwarded = request;
            return new Response(200, "ok", Map.of("message", "ok"));
        }
    }
}
