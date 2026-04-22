package br.ufrn.pdist.shared.http;

import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class BankingHttpRoutesTest {

    @Test
    void shouldMapDepositPathToTransaction() {
        Request http = new Request("rid-1", "POST " + BankingHttpRoutes.API_PREFIX + "/transactions/deposit", Map.of(
                "accountId", "a1",
                "amount", 5
        ));
        Request internal = BankingHttpRoutes.parseGatewayRequest(http);
        assertNotNull(internal);
        assertEquals(ServiceName.TRANSACTION, internal.service());
        assertEquals("deposit", internal.action());
        assertEquals("a1", internal.payload().get("accountId"));
    }

    @Test
    void shouldMapLegacyJmeterDepositPath() {
        Request http = new Request("rid-2", "POST /accounts/deposit", Map.of("accountId", "a1", "amount", 10));
        Request internal = BankingHttpRoutes.parseGatewayRequest(http);
        assertNotNull(internal);
        assertEquals(ServiceName.TRANSACTION, internal.service());
        assertEquals("deposit", internal.action());
    }

    @Test
    void shouldMapCreditPathToAccount() {
        Request http = new Request("rid-3", "POST " + BankingHttpRoutes.API_PREFIX + "/accounts/acc-9/credit", Map.of("amount", 3));
        Request internal = BankingHttpRoutes.parseGatewayRequest(http);
        assertNotNull(internal);
        assertEquals(ServiceName.ACCOUNT, internal.service());
        assertEquals("credit", internal.action());
        assertEquals("acc-9", internal.payload().get("accountId"));
    }

    @Test
    void shouldReturnNullForUnknownRoute() {
        Request http = new Request("rid-4", "POST /unknown", Map.of());
        assertNull(BankingHttpRoutes.parseGatewayRequest(http));
    }

    @Test
    void shouldBuildOutboundCreditPath() {
        Request internal = new Request(
                "r9",
                ServiceName.ACCOUNT,
                "credit",
                Map.of(
                        "accountId", "x1",
                        "amount", 2,
                        "gatewayForwarded", true,
                        "sourceService", "GATEWAY"
                )
        );
        BankingHttpRoutes.OutboundHttp out = BankingHttpRoutes.toOutboundRequest(internal);
        assertEquals("POST", out.method());
        assertEquals(BankingHttpRoutes.API_PREFIX + "/accounts/x1/credit", out.path());
        assertEquals(2, out.jsonBody().get("amount"));
        assertEquals(Boolean.TRUE, out.jsonBody().get("gatewayForwarded"));
    }
}
