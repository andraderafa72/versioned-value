package br.ufrn.pdist.account.application;

import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DistributedAccountStateTest {

    @Test
    void shouldShareAccountStateAcrossHandlerInstances() {
        InMemoryAccountStateBackend sharedBackend = new InMemoryAccountStateBackend();
        AccountRequestHandler firstInstance = new AccountRequestHandler(new AccountVersioningService(sharedBackend));
        AccountRequestHandler secondInstance = new AccountRequestHandler(new AccountVersioningService(sharedBackend));

        Response created = firstInstance.handle(new Request(
                "req-create",
                ServiceName.ACCOUNT,
                "createAccount",
                gatewayPayload(Map.of("accountId", "acc-shared", "initialBalance", "100.00"))
        ));
        Response debited = secondInstance.handle(new Request(
                "req-debit",
                ServiceName.ACCOUNT,
                "debit",
                gatewayPayload(Map.of("accountId", "acc-shared", "amount", "30.00"))
        ));
        Response credited = firstInstance.handle(new Request(
                "req-credit",
                ServiceName.ACCOUNT,
                "credit",
                gatewayPayload(Map.of("accountId", "acc-shared", "amount", "10.00"))
        ));
        Response read = secondInstance.handle(new Request(
                "req-read",
                ServiceName.ACCOUNT,
                "getBalance",
                gatewayPayload(Map.of("accountId", "acc-shared"))
        ));

        assertEquals(201, created.statusCode());
        assertEquals(200, debited.statusCode());
        assertEquals(200, credited.statusCode());
        assertEquals(200, read.statusCode());
        assertEquals(new BigDecimal("80.00"), new BigDecimal(read.payload().get("balance").toString()));
    }

    private static Map<String, Object> gatewayPayload(Map<String, Object> body) {
        Map<String, Object> payload = new LinkedHashMap<>(body);
        payload.put("gatewayForwarded", true);
        payload.put("sourceService", ServiceName.GATEWAY.name());
        return Map.copyOf(payload);
    }
}
