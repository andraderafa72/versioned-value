package br.ufrn.pdist.account.application;

import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.math.BigDecimal;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AccountRequestHandlerTest {

    @Test
    void shouldCreateThenReadLatestBalance() {
        AccountRequestHandler handler = new AccountRequestHandler(new AccountVersioningService());

        Response create = handler.handle(new Request(
                "req-1",
                ServiceName.ACCOUNT,
                "createAccount",
                gatewayPayload(Map.of("accountId", "acc-1", "initialBalance", "10.00"))
        ));
        Response read = handler.handle(new Request(
                "req-2",
                ServiceName.ACCOUNT,
                "getBalance",
                gatewayPayload(Map.of("accountId", "acc-1"))
        ));

        assertEquals(201, create.statusCode());
        assertEquals(200, read.statusCode());
        assertEquals(new BigDecimal("10.00"), new BigDecimal(read.payload().get("balance").toString()));
    }

    @Test
    void shouldUpdateBalanceWithCreditAndDebit() {
        AccountRequestHandler handler = new AccountRequestHandler(new AccountVersioningService());
        handler.handle(new Request(
                "req-1",
                ServiceName.ACCOUNT,
                "createAccount",
                gatewayPayload(Map.of("accountId", "acc-1", "initialBalance", "20.00"))
        ));

        Response credit = handler.handle(new Request(
                "req-2",
                ServiceName.ACCOUNT,
                "credit",
                gatewayPayload(Map.of("accountId", "acc-1", "amount", "15.50"))
        ));
        Response debit = handler.handle(new Request(
                "req-3",
                ServiceName.ACCOUNT,
                "debit",
                gatewayPayload(Map.of("accountId", "acc-1", "amount", "5.50"))
        ));
        Response read = handler.handle(new Request(
                "req-4",
                ServiceName.ACCOUNT,
                "getBalance",
                gatewayPayload(Map.of("accountId", "acc-1"))
        ));

        assertEquals(200, credit.statusCode());
        assertEquals(200, debit.statusCode());
        assertEquals(new BigDecimal("30.00"), new BigDecimal(read.payload().get("balance").toString()));
    }

    @Test
    void shouldRejectRequestsWithoutGatewayForwarding() {
        AccountRequestHandler handler = new AccountRequestHandler(new AccountVersioningService());

        Response response = handler.handle(new Request(
                "req-1",
                ServiceName.ACCOUNT,
                "getBalance",
                Map.of("accountId", "acc-1")
        ));

        assertEquals(403, response.statusCode());
    }

    private static Map<String, Object> gatewayPayload(Map<String, Object> body) {
        Map<String, Object> payload = new java.util.LinkedHashMap<>(body);
        payload.put("gatewayForwarded", true);
        payload.put("sourceService", ServiceName.GATEWAY.name());
        return Map.copyOf(payload);
    }
}
