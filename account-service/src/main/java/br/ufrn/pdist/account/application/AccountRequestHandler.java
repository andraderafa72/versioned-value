package br.ufrn.pdist.account.application;

import br.ufrn.pdist.account.domain.Account;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import org.springframework.stereotype.Component;

@Component
public class AccountRequestHandler implements RequestHandler {

    private static final String ACTION_CREATE = "createAccount";
    private static final String ACTION_GET_BALANCE = "getBalance";
    private static final String ACTION_CREDIT = "credit";
    private static final String ACTION_DEBIT = "debit";
    private final AccountVersioningService accountService;

    public AccountRequestHandler(AccountVersioningService accountService) {
        this.accountService = accountService;
    }

    @Override
    public Response handle(Request request) {
        if (!isGatewayForwarded(request.payload())) {
            return error(403, request.requestId(), "direct communication blocked; use gateway");
        }
        if (request.action() == null || request.action().isBlank()) {
            return error(400, request.requestId(), "action is required");
        }

        try {
            return switch (request.action()) {
                case ACTION_CREATE -> handleCreate(request);
                case ACTION_GET_BALANCE -> handleGetBalance(request);
                case ACTION_CREDIT -> handleCredit(request);
                case ACTION_DEBIT -> handleDebit(request);
                default -> error(400, request.requestId(), "unsupported action: " + request.action());
            };
        } catch (IllegalArgumentException | NoSuchElementException exception) {
            return error(400, request.requestId(), exception.getMessage());
        }
    }

    private Response handleCreate(Request request) {
        Map<String, Object> payload = payloadOrEmpty(request.payload());
        String accountId = requiredString(payload, "accountId");
        BigDecimal initialBalance = requiredDecimal(payload, "initialBalance");
        if (initialBalance.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("initialBalance must be >= 0");
        }
        Account account = accountService.createAccount(accountId, initialBalance);
        System.out.printf("event=account-created accountId=%s balance=%s%n", account.id(), account.latestBalance());
        return success(201, request.requestId(), "account created", account.id(), account.latestBalance());
    }

    private Response handleGetBalance(Request request) {
        Map<String, Object> payload = payloadOrEmpty(request.payload());
        String accountId = requiredString(payload, "accountId");
        BigDecimal balance = accountService.getCurrentBalance(accountId);
        return success(200, request.requestId(), "balance retrieved", accountId, balance);
    }

    private Response handleCredit(Request request) {
        Map<String, Object> payload = payloadOrEmpty(request.payload());
        String accountId = requiredString(payload, "accountId");
        BigDecimal amount = requiredDecimal(payload, "amount");
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("amount must be > 0");
        }
        BigDecimal currentBalance = accountService.getCurrentBalance(accountId);
        BigDecimal updatedBalance = currentBalance.add(amount);
        accountService.addBalanceVersion(accountId, updatedBalance);
        return success(200, request.requestId(), "balance credited", accountId, updatedBalance);
    }

    private Response handleDebit(Request request) {
        Map<String, Object> payload = payloadOrEmpty(request.payload());
        String accountId = requiredString(payload, "accountId");
        BigDecimal amount = requiredDecimal(payload, "amount");
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("amount must be > 0");
        }
        BigDecimal currentBalance = accountService.getCurrentBalance(accountId);
        if (currentBalance.compareTo(amount) < 0) {
            throw new IllegalArgumentException("insufficient funds");
        }
        BigDecimal updatedBalance = currentBalance.subtract(amount);
        accountService.addBalanceVersion(accountId, updatedBalance);
        return success(200, request.requestId(), "balance debited", accountId, updatedBalance);
    }

    private static Map<String, Object> payloadOrEmpty(Map<String, Object> payload) {
        if (payload == null) {
            return Map.of();
        }
        return payload;
    }

    private static String requiredString(Map<String, Object> payload, String fieldName) {
        Object value = payload.get(fieldName);
        if (value == null || value.toString().isBlank()) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        return value.toString();
    }

    private static BigDecimal requiredDecimal(Map<String, Object> payload, String fieldName) {
        Object value = payload.get(fieldName);
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        try {
            if (value instanceof Number number) {
                return new BigDecimal(number.toString());
            }
            return new BigDecimal(value.toString());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(fieldName + " must be numeric");
        }
    }

    private static boolean isGatewayForwarded(Map<String, Object> payload) {
        if (payload == null) {
            return false;
        }
        Object gatewayForwarded = payload.get("gatewayForwarded");
        Object sourceService = payload.get("sourceService");
        return Boolean.TRUE.equals(gatewayForwarded) && ServiceName.GATEWAY.name().equals(sourceService);
    }

    private static Response success(
            int statusCode,
            String requestId,
            String message,
            String accountId,
            BigDecimal balance
    ) {
        Map<String, Object> responsePayload = new LinkedHashMap<>();
        responsePayload.put("requestId", requestId);
        responsePayload.put("accountId", accountId);
        responsePayload.put("balance", balance);
        responsePayload.put("message", message);
        return new Response(statusCode, message, Map.copyOf(responsePayload));
    }

    private static Response error(int statusCode, String requestId, String message) {
        Map<String, Object> responsePayload = new LinkedHashMap<>();
        responsePayload.put("requestId", requestId);
        responsePayload.put("message", message);
        return new Response(statusCode, message, Map.copyOf(responsePayload));
    }
}
