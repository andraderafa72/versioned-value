package br.ufrn.pdist.transaction.application;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.model.VersionedValue;
import br.ufrn.pdist.shared.transport.TransportLayer;
import br.ufrn.pdist.transaction.domain.TransactionState;
import br.ufrn.pdist.transaction.domain.TransactionStatus;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TransactionRequestHandler {

    private static final String ACTION_DEPOSIT = "deposit";
    private static final String ACTION_WITHDRAW = "withdraw";
    private static final String ACTION_TRANSFER = "transfer";
    private static final String ACTION_ACCOUNT_CREDIT = "credit";
    private static final String ACTION_ACCOUNT_DEBIT = "debit";
    private final TransportLayer transport;
    private final Instance gatewayInstance;
    private final TransactionVersioningService transactionVersioningService;
    private final AccountLockManager accountLockManager;
    private final ConcurrentMap<String, Response> responsesByRequestId = new ConcurrentHashMap<>();

    public TransactionRequestHandler(TransportLayer transport, Instance gatewayInstance) {
        this(transport, gatewayInstance, new TransactionVersioningService(), new AccountLockManager());
    }

    TransactionRequestHandler(
            TransportLayer transport,
            Instance gatewayInstance,
            TransactionVersioningService transactionVersioningService,
            AccountLockManager accountLockManager
    ) {
        this.transport = transport;
        this.gatewayInstance = gatewayInstance;
        this.transactionVersioningService = transactionVersioningService;
        this.accountLockManager = accountLockManager;
    }

    public Response handle(Request request) {
        Response cached = responsesByRequestId.get(request.requestId());
        if (cached != null) {
            System.out.printf(
                    "event=transaction-finish requestId=%s status=%d action=%s idempotentReplay=true%n",
                    request.requestId(),
                    cached.statusCode(),
                    request.action()
            );
            return cached;
        }

        System.out.printf("event=transaction-start requestId=%s action=%s%n", request.requestId(), request.action());
        appendState(request, TransactionStatus.STARTED, "request accepted");
        Response response;
        try {
            if (!isGatewayForwarded(request.payload())) {
                response = failure(request, 403, "direct communication blocked; use gateway");
            } else if (request.action() == null || request.action().isBlank()) {
                response = failure(request, 400, "action is required");
            } else {
                List<String> lockedAccounts = accountsToLock(request.action(), payloadOrEmpty(request.payload()));
                try (AccountLockManager.LockedAccounts ignored = accountLockManager.lockAll(lockedAccounts)) {
                    response = switch (request.action()) {
                        case ACTION_DEPOSIT -> handleDeposit(request);
                        case ACTION_WITHDRAW -> handleWithdraw(request);
                        case ACTION_TRANSFER -> handleTransfer(request);
                        default -> failure(request, 400, "unsupported action: " + request.action());
                    };
                }
            }
        } catch (IllegalArgumentException | NoSuchElementException exception) {
            response = failure(request, 400, exception.getMessage());
        } catch (Exception exception) {
            response = failure(request, 500, "transaction processing failed: " + exception.getMessage());
        }

        responsesByRequestId.putIfAbsent(request.requestId(), response);
        System.out.printf(
                "event=transaction-finish requestId=%s status=%d action=%s message=\"%s\"%n",
                request.requestId(),
                response.statusCode(),
                request.action(),
                response.message()
        );
        return response;
    }

    private Response handleDeposit(Request request) {
        Map<String, Object> payload = payloadOrEmpty(request.payload());
        String accountId = requiredString(payload, "accountId");
        BigDecimal amount = requiredDecimal(payload, "amount");
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("amount must be > 0");
        }

        Response credit = sendAccountAction(request.requestId(), ACTION_ACCOUNT_CREDIT, Map.of(
                "accountId", accountId,
                "amount", amount
        ));
        if (credit.statusCode() >= 400) {
            return failure(request, credit.statusCode(), credit.message());
        }
        appendState(request, TransactionStatus.CREDIT_APPLIED, "deposit credit applied");
        return success(request, 200, "deposit completed", Map.of(
                "accountId", accountId,
                "balance", credit.payload().get("balance")
        ));
    }

    private Response handleWithdraw(Request request) {
        Map<String, Object> payload = payloadOrEmpty(request.payload());
        String accountId = requiredString(payload, "accountId");
        BigDecimal amount = requiredDecimal(payload, "amount");
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("amount must be > 0");
        }

        Response debit = sendAccountAction(request.requestId(), ACTION_ACCOUNT_DEBIT, Map.of(
                "accountId", accountId,
                "amount", amount
        ));
        if (debit.statusCode() >= 400) {
            return failure(request, debit.statusCode(), debit.message());
        }
        appendState(request, TransactionStatus.DEBIT_APPLIED, "withdraw debit applied");
        return success(request, 200, "withdraw completed", Map.of(
                "accountId", accountId,
                "balance", debit.payload().get("balance")
        ));
    }

    private Response handleTransfer(Request request) {
        Map<String, Object> payload = payloadOrEmpty(request.payload());
        String fromAccountId = requiredString(payload, "fromAccountId");
        String toAccountId = requiredString(payload, "toAccountId");
        BigDecimal amount = requiredDecimal(payload, "amount");
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("amount must be > 0");
        }
        if (fromAccountId.equals(toAccountId)) {
            throw new IllegalArgumentException("fromAccountId and toAccountId must be different");
        }

        Response debit = sendAccountAction(request.requestId(), ACTION_ACCOUNT_DEBIT, Map.of(
                "accountId", fromAccountId,
                "amount", amount
        ));
        if (debit.statusCode() >= 400) {
            return failure(request, debit.statusCode(), debit.message());
        }
        appendState(request, TransactionStatus.DEBIT_APPLIED, "transfer debit applied");

        Response credit = sendAccountAction(request.requestId(), ACTION_ACCOUNT_CREDIT, Map.of(
                "accountId", toAccountId,
                "amount", amount
        ));
        if (credit.statusCode() >= 400) {
            Response compensation = sendAccountAction(request.requestId(), ACTION_ACCOUNT_CREDIT, Map.of(
                    "accountId", fromAccountId,
                    "amount", amount
            ));
            appendState(request, TransactionStatus.COMPENSATED, "transfer compensated after credit failure");
            String message = "transfer failed at credit step; compensationStatus=" + compensation.statusCode();
            return failure(request, 502, message);
        }
        appendState(request, TransactionStatus.CREDIT_APPLIED, "transfer credit applied");

        return success(request, 200, "transfer completed", Map.of(
                "fromAccountId", fromAccountId,
                "toAccountId", toAccountId,
                "amount", amount,
                "fromBalance", debit.payload().get("balance"),
                "toBalance", credit.payload().get("balance")
        ));
    }

    private Response sendAccountAction(String requestId, String action, Map<String, Object> payload) {
        Request downstreamRequest = new Request(requestId, ServiceName.ACCOUNT, action, payload);
        return transport.send(downstreamRequest, gatewayInstance);
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

    private Response success(Request request, int statusCode, String message, Map<String, Object> details) {
        appendState(request, TransactionStatus.COMPLETED, message);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("requestId", request.requestId());
        payload.put("message", message);
        payload.putAll(details);
        payload.putAll(transactionMetadata(request.requestId()));
        return new Response(statusCode, message, Map.copyOf(payload));
    }

    private Response failure(Request request, int statusCode, String message) {
        appendState(request, TransactionStatus.FAILED, message);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("requestId", request.requestId());
        payload.put("message", message);
        payload.putAll(transactionMetadata(request.requestId()));
        return new Response(statusCode, message, Map.copyOf(payload));
    }

    private void appendState(Request request, TransactionStatus status, String detail) {
        Map<String, Object> payload = payloadOrEmpty(request.payload());
        BigDecimal amount = optionalDecimal(payload.get("amount"));
        TransactionState state = new TransactionState(
                status,
                request.action(),
                optionalString(payload.get("accountId")),
                optionalString(payload.get("fromAccountId")),
                optionalString(payload.get("toAccountId")),
                amount,
                detail
        );
        VersionedValue<TransactionState> versioned = transactionVersioningService.appendState(request.requestId(), state);
        System.out.printf(
                "event=transaction-state requestId=%s version=%d status=%s detail=\"%s\"%n",
                request.requestId(),
                versioned.version(),
                versioned.value().status(),
                detail
        );
    }

    private Map<String, Object> transactionMetadata(String requestId) {
        VersionedValue<TransactionState> latest = transactionVersioningService.latestState(requestId);
        if (latest == null) {
            return Map.of();
        }
        return Map.of(
                "transactionVersion", latest.version(),
                "transactionStatus", latest.value().status().name()
        );
    }

    private static List<String> accountsToLock(String action, Map<String, Object> payload) {
        if (ACTION_DEPOSIT.equals(action) || ACTION_WITHDRAW.equals(action)) {
            return List.of(requiredString(payload, "accountId"));
        }
        if (ACTION_TRANSFER.equals(action)) {
            List<String> accounts = new ArrayList<>(2);
            accounts.add(requiredString(payload, "fromAccountId"));
            accounts.add(requiredString(payload, "toAccountId"));
            return accounts;
        }
        return List.of();
    }

    private static String optionalString(Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        if (text.isBlank()) {
            return null;
        }
        return text;
    }

    private static BigDecimal optionalDecimal(Object value) {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof Number number) {
                return new BigDecimal(number.toString());
            }
            return new BigDecimal(value.toString());
        } catch (NumberFormatException exception) {
            return null;
        }
    }
}
