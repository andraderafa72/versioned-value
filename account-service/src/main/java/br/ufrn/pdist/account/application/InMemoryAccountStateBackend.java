package br.ufrn.pdist.account.application;

import br.ufrn.pdist.account.domain.AccountVersionEntry;
import br.ufrn.pdist.shared.version.VersionClock;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

final class InMemoryAccountStateBackend implements AccountStateBackend {
    private final ConcurrentHashMap<String, List<AccountVersionEntry>> entriesByAccountId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AccountVersionEntry> idempotencyByKey = new ConcurrentHashMap<>();
    private final VersionClock versionClock = new VersionClock();

    @Override
    public synchronized AccountVersionEntry createAccount(String accountId, BigDecimal initialBalance, String requestId) {
        String reqId = normalizedRequestId(requestId);
        String idempotencyKey = idempotencyKey(accountId, "createAccount", reqId);
        AccountVersionEntry replay = idempotencyByKey.get(idempotencyKey);
        if (replay != null) {
            return replay;
        }
        if (entriesByAccountId.containsKey(accountId)) {
            throw new IllegalArgumentException("account already exists: " + accountId);
        }
        AccountVersionEntry created = new AccountVersionEntry(
                versionClock.nextVersion(),
                initialBalance,
                "createAccount",
                reqId,
                Instant.now(),
                Map.of("accountId", accountId)
        );
        entriesByAccountId.put(accountId, new ArrayList<>(List.of(created)));
        idempotencyByKey.put(idempotencyKey, created);
        return created;
    }

    @Override
    public synchronized AccountVersionEntry credit(String accountId, BigDecimal amount, String requestId) {
        String reqId = normalizedRequestId(requestId);
        String idempotencyKey = idempotencyKey(accountId, "credit", reqId);
        AccountVersionEntry replay = idempotencyByKey.get(idempotencyKey);
        if (replay != null) {
            return replay;
        }
        AccountVersionEntry current = current(accountId);
        AccountVersionEntry next = append(
                accountId,
                current.balance().add(amount),
                "credit",
                reqId,
                Map.of("accountId", accountId, "amount", amount.toPlainString())
        );
        idempotencyByKey.put(idempotencyKey, next);
        return next;
    }

    @Override
    public synchronized AccountVersionEntry debit(String accountId, BigDecimal amount, String requestId) {
        String reqId = normalizedRequestId(requestId);
        String idempotencyKey = idempotencyKey(accountId, "debit", reqId);
        AccountVersionEntry replay = idempotencyByKey.get(idempotencyKey);
        if (replay != null) {
            return replay;
        }
        AccountVersionEntry current = current(accountId);
        if (current.balance().compareTo(amount) < 0) {
            throw new IllegalArgumentException("insufficient funds");
        }
        AccountVersionEntry next = append(
                accountId,
                current.balance().subtract(amount),
                "debit",
                reqId,
                Map.of("accountId", accountId, "amount", amount.toPlainString())
        );
        idempotencyByKey.put(idempotencyKey, next);
        return next;
    }

    @Override
    public synchronized AccountVersionEntry current(String accountId) {
        List<AccountVersionEntry> history = entriesByAccountId.get(accountId);
        if (history == null || history.isEmpty()) {
            throw new NoSuchElementException("account not found: " + accountId);
        }
        return history.get(history.size() - 1);
    }

    @Override
    public synchronized List<AccountVersionEntry> history(String accountId) {
        List<AccountVersionEntry> history = entriesByAccountId.get(accountId);
        if (history == null || history.isEmpty()) {
            throw new NoSuchElementException("account not found: " + accountId);
        }
        return List.copyOf(history);
    }

    private AccountVersionEntry append(
            String accountId,
            BigDecimal newBalance,
            String operation,
            String requestId,
            Map<String, String> metadata
    ) {
        List<AccountVersionEntry> history = entriesByAccountId.get(accountId);
        if (history == null || history.isEmpty()) {
            throw new NoSuchElementException("account not found: " + accountId);
        }
        AccountVersionEntry entry = new AccountVersionEntry(
                versionClock.nextVersion(),
                newBalance,
                operation,
                requestId,
                Instant.now(),
                Map.copyOf(new LinkedHashMap<>(metadata))
        );
        history.add(entry);
        return entry;
    }

    private static String idempotencyKey(String accountId, String operation, String requestId) {
        return accountId + "|" + operation + "|" + requestId;
    }

    private static String normalizedRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return UUID.randomUUID().toString();
        }
        return requestId;
    }
}
