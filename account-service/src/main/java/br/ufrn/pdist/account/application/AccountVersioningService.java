package br.ufrn.pdist.account.application;

import br.ufrn.pdist.account.domain.Account;
import br.ufrn.pdist.account.domain.AccountVersionEntry;
import br.ufrn.pdist.shared.config.RedisConfig;
import br.ufrn.pdist.shared.model.VersionedValue;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

public class AccountVersioningService {
    private final AccountStateBackend backend;

    public AccountVersioningService() {
        this(new InMemoryAccountStateBackend());
    }

    public AccountVersioningService(RedisConfig redisConfig) {
        this(redisConfig.enabled() ? new RedisAccountStateBackend(redisConfig) : new InMemoryAccountStateBackend());
    }

    AccountVersioningService(AccountStateBackend backend) {
        this.backend = backend;
    }

    public Account createAccount(String accountId, BigDecimal initialBalance) {
        return createAccount(accountId, initialBalance, randomRequestId());
    }

    public Account createAccount(String accountId, BigDecimal initialBalance, String requestId) {
        AccountVersionEntry created = backend.createAccount(accountId, initialBalance, normalizedRequestId(requestId));
        logVersionCreation(accountId, created.version(), created.balance(), created.operation(), created.requestId());
        return toAccount(accountId, backend.history(accountId));
    }

    public Account addBalanceVersion(String accountId, BigDecimal newBalance) {
        BigDecimal current = getCurrentBalance(accountId);
        BigDecimal delta = newBalance.subtract(current);
        if (delta.compareTo(BigDecimal.ZERO) >= 0) {
            return credit(accountId, delta, randomRequestId());
        }
        return debit(accountId, delta.abs(), randomRequestId());
    }

    public Account credit(String accountId, BigDecimal amount, String requestId) {
        AccountVersionEntry updated = backend.credit(accountId, amount, normalizedRequestId(requestId));
        logVersionCreation(accountId, updated.version(), updated.balance(), updated.operation(), updated.requestId());
        return toAccount(accountId, backend.history(accountId));
    }

    public Account debit(String accountId, BigDecimal amount, String requestId) {
        AccountVersionEntry updated = backend.debit(accountId, amount, normalizedRequestId(requestId));
        logVersionCreation(accountId, updated.version(), updated.balance(), updated.operation(), updated.requestId());
        return toAccount(accountId, backend.history(accountId));
    }

    public BigDecimal getCurrentBalance(String accountId) {
        return backend.current(accountId).balance();
    }

    public Account getAccount(String accountId) {
        List<AccountVersionEntry> history = backend.history(accountId);
        if (history.isEmpty()) {
            throw new NoSuchElementException("account not found: " + accountId);
        }
        return toAccount(accountId, history);
    }

    private static Account toAccount(String accountId, List<AccountVersionEntry> entries) {
        List<VersionedValue<BigDecimal>> history = new ArrayList<>(entries.size());
        for (AccountVersionEntry entry : entries) {
            history.add(new VersionedValue<>(entry.version(), entry.balance()));
        }
        return new Account(accountId, history);
    }

    private static String normalizedRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return randomRequestId();
        }
        return requestId;
    }

    private static String randomRequestId() {
        return UUID.randomUUID().toString();
    }

    private void logVersionCreation(String accountId, long version, BigDecimal value, String operation, String requestId) {
        System.out.printf(
                "version creation accountId=%s version=%d value=%s operation=%s requestId=%s%n",
                accountId,
                version,
                value,
                operation,
                requestId
        );
    }
}
