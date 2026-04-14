package br.ufrn.pdist.account.application;

import br.ufrn.pdist.account.domain.Account;
import br.ufrn.pdist.account.domain.VersionedValue;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class AccountVersioningService {
    private final ConcurrentHashMap<String, Account> accounts = new ConcurrentHashMap<>();
    private final AtomicLong versionClock = new AtomicLong(0);

    public Account createAccount(String accountId, BigDecimal initialBalance) {
        long initialVersion = versionClock.incrementAndGet();
        Account account = new Account(
                accountId,
                List.of(new VersionedValue<>(initialVersion, initialBalance))
        );
        Account existing = accounts.putIfAbsent(accountId, account);
        if (existing != null) {
            throw new IllegalArgumentException("account already exists: " + accountId);
        }
        logVersionCreation(accountId, initialVersion, initialBalance);
        return account;
    }

    public BigDecimal getCurrentBalance(String accountId) {
        return getAccount(accountId).latestBalance();
    }

    public Account addBalanceVersion(String accountId, BigDecimal newBalance) {
        Account updated = accounts.compute(accountId, (id, current) -> {
            if (current == null) {
                throw new NoSuchElementException("account not found: " + id);
            }
            long version = versionClock.incrementAndGet();
            Account appended = current.appendBalance(version, newBalance);
            logVersionCreation(accountId, version, newBalance);
            return appended;
        });
        return updated;
    }

    public Account getAccount(String accountId) {
        Account account = accounts.get(accountId);
        if (account == null) {
            throw new NoSuchElementException("account not found: " + accountId);
        }
        return account;
    }

    private void logVersionCreation(String accountId, long version, BigDecimal value) {
        System.out.printf(
                "version creation accountId=%s version=%d value=%s%n",
                accountId,
                version,
                value
        );
    }
}
