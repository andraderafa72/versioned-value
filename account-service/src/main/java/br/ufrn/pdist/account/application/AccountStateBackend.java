package br.ufrn.pdist.account.application;

import br.ufrn.pdist.account.domain.AccountVersionEntry;
import java.math.BigDecimal;
import java.util.List;

interface AccountStateBackend extends AutoCloseable {
    AccountVersionEntry createAccount(String accountId, BigDecimal initialBalance, String requestId);

    AccountVersionEntry credit(String accountId, BigDecimal amount, String requestId);

    AccountVersionEntry debit(String accountId, BigDecimal amount, String requestId);

    AccountVersionEntry current(String accountId);

    List<AccountVersionEntry> history(String accountId);

    @Override
    default void close() {
        // Optional no-op for in-memory backend.
    }
}
