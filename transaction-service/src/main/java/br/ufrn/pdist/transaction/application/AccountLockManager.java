package br.ufrn.pdist.transaction.application;

import java.util.List;

public interface AccountLockManager {
    LockedAccounts lockAll(List<String> accountIds);

    interface LockedAccounts extends AutoCloseable {
        @Override
        void close();
    }
}
