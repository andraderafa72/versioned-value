package br.ufrn.pdist.transaction.application;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

final class AccountLockManager {
    private final ConcurrentMap<String, ReentrantLock> locksByAccountId = new ConcurrentHashMap<>();

    LockedAccounts lockAll(List<String> accountIds) {
        List<String> uniqueSortedIds = accountIds.stream()
                .filter(Objects::nonNull)
                .distinct()
                .sorted(Comparator.naturalOrder())
                .toList();
        List<ReentrantLock> acquired = new ArrayList<>(uniqueSortedIds.size());
        for (String accountId : uniqueSortedIds) {
            ReentrantLock lock = locksByAccountId.computeIfAbsent(accountId, key -> new ReentrantLock());
            lock.lock();
            acquired.add(lock);
        }
        return new LockedAccounts(acquired);
    }

    static final class LockedAccounts implements AutoCloseable {
        private final List<ReentrantLock> locks;

        private LockedAccounts(List<ReentrantLock> locks) {
            this.locks = locks;
        }

        @Override
        public void close() {
            for (int i = locks.size() - 1; i >= 0; i--) {
                locks.get(i).unlock();
            }
        }
    }
}
