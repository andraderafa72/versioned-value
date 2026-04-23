package br.ufrn.pdist.transaction.application;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public final class LocalAccountLockManager implements AccountLockManager {
    private final ConcurrentMap<String, ReentrantLock> locksByAccountId = new ConcurrentHashMap<>();

    @Override
    public LockedAccounts lockAll(List<String> accountIds) {
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
        return () -> {
            for (int i = acquired.size() - 1; i >= 0; i--) {
                acquired.get(i).unlock();
            }
        };
    }
}
