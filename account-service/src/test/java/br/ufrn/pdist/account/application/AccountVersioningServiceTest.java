package br.ufrn.pdist.account.application;

import br.ufrn.pdist.account.domain.Account;
import br.ufrn.pdist.shared.model.VersionedValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccountVersioningServiceTest {

    @Test
    void shouldCreateAccountAndReturnLatestBalance() {
        AccountVersioningService service = new AccountVersioningService();

        service.createAccount("acc-1", BigDecimal.TEN);

        assertEquals(BigDecimal.TEN, service.getCurrentBalance("acc-1"));
    }

    @Test
    void shouldAppendWithoutMutatingPreviousSnapshot() {
        AccountVersioningService service = new AccountVersioningService();
        service.createAccount("acc-1", BigDecimal.TEN);

        Account firstSnapshot = service.addBalanceVersion("acc-1", BigDecimal.valueOf(15));
        Account secondSnapshot = service.addBalanceVersion("acc-1", BigDecimal.valueOf(20));

        assertNotSame(firstSnapshot, secondSnapshot);
        assertEquals(2, firstSnapshot.history().size());
        assertEquals(3, secondSnapshot.history().size());
        assertEquals(BigDecimal.valueOf(15), firstSnapshot.latestBalance());
        assertEquals(BigDecimal.valueOf(20), secondSnapshot.latestBalance());
    }

    @Test
    void shouldRejectLatestBalanceWhenHistoryIsEmpty() {
        Account account = new Account("acc-empty", List.of());

        assertThrows(IllegalStateException.class, account::latestBalance);
    }

    @Test
    void shouldKeepVersionsStrictlyIncreasingUnderConcurrentAppends()
            throws InterruptedException, ExecutionException {
        AccountVersioningService service = new AccountVersioningService();
        service.createAccount("acc-1", BigDecimal.ZERO);

        int updates = 20;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < updates; i++) {
                BigDecimal nextBalance = BigDecimal.valueOf(i + 1L);
                tasks.add(() -> {
                    service.addBalanceVersion("acc-1", nextBalance);
                    return null;
                });
            }

            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdown();
        }

        List<VersionedValue<BigDecimal>> history = service.getAccount("acc-1").history();
        assertEquals(updates + 1, history.size());
        for (int i = 1; i < history.size(); i++) {
            long previousVersion = history.get(i - 1).version();
            long currentVersion = history.get(i).version();
            assertTrue(currentVersion > previousVersion);
        }
    }
}
