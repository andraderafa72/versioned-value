package br.ufrn.pdist.transaction.application;

import br.ufrn.pdist.shared.config.RedisConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public final class RedisAccountLockManager implements AccountLockManager, AutoCloseable {
    private static final long RETRY_DELAY_MILLIS = 10L;
    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final RedisConfig redisConfig;

    public RedisAccountLockManager(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
        RedisURI.Builder builder = RedisURI.builder()
                .withHost(redisConfig.host())
                .withPort(redisConfig.port())
                .withDatabase(redisConfig.database())
                .withTimeout(redisConfig.timeout());
        if (redisConfig.password() != null && !redisConfig.password().isBlank()) {
            builder.withPassword(redisConfig.password().toCharArray());
        }
        this.client = RedisClient.create(builder.build());
        this.connection = client.connect();
        this.commands = connection.sync();
        String ping = commands.ping();
        if (!"PONG".equalsIgnoreCase(ping)) {
            throw new IllegalStateException("redis healthcheck failed");
        }
    }

    @Override
    public LockedAccounts lockAll(List<String> accountIds) {
        List<String> uniqueSortedIds = accountIds.stream()
                .filter(Objects::nonNull)
                .distinct()
                .sorted(Comparator.naturalOrder())
                .toList();
        List<LockToken> acquired = new ArrayList<>(uniqueSortedIds.size());
        try {
            for (String accountId : uniqueSortedIds) {
                LockToken token = acquire(accountId);
                acquired.add(token);
            }
            return () -> {
                for (int i = acquired.size() - 1; i >= 0; i--) {
                    release(acquired.get(i));
                }
            };
        } catch (RuntimeException exception) {
            for (int i = acquired.size() - 1; i >= 0; i--) {
                release(acquired.get(i));
            }
            throw exception;
        }
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
    }

    private LockToken acquire(String accountId) {
        String lockKey = "lock:account:" + accountId;
        String token = UUID.randomUUID().toString();
        long timeoutNanos = redisConfig.lockTtl().toNanos();
        long startNanos = System.nanoTime();
        while (System.nanoTime() - startNanos < timeoutNanos) {
            String ok = commands.set(lockKey, token, io.lettuce.core.SetArgs.Builder.nx().px(redisConfig.lockTtl().toMillis()));
            if ("OK".equals(ok)) {
                return new LockToken(lockKey, token);
            }
            sleep(RETRY_DELAY_MILLIS);
        }
        throw new IllegalStateException("failed to acquire distributed lock for " + accountId);
    }

    private void release(LockToken token) {
        String currentToken = commands.get(token.lockKey());
        if (token.token().equals(currentToken)) {
            commands.del(token.lockKey());
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
    }

    private record LockToken(String lockKey, String token) {
    }
}
