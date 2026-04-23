package br.ufrn.pdist.account.application;

import br.ufrn.pdist.account.domain.AccountVersionEntry;
import br.ufrn.pdist.shared.config.RedisConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

final class RedisAccountStateBackend implements AccountStateBackend {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final String RESULT_OK = "OK";
    private static final String RESULT_EXISTS = "EXISTS";
    private static final String RESULT_NOT_FOUND = "NOT_FOUND";
    private static final String RESULT_INSUFFICIENT = "INSUFFICIENT_FUNDS";
    private static final String LUA_CREATE = """
            local currentKey = KEYS[1]
            local versionsKey = KEYS[2]
            local versionKey = KEYS[3]
            local reqKey = KEYS[4]
            local reqTtlSec = tonumber(ARGV[1])
            local requestId = ARGV[2]
            local accountId = ARGV[3]
            local initialBalance = ARGV[4]
            local now = ARGV[5]
            local metadataJson = ARGV[6]

            local replay = redis.call('GET', reqKey)
            if replay then
                return {'REPLAY', replay}
            end

            if redis.call('EXISTS', currentKey) == 1 then
                return {'EXISTS', ''}
            end

            local version = redis.call('INCR', versionKey)
            local entry = cjson.encode({
                version = version,
                balance = initialBalance,
                operation = 'createAccount',
                requestId = requestId,
                timestamp = now,
                metadata = cjson.decode(metadataJson)
            })
            redis.call('HSET', currentKey,
                'version', tostring(version),
                'balance', initialBalance,
                'operation', 'createAccount',
                'requestId', requestId,
                'timestamp', now)
            redis.call('RPUSH', versionsKey, entry)
            local result = cjson.encode({version = version, balance = initialBalance, operation='createAccount', requestId=requestId, timestamp=now, metadata=cjson.decode(metadataJson)})
            redis.call('SET', reqKey, result, 'EX', reqTtlSec)
            return {'OK', result}
            """;
    private static final String LUA_MUTATE = """
            local currentKey = KEYS[1]
            local versionsKey = KEYS[2]
            local versionKey = KEYS[3]
            local reqKey = KEYS[4]
            local reqTtlSec = tonumber(ARGV[1])
            local requestId = ARGV[2]
            local operation = ARGV[3]
            local delta = tonumber(ARGV[4])
            local now = ARGV[5]
            local metadataJson = ARGV[6]

            local replay = redis.call('GET', reqKey)
            if replay then
                return {'REPLAY', replay}
            end

            if redis.call('EXISTS', currentKey) == 0 then
                return {'NOT_FOUND', ''}
            end

            local currentBalance = tonumber(redis.call('HGET', currentKey, 'balance'))
            local nextBalance = currentBalance + delta
            if nextBalance < 0 then
                return {'INSUFFICIENT_FUNDS', ''}
            end

            local version = redis.call('INCR', versionKey)
            local nextBalanceStr = tostring(nextBalance)
            local entry = cjson.encode({
                version = version,
                balance = nextBalanceStr,
                operation = operation,
                requestId = requestId,
                timestamp = now,
                metadata = cjson.decode(metadataJson)
            })
            redis.call('HSET', currentKey,
                'version', tostring(version),
                'balance', nextBalanceStr,
                'operation', operation,
                'requestId', requestId,
                'timestamp', now)
            redis.call('RPUSH', versionsKey, entry)
            local result = cjson.encode({version = version, balance = nextBalanceStr, operation=operation, requestId=requestId, timestamp=now, metadata=cjson.decode(metadataJson)})
            redis.call('SET', reqKey, result, 'EX', reqTtlSec)
            return {'OK', result}
            """;

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final RedisConfig redisConfig;

    RedisAccountStateBackend(RedisConfig redisConfig) {
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
    public AccountVersionEntry createAccount(String accountId, BigDecimal initialBalance, String requestId) {
        String reqId = normalizedRequestId(requestId);
        String accountKey = accountCurrentKey(accountId);
        List<String> result = castList(commands.eval(
                LUA_CREATE,
                ScriptOutputType.MULTI,
                new String[]{
                        accountKey,
                        accountVersionsKey(accountId),
                        accountVersionCounterKey(accountId),
                        idempotencyKey(accountId, "createAccount", reqId)
                },
                String.valueOf(redisConfig.requestIdTtl().toSeconds()),
                reqId,
                accountId,
                initialBalance.toPlainString(),
                Instant.now().toString(),
                toJson(Map.of("accountId", accountId))
        ));
        String status = result.get(0);
        if (RESULT_OK.equals(status) || "REPLAY".equals(status)) {
            return decodeEntry(result.get(1));
        }
        if (RESULT_EXISTS.equals(status)) {
            throw new IllegalArgumentException("account already exists: " + accountId);
        }
        throw new IllegalStateException("unexpected redis create result: " + status);
    }

    @Override
    public AccountVersionEntry credit(String accountId, BigDecimal amount, String requestId) {
        return mutate(accountId, amount, requestId, "credit");
    }

    @Override
    public AccountVersionEntry debit(String accountId, BigDecimal amount, String requestId) {
        return mutate(accountId, amount.negate(), requestId, "debit");
    }

    @Override
    public AccountVersionEntry current(String accountId) {
        Map<String, String> state = commands.hgetall(accountCurrentKey(accountId));
        if (state == null || state.isEmpty()) {
            throw new NoSuchElementException("account not found: " + accountId);
        }
        return new AccountVersionEntry(
                Long.parseLong(state.getOrDefault("version", "0")),
                new BigDecimal(state.get("balance")),
                state.getOrDefault("operation", "unknown"),
                state.getOrDefault("requestId", ""),
                Instant.parse(state.getOrDefault("timestamp", Instant.EPOCH.toString())),
                Map.of("accountId", accountId)
        );
    }

    @Override
    public List<AccountVersionEntry> history(String accountId) {
        List<String> rawEntries = commands.lrange(accountVersionsKey(accountId), 0, -1);
        if (rawEntries == null || rawEntries.isEmpty()) {
            throw new NoSuchElementException("account not found: " + accountId);
        }
        List<AccountVersionEntry> history = new ArrayList<>(rawEntries.size());
        for (String rawEntry : rawEntries) {
            history.add(decodeEntry(rawEntry));
        }
        return List.copyOf(history);
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
    }

    private AccountVersionEntry mutate(String accountId, BigDecimal delta, String requestId, String operation) {
        String reqId = normalizedRequestId(requestId);
        List<String> result = castList(commands.eval(
                LUA_MUTATE,
                ScriptOutputType.MULTI,
                new String[]{
                        accountCurrentKey(accountId),
                        accountVersionsKey(accountId),
                        accountVersionCounterKey(accountId),
                        idempotencyKey(accountId, operation, reqId)
                },
                String.valueOf(redisConfig.requestIdTtl().toSeconds()),
                reqId,
                operation,
                delta.toPlainString(),
                Instant.now().toString(),
                toJson(Map.of("accountId", accountId, "delta", delta.toPlainString()))
        ));
        String status = result.get(0);
        if (RESULT_OK.equals(status) || "REPLAY".equals(status)) {
            return decodeEntry(result.get(1));
        }
        if (RESULT_NOT_FOUND.equals(status)) {
            throw new NoSuchElementException("account not found: " + accountId);
        }
        if (RESULT_INSUFFICIENT.equals(status)) {
            throw new IllegalArgumentException("insufficient funds");
        }
        throw new IllegalStateException("unexpected redis mutate result: " + status);
    }

    private static String normalizedRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return UUID.randomUUID().toString();
        }
        return requestId;
    }

    private static String accountCurrentKey(String accountId) {
        return "account:" + accountId + ":current";
    }

    private static String accountVersionsKey(String accountId) {
        return "account:" + accountId + ":versions";
    }

    private static String accountVersionCounterKey(String accountId) {
        return "account:" + accountId + ":version";
    }

    /**
     * Idempotency must be scoped per account and operation so a single transaction
     * requestId can debit one account and credit another; compensation credit also
     * stays distinct from the original debit on the same account.
     */
    private static String idempotencyKey(String accountId, String operation, String requestId) {
        return "idemp:" + accountId + ":" + operation + ":" + requestId;
    }

    private static AccountVersionEntry decodeEntry(String rawJson) {
        try {
            Map<String, Object> raw = OBJECT_MAPPER.readValue(rawJson, MAP_TYPE);
            Map<String, String> metadata = new LinkedHashMap<>();
            Object metadataNode = raw.get("metadata");
            if (metadataNode instanceof Map<?, ?> mapNode) {
                for (Map.Entry<?, ?> entry : mapNode.entrySet()) {
                    metadata.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
                }
            }
            return new AccountVersionEntry(
                    Long.parseLong(String.valueOf(raw.get("version"))),
                    new BigDecimal(String.valueOf(raw.get("balance"))),
                    String.valueOf(raw.get("operation")),
                    String.valueOf(raw.get("requestId")),
                    Instant.parse(String.valueOf(raw.get("timestamp"))),
                    Map.copyOf(metadata)
            );
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("cannot decode redis entry", exception);
        }
    }

    private static String toJson(Map<String, String> payload) {
        try {
            return OBJECT_MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("cannot encode redis payload", exception);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> castList(Object result) {
        List<Object> values = (List<Object>) result;
        List<String> mapped = new ArrayList<>(values.size());
        for (Object value : values) {
            mapped.add(value == null ? "" : value.toString());
        }
        return mapped;
    }
}
