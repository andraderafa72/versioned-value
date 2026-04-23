package br.ufrn.pdist.shared.config;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public record RedisConfig(
        boolean enabled,
        String host,
        int port,
        String password,
        int database,
        Duration timeout,
        Duration requestIdTtl,
        Duration lockTtl
) {
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 6379;
    private static final int DEFAULT_DATABASE = 0;
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration DEFAULT_REQUEST_ID_TTL = Duration.ofMinutes(10);
    private static final Duration DEFAULT_LOCK_TTL = Duration.ofSeconds(5);

    public static RedisConfig fromArgs(String[] args) {
        Map<String, String> options = parseOptions(args);

        boolean enabled = readBoolean(options, "redis.enabled", "REDIS_ENABLED", false);
        String host = readString(options, "redis.host", "REDIS_HOST", DEFAULT_HOST);
        int port = readInt(options, "redis.port", "REDIS_PORT", DEFAULT_PORT);
        String password = readString(options, "redis.password", "REDIS_PASSWORD", "");
        int database = readInt(options, "redis.db", "REDIS_DB", DEFAULT_DATABASE);
        Duration timeout = Duration.ofMillis(readLong(options, "redis.timeout-ms", "REDIS_TIMEOUT_MS", DEFAULT_TIMEOUT.toMillis()));
        Duration requestIdTtl = Duration.ofMillis(readLong(
                options,
                "redis.request-id-ttl-ms",
                "REDIS_REQUEST_ID_TTL_MS",
                DEFAULT_REQUEST_ID_TTL.toMillis()
        ));
        Duration lockTtl = Duration.ofMillis(readLong(options, "redis.lock-ttl-ms", "REDIS_LOCK_TTL_MS", DEFAULT_LOCK_TTL.toMillis()));

        return new RedisConfig(enabled, host, port, password, database, timeout, requestIdTtl, lockTtl);
    }

    public String redisUri() {
        return "redis://" + host + ":" + port + "/" + database;
    }

    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> options = new LinkedHashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--")) {
                continue;
            }
            String raw = arg.substring(2);
            int separatorIndex = raw.indexOf('=');
            if (separatorIndex <= 0 || separatorIndex == raw.length() - 1) {
                continue;
            }
            String key = raw.substring(0, separatorIndex).trim();
            String value = raw.substring(separatorIndex + 1).trim();
            options.put(key, value);
        }
        return options;
    }

    private static boolean readBoolean(Map<String, String> options, String optionKey, String envKey, boolean defaultValue) {
        String raw = readNullable(options, optionKey, envKey);
        if (raw == null) {
            return defaultValue;
        }
        return "true".equalsIgnoreCase(raw) || "1".equals(raw) || "yes".equalsIgnoreCase(raw);
    }

    private static int readInt(Map<String, String> options, String optionKey, String envKey, int defaultValue) {
        String raw = readNullable(options, optionKey, envKey);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static long readLong(Map<String, String> options, String optionKey, String envKey, long defaultValue) {
        String raw = readNullable(options, optionKey, envKey);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static String readString(Map<String, String> options, String optionKey, String envKey, String defaultValue) {
        String raw = readNullable(options, optionKey, envKey);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return raw;
    }

    private static String readNullable(Map<String, String> options, String optionKey, String envKey) {
        String fromArgs = options.get(optionKey);
        if (fromArgs != null && !fromArgs.isBlank()) {
            return fromArgs;
        }
        String fromEnv = System.getenv(envKey);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        return null;
    }
}
