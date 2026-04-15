package br.ufrn.pdist.gateway;

final class GatewayRetryPolicy {

    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final long DEFAULT_BACKOFF_MILLIS = 150L;
    private final int maxAttempts;
    private final long backoffMillis;

    private GatewayRetryPolicy(int maxAttempts, long backoffMillis) {
        this.maxAttempts = maxAttempts;
        this.backoffMillis = backoffMillis;
    }

    static GatewayRetryPolicy defaults() {
        return new GatewayRetryPolicy(DEFAULT_MAX_ATTEMPTS, DEFAULT_BACKOFF_MILLIS);
    }

    static GatewayRetryPolicy fromArgs(String[] args) {
        int maxAttempts = parsePositiveInt(
                readOption(args, "--gateway.retry.max-attempts=", "GATEWAY_RETRY_MAX_ATTEMPTS"),
                DEFAULT_MAX_ATTEMPTS
        );
        long backoffMillis = parsePositiveLong(
                readOption(args, "--gateway.retry.backoff-ms=", "GATEWAY_RETRY_BACKOFF_MS"),
                DEFAULT_BACKOFF_MILLIS
        );
        return new GatewayRetryPolicy(maxAttempts, backoffMillis);
    }

    int maxAttempts() {
        return maxAttempts;
    }

    long backoffMillis() {
        return backoffMillis;
    }

    private static String readOption(String[] args, String cliPrefix, String envKey) {
        for (String arg : args) {
            if (arg.startsWith(cliPrefix)) {
                return arg.substring(cliPrefix.length()).trim();
            }
        }
        String envValue = System.getenv(envKey);
        return envValue == null ? "" : envValue.trim();
    }

    private static int parsePositiveInt(String rawValue, int defaultValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(rawValue);
            return parsed > 0 ? parsed : defaultValue;
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static long parsePositiveLong(String rawValue, long defaultValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(rawValue);
            return parsed > 0 ? parsed : defaultValue;
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }
}
