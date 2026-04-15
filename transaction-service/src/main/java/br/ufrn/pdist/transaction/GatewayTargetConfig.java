package br.ufrn.pdist.transaction;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.util.LinkedHashMap;
import java.util.Map;

final class GatewayTargetConfig {

    private static final String DEFAULT_GATEWAY_HOST = "127.0.0.1";
    private static final int DEFAULT_GATEWAY_PORT = 8080;
    private final Instance instance;

    private GatewayTargetConfig(Instance instance) {
        this.instance = instance;
    }

    static GatewayTargetConfig fromArgs(String[] args) {
        Map<String, String> options = parseOptions(args);
        String endpoint = options.get("gateway.endpoint");
        if (endpoint == null || endpoint.isBlank()) {
            endpoint = System.getenv("GATEWAY_ENDPOINT");
        }
        if (endpoint == null || endpoint.isBlank()) {
            return new GatewayTargetConfig(
                    new Instance("gateway-1", ServiceName.GATEWAY, DEFAULT_GATEWAY_HOST, DEFAULT_GATEWAY_PORT)
            );
        }

        String[] hostAndPort = endpoint.trim().split(":");
        if (hostAndPort.length != 2) {
            throw new IllegalArgumentException("Invalid gateway endpoint format: " + endpoint);
        }
        String host = hostAndPort[0].isBlank() ? DEFAULT_GATEWAY_HOST : hostAndPort[0].trim();
        int port;
        try {
            port = Integer.parseInt(hostAndPort[1].trim());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid gateway endpoint format: " + endpoint);
        }
        return new GatewayTargetConfig(new Instance("gateway-1", ServiceName.GATEWAY, host, port));
    }

    Instance instance() {
        return instance;
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
}
