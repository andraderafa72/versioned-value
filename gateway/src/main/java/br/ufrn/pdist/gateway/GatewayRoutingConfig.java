package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class GatewayRoutingConfig {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final List<String> DEFAULT_ACCOUNT_ENDPOINTS = List.of("127.0.0.1:8081", "127.0.0.1:8082");
    private static final List<String> DEFAULT_TRANSACTION_ENDPOINTS = List.of("127.0.0.1:8083", "127.0.0.1:8084");
    private final Map<ServiceName, List<Instance>> serviceRegistry;

    private GatewayRoutingConfig(Map<ServiceName, List<Instance>> serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    static GatewayRoutingConfig fromArgs(String[] args) {
        Map<String, String> options = parseOptions(args);
        List<String> accountEndpoints = resolveEndpoints(
                options,
                "account.instances",
                "ACCOUNT_INSTANCES",
                DEFAULT_ACCOUNT_ENDPOINTS
        );
        List<String> transactionEndpoints = resolveEndpoints(
                options,
                "transaction.instances",
                "TRANSACTION_INSTANCES",
                DEFAULT_TRANSACTION_ENDPOINTS
        );

        Map<ServiceName, List<Instance>> registry = new LinkedHashMap<>();
        registry.put(ServiceName.ACCOUNT, parseInstances(ServiceName.ACCOUNT, accountEndpoints));
        registry.put(ServiceName.TRANSACTION, parseInstances(ServiceName.TRANSACTION, transactionEndpoints));
        return new GatewayRoutingConfig(Map.copyOf(registry));
    }

    Map<ServiceName, List<Instance>> serviceRegistry() {
        return serviceRegistry;
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

    private static List<String> resolveEndpoints(
            Map<String, String> options,
            String argName,
            String envName,
            List<String> fallback
    ) {
        String fromArgs = options.get(argName);
        if (fromArgs != null && !fromArgs.isBlank()) {
            return splitCsv(fromArgs);
        }
        String fromEnv = System.getenv(envName);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return splitCsv(fromEnv);
        }
        return fallback;
    }

    private static List<String> splitCsv(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .toList();
    }

    private static List<Instance> parseInstances(ServiceName serviceName, List<String> endpoints) {
        if (endpoints.size() < 2) {
            throw new IllegalArgumentException(serviceName + " requires at least 2 instances");
        }

        List<Instance> instances = new ArrayList<>(endpoints.size());
        for (int i = 0; i < endpoints.size(); i++) {
            String endpoint = endpoints.get(i);
            String[] hostAndPort = endpoint.split(":");
            if (hostAndPort.length != 2) {
                throw new IllegalArgumentException("Invalid endpoint format: " + endpoint);
            }
            String host = hostAndPort[0].isBlank() ? DEFAULT_HOST : hostAndPort[0].trim();
            int port = parsePort(hostAndPort[1].trim(), endpoint);
            String instanceId = serviceName.name().toLowerCase() + "-" + (i + 1);
            instances.add(new Instance(instanceId, serviceName, host, port));
        }
        return List.copyOf(instances);
    }

    private static int parsePort(String rawPort, String endpoint) {
        try {
            return Integer.parseInt(rawPort);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid endpoint format: " + endpoint);
        }
    }
}
