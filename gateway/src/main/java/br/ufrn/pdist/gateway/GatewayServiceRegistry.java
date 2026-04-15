package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class GatewayServiceRegistry {

    private static final long DEFAULT_HEARTBEAT_TIMEOUT_MILLIS = 7000L;
    private static final long DEFAULT_HEARTBEAT_SCAN_INTERVAL_MILLIS = 2000L;
    private final Map<ServiceName, Map<String, Instance>> instancesByService;
    private final Map<String, Long> lastSeenByInstanceId;
    private final Map<ServiceName, AtomicInteger> roundRobinIndexByService;
    private final ScheduledExecutorService heartbeatScanner;

    GatewayServiceRegistry(Map<ServiceName, List<Instance>> initialRegistry) {
        this.instancesByService = new ConcurrentHashMap<>();
        this.lastSeenByInstanceId = new ConcurrentHashMap<>();
        this.roundRobinIndexByService = new ConcurrentHashMap<>();
        this.heartbeatScanner = Executors.newSingleThreadScheduledExecutor(daemonFactory("gateway-heartbeat-scanner"));
        for (Map.Entry<ServiceName, List<Instance>> entry : initialRegistry.entrySet()) {
            Map<String, Instance> instances = new ConcurrentHashMap<>();
            for (Instance instance : entry.getValue()) {
                instances.put(instance.instanceId(), instance);
                lastSeenByInstanceId.put(instance.instanceId(), System.currentTimeMillis());
            }
            instancesByService.put(entry.getKey(), instances);
            if (!instances.isEmpty()) {
                roundRobinIndexByService.put(entry.getKey(), new AtomicInteger(0));
            }
        }
    }

    List<Instance> instancesFor(ServiceName serviceName) {
        Map<String, Instance> instances = instancesByService.get(serviceName);
        if (instances == null) {
            return List.of();
        }
        List<Instance> snapshot = new ArrayList<>(instances.values());
        snapshot.sort(Comparator.comparing(Instance::instanceId));
        return snapshot;
    }

    Instance selectNextInstance(ServiceName serviceName) {
        List<Instance> healthyInstances = instancesFor(serviceName);
        if (healthyInstances.isEmpty()) {
            return null;
        }

        AtomicInteger pointer = roundRobinIndexByService.computeIfAbsent(serviceName, ignored -> new AtomicInteger(0));
        int currentIndex = pointer.getAndUpdate(previous -> previous == Integer.MAX_VALUE ? 0 : previous + 1);
        int selectedIndex = Math.floorMod(currentIndex, healthyInstances.size());
        Instance selected = healthyInstances.get(selectedIndex);
        System.out.printf(
                "event=round-robin-selection service=%s instanceId=%s host=%s port=%d healthyPoolSize=%d selectedIndex=%d%n",
                selected.serviceName(),
                selected.instanceId(),
                selected.host(),
                selected.port(),
                healthyInstances.size(),
                selectedIndex
        );
        return selected;
    }

    void register(Instance instance) {
        Map<String, Instance> instances = instancesByService.computeIfAbsent(
                instance.serviceName(),
                ignored -> new ConcurrentHashMap<>()
        );
        instances.put(instance.instanceId(), instance);
        lastSeenByInstanceId.put(instance.instanceId(), System.currentTimeMillis());
        roundRobinIndexByService.computeIfAbsent(instance.serviceName(), ignored -> new AtomicInteger(0));
        System.out.printf(
                "event=instance-registration service=%s instanceId=%s host=%s port=%d%n",
                instance.serviceName(),
                instance.instanceId(),
                instance.host(),
                instance.port()
        );
    }

    void heartbeat(Instance instance, long timestampMillis) {
        Map<String, Instance> instances = instancesByService.computeIfAbsent(
                instance.serviceName(),
                ignored -> new ConcurrentHashMap<>()
        );
        instances.put(instance.instanceId(), instance);
        lastSeenByInstanceId.put(instance.instanceId(), timestampMillis);
        roundRobinIndexByService.computeIfAbsent(instance.serviceName(), ignored -> new AtomicInteger(0));
        System.out.printf(
                "event=heartbeat-received service=%s instanceId=%s host=%s port=%d timestamp=%d%n",
                instance.serviceName(),
                instance.instanceId(),
                instance.host(),
                instance.port(),
                timestampMillis
        );
    }

    void startHeartbeatMonitor(String[] args) {
        long timeoutMillis = resolveDurationArg(args, "--gateway.heartbeat.timeout-ms=", "GATEWAY_HEARTBEAT_TIMEOUT_MS",
                DEFAULT_HEARTBEAT_TIMEOUT_MILLIS);
        long scanIntervalMillis = resolveDurationArg(args, "--gateway.heartbeat.scan-ms=", "GATEWAY_HEARTBEAT_SCAN_MS",
                DEFAULT_HEARTBEAT_SCAN_INTERVAL_MILLIS);

        heartbeatScanner.scheduleAtFixedRate(
                () -> evictTimedOutInstances(timeoutMillis),
                scanIntervalMillis,
                scanIntervalMillis,
                TimeUnit.MILLISECONDS
        );
        System.out.printf(
                "event=heartbeat-monitor-started timeoutMs=%d scanIntervalMs=%d%n",
                timeoutMillis,
                scanIntervalMillis
        );
    }

    void evictTimedOutInstances(long timeoutMillis) {
        long now = System.currentTimeMillis();
        for (Map.Entry<ServiceName, Map<String, Instance>> serviceEntry : instancesByService.entrySet()) {
            Map<String, Instance> instances = serviceEntry.getValue();
            for (Map.Entry<String, Instance> instanceEntry : instances.entrySet()) {
                String instanceId = instanceEntry.getKey();
                Long lastSeen = lastSeenByInstanceId.get(instanceId);
                if (lastSeen == null || now - lastSeen <= timeoutMillis) {
                    continue;
                }
                Instance removed = instances.remove(instanceId);
                lastSeenByInstanceId.remove(instanceId);
                if (removed != null) {
                    System.out.printf(
                            "event=instance-removal reason=heartbeat-timeout service=%s instanceId=%s host=%s port=%d lastSeen=%d now=%d%n",
                            removed.serviceName(),
                            removed.instanceId(),
                            removed.host(),
                            removed.port(),
                            lastSeen,
                            now
                    );
                    if (instances.isEmpty()) {
                        roundRobinIndexByService.remove(serviceEntry.getKey());
                    }
                }
            }
        }
    }

    private static long resolveDurationArg(String[] args, String cliPrefix, String envKey, long defaultValue) {
        for (String arg : args) {
            if (arg.startsWith(cliPrefix)) {
                return parsePositiveLong(arg.substring(cliPrefix.length()).trim(), defaultValue);
            }
        }
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isBlank()) {
            return parsePositiveLong(envValue.trim(), defaultValue);
        }
        return defaultValue;
    }

    private static long parsePositiveLong(String rawValue, long defaultValue) {
        try {
            long parsed = Long.parseLong(rawValue);
            return parsed > 0 ? parsed : defaultValue;
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static ThreadFactory daemonFactory(String threadName) {
        return runnable -> {
            Thread thread = new Thread(runnable, threadName);
            thread.setDaemon(true);
            return thread;
        };
    }
}
