package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class GatewayServiceRegistry {

    private final Map<ServiceName, Map<String, Instance>> instancesByService;

    GatewayServiceRegistry(Map<ServiceName, List<Instance>> initialRegistry) {
        this.instancesByService = new ConcurrentHashMap<>();
        for (Map.Entry<ServiceName, List<Instance>> entry : initialRegistry.entrySet()) {
            Map<String, Instance> instances = new ConcurrentHashMap<>();
            for (Instance instance : entry.getValue()) {
                instances.put(instance.instanceId(), instance);
            }
            instancesByService.put(entry.getKey(), instances);
        }
    }

    List<Instance> instancesFor(ServiceName serviceName) {
        Map<String, Instance> instances = instancesByService.get(serviceName);
        if (instances == null) {
            return List.of();
        }
        return new ArrayList<>(instances.values());
    }

    void register(Instance instance) {
        Map<String, Instance> instances = instancesByService.computeIfAbsent(
                instance.serviceName(),
                ignored -> new ConcurrentHashMap<>()
        );
        instances.put(instance.instanceId(), instance);
        System.out.printf(
                "event=instance-registration service=%s instanceId=%s host=%s port=%d%n",
                instance.serviceName(),
                instance.instanceId(),
                instance.host(),
                instance.port()
        );
    }
}
