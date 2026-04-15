package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GatewayServiceRegistryTest {

    @Test
    void shouldUpdateMetadataWhenInstanceIdAlreadyExists() {
        GatewayServiceRegistry registry = new GatewayServiceRegistry(
                Map.of(
                        ServiceName.ACCOUNT,
                        List.of(new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081))
                )
        );

        registry.register(new Instance("account-1", ServiceName.ACCOUNT, "10.0.0.20", 9101));
        List<Instance> instances = registry.instancesFor(ServiceName.ACCOUNT);

        assertEquals(1, instances.size());
        assertEquals("10.0.0.20", instances.get(0).host());
        assertEquals(9101, instances.get(0).port());
    }

    @Test
    void shouldRemoveInstanceWhenHeartbeatTimesOut() {
        GatewayServiceRegistry registry = new GatewayServiceRegistry(
                Map.of(
                        ServiceName.ACCOUNT,
                        List.of(new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081))
                )
        );

        registry.heartbeat(new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081), System.currentTimeMillis() - 10_000L);
        registry.evictTimedOutInstances(1_000L);

        assertEquals(0, registry.instancesFor(ServiceName.ACCOUNT).size());
    }

    @Test
    void shouldKeepInstanceEligibleWhenHeartbeatIsRecent() {
        GatewayServiceRegistry registry = new GatewayServiceRegistry(
                Map.of(
                        ServiceName.ACCOUNT,
                        List.of(new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081))
                )
        );

        registry.heartbeat(new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081), System.currentTimeMillis());
        registry.evictTimedOutInstances(10_000L);

        assertEquals(1, registry.instancesFor(ServiceName.ACCOUNT).size());
    }

    @Test
    void shouldDistributeSelectionsUsingRoundRobinAcrossHealthyInstances() {
        GatewayServiceRegistry registry = new GatewayServiceRegistry(
                Map.of(
                        ServiceName.ACCOUNT,
                        List.of(
                                new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081),
                                new Instance("account-2", ServiceName.ACCOUNT, "127.0.0.1", 8082)
                        )
                )
        );

        Instance firstSelection = registry.selectNextInstance(ServiceName.ACCOUNT);
        Instance secondSelection = registry.selectNextInstance(ServiceName.ACCOUNT);
        Instance thirdSelection = registry.selectNextInstance(ServiceName.ACCOUNT);

        assertNotNull(firstSelection);
        assertNotNull(secondSelection);
        assertNotNull(thirdSelection);
        assertNotEquals(firstSelection.instanceId(), secondSelection.instanceId());
        assertEquals(firstSelection.instanceId(), thirdSelection.instanceId());
    }

    @Test
    void shouldSkipRemovedInstanceFromRoundRobinSelection() {
        GatewayServiceRegistry registry = new GatewayServiceRegistry(
                Map.of(
                        ServiceName.ACCOUNT,
                        List.of(
                                new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081),
                                new Instance("account-2", ServiceName.ACCOUNT, "127.0.0.1", 8082)
                        )
                )
        );

        registry.heartbeat(new Instance("account-1", ServiceName.ACCOUNT, "127.0.0.1", 8081), System.currentTimeMillis() - 10_000L);
        registry.heartbeat(new Instance("account-2", ServiceName.ACCOUNT, "127.0.0.1", 8082), System.currentTimeMillis());
        registry.evictTimedOutInstances(1_000L);

        Instance selected = registry.selectNextInstance(ServiceName.ACCOUNT);
        assertNotNull(selected);
        assertEquals("account-2", selected.instanceId());
        assertEquals(1, registry.instancesFor(ServiceName.ACCOUNT).size());
    }
}
