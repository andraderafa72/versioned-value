package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
