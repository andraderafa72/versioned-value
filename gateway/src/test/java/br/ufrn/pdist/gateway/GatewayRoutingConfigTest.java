package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayRoutingConfigTest {

    @Test
    void shouldUseAtLeastTwoDefaultInstancesPerService() {
        GatewayRoutingConfig config = GatewayRoutingConfig.fromArgs(new String[0]);
        Map<ServiceName, List<Instance>> registry = config.serviceRegistry();

        assertEquals(2, registry.get(ServiceName.ACCOUNT).size());
        assertEquals(2, registry.get(ServiceName.TRANSACTION).size());
        assertTrue(registry.get(ServiceName.ACCOUNT).stream().noneMatch(i -> i.serviceName() == ServiceName.GATEWAY));
        assertTrue(registry.get(ServiceName.TRANSACTION).stream().noneMatch(i -> i.serviceName() == ServiceName.GATEWAY));
    }

    @Test
    void shouldAllowCustomInstancesByArgs() {
        GatewayRoutingConfig config = GatewayRoutingConfig.fromArgs(
                new String[] {
                        "--account.instances=10.0.0.1:9001,10.0.0.2:9002",
                        "--transaction.instances=10.0.1.1:9101,10.0.1.2:9102,10.0.1.3:9103"
                }
        );

        Map<ServiceName, List<Instance>> registry = config.serviceRegistry();
        assertEquals(2, registry.get(ServiceName.ACCOUNT).size());
        assertEquals(3, registry.get(ServiceName.TRANSACTION).size());
        assertEquals(9001, registry.get(ServiceName.ACCOUNT).get(0).port());
        assertEquals("10.0.1.3", registry.get(ServiceName.TRANSACTION).get(2).host());
    }

    @Test
    void shouldRejectConfigurationsWithLessThanTwoInstances() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> GatewayRoutingConfig.fromArgs(new String[] {"--account.instances=127.0.0.1:8081"})
        );

        assertEquals("ACCOUNT requires at least 2 instances", exception.getMessage());
    }
}
