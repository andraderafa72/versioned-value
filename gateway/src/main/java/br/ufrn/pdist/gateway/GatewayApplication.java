package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.boot.StartupLogger;
import br.ufrn.pdist.shared.config.Protocol;
import br.ufrn.pdist.shared.config.StartupConfig;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.transport.TransportFactory;
import br.ufrn.pdist.shared.transport.TransportLayer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }

    @Bean
    CommandLineRunner gatewayStartupRunner() {
        return args -> {
            StartupConfig config = StartupConfig.fromArgs(args, 8080, "gateway-1");
            StartupLogger.logStartup(ServiceName.GATEWAY, config);
            TransportLayer transport = TransportFactory.create(config.protocol());
            GatewayRoutingConfig routingConfig = GatewayRoutingConfig.fromArgs(args);
            GatewayServiceRegistry serviceRegistry = new GatewayServiceRegistry(routingConfig.serviceRegistry());
            GatewayRetryPolicy retryPolicy = GatewayRetryPolicy.fromArgs(args);
            serviceRegistry.startHeartbeatMonitor(args);
            new UdpRegistrationListener(serviceRegistry).start(args);
            GatewayRouter router = new GatewayRouter(transport, serviceRegistry, retryPolicy);
            RequestHandler entryPoint = router::route;
            if (config.protocol() == Protocol.HTTP) {
                entryPoint = request -> GatewayHttpInboundHandler.handle(router, request);
            }
            transport.startServer(config.port(), entryPoint);
        };
    }
}
