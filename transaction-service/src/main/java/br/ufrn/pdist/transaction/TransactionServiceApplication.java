package br.ufrn.pdist.transaction;

import br.ufrn.pdist.transaction.application.TransactionRequestHandler;
import br.ufrn.pdist.shared.boot.StartupLogger;
import br.ufrn.pdist.shared.config.StartupConfig;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.discovery.UdpRegistrationClient;
import br.ufrn.pdist.shared.transport.TransportFactory;
import br.ufrn.pdist.shared.transport.TransportLayer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class TransactionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransactionServiceApplication.class, args);
    }

    @Bean
    CommandLineRunner transactionStartupRunner() {
        return args -> {
            StartupConfig config = StartupConfig.fromArgs(args, 8083, "transaction-1");
            StartupLogger.logStartup(ServiceName.TRANSACTION, config);
            UdpRegistrationClient.registerAtGateway(ServiceName.TRANSACTION, config, args);
            TransportLayer transport = TransportFactory.create(config.protocol());
            GatewayTargetConfig gatewayTarget = GatewayTargetConfig.fromArgs(args);
            TransactionRequestHandler handler = new TransactionRequestHandler(transport, gatewayTarget.instance());
            transport.startServer(config.port(), handler::handle);
        };
    }
}
