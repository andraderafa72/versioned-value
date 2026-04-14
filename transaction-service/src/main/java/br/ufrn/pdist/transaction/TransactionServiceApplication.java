package br.ufrn.pdist.transaction;

import br.ufrn.pdist.shared.boot.StartupLogger;
import br.ufrn.pdist.shared.config.StartupConfig;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.transport.TransportFactory;
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
            StartupConfig config = StartupConfig.fromArgs(args, 8082, "transaction-1");
            StartupLogger.logStartup(ServiceName.TRANSACTION, config);
            TransportFactory.create(config.protocol());
        };
    }
}
