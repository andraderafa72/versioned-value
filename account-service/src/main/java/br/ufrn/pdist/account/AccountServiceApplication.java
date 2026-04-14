package br.ufrn.pdist.account;

import br.ufrn.pdist.shared.boot.StartupLogger;
import br.ufrn.pdist.shared.config.StartupConfig;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.transport.TransportFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class AccountServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AccountServiceApplication.class, args);
    }

    @Bean
    CommandLineRunner accountStartupRunner() {
        return args -> {
            StartupConfig config = StartupConfig.fromArgs(args, 8081, "account-1");
            StartupLogger.logStartup(ServiceName.ACCOUNT, config);
            TransportFactory.create(config.protocol());
        };
    }
}
