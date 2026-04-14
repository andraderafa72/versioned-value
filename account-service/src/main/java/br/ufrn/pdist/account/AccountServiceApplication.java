package br.ufrn.pdist.account;

import br.ufrn.pdist.shared.boot.StartupLogger;
import br.ufrn.pdist.shared.config.StartupConfig;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.transport.TransportFactory;
import br.ufrn.pdist.shared.transport.TransportLayer;
import java.util.Map;
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
            TransportLayer transport = TransportFactory.create(config.protocol());
            transport.startServer(config.port(), AccountServiceApplication::handleRequest);
        };
    }

    private static Response handleRequest(Request request) {
        return new Response(200, "account-service request processed", Map.of("requestId", request.requestId()));
    }
}
