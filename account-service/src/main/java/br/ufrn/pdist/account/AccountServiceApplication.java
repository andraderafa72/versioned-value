package br.ufrn.pdist.account;

import br.ufrn.pdist.account.application.AccountRequestHandler;
import br.ufrn.pdist.shared.boot.StartupLogger;
import br.ufrn.pdist.shared.config.Protocol;
import br.ufrn.pdist.shared.config.StartupConfig;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.http.BankingHttpRoutes;
import br.ufrn.pdist.shared.discovery.UdpRegistrationClient;
import br.ufrn.pdist.shared.transport.TransportFactory;
import br.ufrn.pdist.shared.transport.TransportLayer;
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
    CommandLineRunner accountStartupRunner(AccountRequestHandler handler) {
        return args -> {
            StartupConfig config = StartupConfig.fromArgs(args, 8081, "account-1");
            StartupLogger.logStartup(ServiceName.ACCOUNT, config);
            UdpRegistrationClient.registerAtGateway(ServiceName.ACCOUNT, config, args);
            TransportLayer transport = TransportFactory.create(config.protocol());
            RequestHandler pipeline = handler::handle;
            if (config.protocol() == Protocol.HTTP) {
                pipeline = (Request req) -> {
                    Request mapped = BankingHttpRoutes.parseServiceRequest(ServiceName.ACCOUNT, req);
                    if (mapped == null) {
                        return BankingHttpRoutes.unknownRouteResponse(req);
                    }
                    return handler.handle(mapped);
                };
            }
            transport.startServer(config.port(), pipeline);
        };
    }
}
