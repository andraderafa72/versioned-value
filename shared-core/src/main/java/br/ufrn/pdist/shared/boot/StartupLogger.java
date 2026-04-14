package br.ufrn.pdist.shared.boot;

import br.ufrn.pdist.shared.config.StartupConfig;
import br.ufrn.pdist.shared.contracts.ServiceName;

public final class StartupLogger {

    private StartupLogger() {
    }

    public static void logStartup(ServiceName service, StartupConfig config) {
        System.out.printf(
                "event=startup service=%s instanceId=%s host=%s port=%d protocol=%s%n",
                service,
                config.instanceId(),
                config.host(),
                config.port(),
                config.protocol()
        );
    }
}
