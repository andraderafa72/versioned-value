package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.config.Protocol;

public final class TransportFactory {

    private TransportFactory() {
    }

    public static TransportLayer create(Protocol protocol) {
        return new NoopTransport(protocol);
    }
}
