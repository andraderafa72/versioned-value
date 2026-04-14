package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.config.Protocol;

public final class TransportFactory {

    private TransportFactory() {
    }

    public static TransportLayer create(Protocol protocol) {
        if (protocol == Protocol.TCP) {
            return new TcpTransport();
        }
        return new NoopTransport(protocol);
    }
}
