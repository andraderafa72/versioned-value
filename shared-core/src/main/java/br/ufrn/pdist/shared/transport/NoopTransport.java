package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.config.Protocol;
import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.Response;
import java.util.Map;

public final class NoopTransport implements TransportLayer {

    private final Protocol protocol;

    public NoopTransport(Protocol protocol) {
        this.protocol = protocol;
    }

    @Override
    public void startServer(int port, RequestHandler handler) {
        System.out.printf("transport=start-server protocol=%s port=%d%n", protocol, port);
    }

    @Override
    public Response send(Request request, Instance target) {
        return new Response(501, "Transport not implemented yet for " + protocol, Map.of("requestId", request.requestId()));
    }
}
