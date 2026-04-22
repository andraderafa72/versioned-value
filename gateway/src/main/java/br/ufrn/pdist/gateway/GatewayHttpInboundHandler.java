package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.http.BankingHttpRoutes;

/**
 * Translates external HTTP/2 requests (method + path + JSON body) into internal {@link Request} for {@link GatewayRouter}.
 */
public final class GatewayHttpInboundHandler {

    private GatewayHttpInboundHandler() {
    }

    public static Response handle(GatewayRouter router, Request httpRequest) {
        Request mapped = BankingHttpRoutes.parseGatewayRequest(httpRequest);
        if (mapped == null) {
            return BankingHttpRoutes.unknownRouteResponse(httpRequest);
        }
        return router.route(mapped);
    }
}
