package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.transport.TransportLayer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class GatewayRouter {

    private static final String GATEWAY_FORWARDED_KEY = "gatewayForwarded";
    private static final String SOURCE_SERVICE_KEY = "sourceService";
    private final TransportLayer transport;
    private final GatewayServiceRegistry serviceRegistry;

    GatewayRouter(TransportLayer transport) {
        this(transport, new GatewayServiceRegistry(GatewayRoutingConfig.fromArgs(new String[0]).serviceRegistry()));
    }

    GatewayRouter(TransportLayer transport, GatewayServiceRegistry serviceRegistry) {
        this.transport = transport;
        this.serviceRegistry = serviceRegistry;
    }

    Response route(Request request) {
        if (request.service() == null) {
            return deterministicError(400, "target service is required", request.requestId(), null);
        }

        List<Instance> instances = serviceRegistry.instancesFor(request.service());
        if (instances == null || instances.isEmpty()) {
            return deterministicError(404, "service not found", request.requestId(), request.service());
        }

        Instance selectedInstance = instances.get(0);
        logRoutingDecision(request.requestId(), request.service(), selectedInstance);

        Map<String, Object> forwardedPayload = new LinkedHashMap<>();
        if (request.payload() != null) {
            forwardedPayload.putAll(request.payload());
        }
        forwardedPayload.put(GATEWAY_FORWARDED_KEY, true);
        forwardedPayload.put(SOURCE_SERVICE_KEY, ServiceName.GATEWAY.name());

        Request forwarded = new Request(request.requestId(), request.service(), request.action(), forwardedPayload);
        Response response = transport.send(forwarded, selectedInstance);

        if (response.statusCode() >= 500) {
            return deterministicError(503, "service unavailable", request.requestId(), request.service());
        }
        return response;
    }

    private static Response deterministicError(
            int statusCode,
            String message,
            String requestId,
            ServiceName service
    ) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("requestId", requestId);
        if (service != null) {
            payload.put("service", service.name());
        }
        payload.put("message", message);
        return new Response(statusCode, message, payload);
    }

    private static void logRoutingDecision(String requestId, ServiceName service, Instance selectedInstance) {
        System.out.printf(
                "event=routing-decision requestId=%s service=%s instanceId=%s host=%s port=%d%n",
                requestId,
                service,
                selectedInstance.instanceId(),
                selectedInstance.host(),
                selectedInstance.port()
        );
    }
}
