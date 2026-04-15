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
    private static final int TRANSIENT_FAILURE_STATUS_CODE = 500;
    private final TransportLayer transport;
    private final GatewayServiceRegistry serviceRegistry;
    private final GatewayRetryPolicy retryPolicy;

    GatewayRouter(TransportLayer transport) {
        this(
                transport,
                new GatewayServiceRegistry(GatewayRoutingConfig.fromArgs(new String[0]).serviceRegistry()),
                GatewayRetryPolicy.defaults()
        );
    }

    GatewayRouter(TransportLayer transport, GatewayServiceRegistry serviceRegistry) {
        this(transport, serviceRegistry, GatewayRetryPolicy.defaults());
    }

    GatewayRouter(TransportLayer transport, GatewayServiceRegistry serviceRegistry, GatewayRetryPolicy retryPolicy) {
        this.transport = transport;
        this.serviceRegistry = serviceRegistry;
        this.retryPolicy = retryPolicy;
    }

    Response route(Request request) {
        if (request.service() == null) {
            return deterministicError(400, "target service is required", request.requestId(), null);
        }

        List<Instance> instances = serviceRegistry.instancesFor(request.service());
        if (instances == null || instances.isEmpty()) {
            return deterministicError(404, "service not found", request.requestId(), request.service());
        }

        Request forwarded = toForwardedRequest(request);
        String previousInstanceId = null;
        Response lastTransientResponse = null;
        int maxAttempts = retryPolicy.maxAttempts();

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            List<Instance> healthyInstances = serviceRegistry.instancesFor(request.service());
            if (healthyInstances.isEmpty()) {
                break;
            }

            Instance selectedInstance = selectNextCandidate(
                    request.requestId(),
                    request.service(),
                    previousInstanceId,
                    healthyInstances.size()
            );
            if (selectedInstance == null) {
                break;
            }

            logRetryAttempt(request.requestId(), request.service(), attempt, maxAttempts, selectedInstance, healthyInstances.size());
            Response response = transport.send(forwarded, selectedInstance);

            if (!isTransientFailure(response)) {
                logRetryFinalResult(request.requestId(), request.service(), response.statusCode(), attempt, false);
                return response;
            }

            lastTransientResponse = response;
            logRetryFailure(request.requestId(), request.service(), attempt, maxAttempts, selectedInstance, response);
            previousInstanceId = selectedInstance.instanceId();

            if (attempt < maxAttempts) {
                logRetryFallback(request.requestId(), request.service(), selectedInstance, attempt + 1);
                backoff();
            }
        }

        logRetryFinalResult(
                request.requestId(),
                request.service(),
                lastTransientResponse == null ? 503 : lastTransientResponse.statusCode(),
                maxAttempts,
                true
        );
        return deterministicError(503, "service unavailable", request.requestId(), request.service());
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

    private Request toForwardedRequest(Request request) {
        Map<String, Object> forwardedPayload = new LinkedHashMap<>();
        if (request.payload() != null) {
            forwardedPayload.putAll(request.payload());
        }
        forwardedPayload.put(GATEWAY_FORWARDED_KEY, true);
        forwardedPayload.put(SOURCE_SERVICE_KEY, ServiceName.GATEWAY.name());
        return new Request(request.requestId(), request.service(), request.action(), forwardedPayload);
    }

    private Instance selectNextCandidate(
            String requestId,
            ServiceName service,
            String previousInstanceId,
            int healthyPoolSize
    ) {
        for (int selectionAttempt = 0; selectionAttempt < healthyPoolSize; selectionAttempt++) {
            Instance candidate = serviceRegistry.selectNextInstance(service);
            if (candidate == null) {
                return null;
            }
            if (healthyPoolSize == 1 || !candidate.instanceId().equals(previousInstanceId)) {
                logRoutingDecision(requestId, service, candidate, healthyPoolSize);
                return candidate;
            }
        }
        return null;
    }

    private void backoff() {
        if (retryPolicy.backoffMillis() <= 0) {
            return;
        }
        try {
            Thread.sleep(retryPolicy.backoffMillis());
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean isTransientFailure(Response response) {
        return response != null && response.statusCode() >= TRANSIENT_FAILURE_STATUS_CODE;
    }

    private static void logRetryAttempt(
            String requestId,
            ServiceName service,
            int attempt,
            int maxAttempts,
            Instance selectedInstance,
            int healthyPoolSize
    ) {
        System.out.printf(
                "event=retry-attempt requestId=%s service=%s attempt=%d maxAttempts=%d instanceId=%s host=%s port=%d healthyPoolSize=%d%n",
                requestId,
                service,
                attempt,
                maxAttempts,
                selectedInstance.instanceId(),
                selectedInstance.host(),
                selectedInstance.port(),
                healthyPoolSize
        );
    }

    private static void logRetryFailure(
            String requestId,
            ServiceName service,
            int attempt,
            int maxAttempts,
            Instance selectedInstance,
            Response response
    ) {
        System.out.printf(
                "event=retry-failure requestId=%s service=%s attempt=%d maxAttempts=%d instanceId=%s status=%d cause=\"%s\"%n",
                requestId,
                service,
                attempt,
                maxAttempts,
                selectedInstance.instanceId(),
                response.statusCode(),
                response.message()
        );
    }

    private static void logRetryFallback(
            String requestId,
            ServiceName service,
            Instance failedInstance,
            int nextAttempt
    ) {
        System.out.printf(
                "event=retry-fallback requestId=%s service=%s failedInstanceId=%s nextAttempt=%d%n",
                requestId,
                service,
                failedInstance.instanceId(),
                nextAttempt
        );
    }

    private static void logRetryFinalResult(
            String requestId,
            ServiceName service,
            int lastStatusCode,
            int attempts,
            boolean exhausted
    ) {
        System.out.printf(
                "event=retry-final requestId=%s service=%s attempts=%d exhausted=%s lastStatus=%d%n",
                requestId,
                service,
                attempts,
                exhausted,
                lastStatusCode
        );
    }

    private static void logRoutingDecision(
            String requestId,
            ServiceName service,
            Instance selectedInstance,
            int healthyPoolSize
    ) {
        System.out.printf(
                "event=routing-decision requestId=%s service=%s instanceId=%s host=%s port=%d healthyPoolSize=%d%n",
                requestId,
                service,
                selectedInstance.instanceId(),
                selectedInstance.host(),
                selectedInstance.port(),
                healthyPoolSize
        );
    }
}
