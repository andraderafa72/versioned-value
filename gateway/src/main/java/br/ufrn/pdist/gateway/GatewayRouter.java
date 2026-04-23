package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.logging.EventLog;
import br.ufrn.pdist.shared.transport.TransportLayer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
        String requestId = normalizeRequestId(request);
        try {
            if (request.service() == null) {
                return deterministicError(400, "target service is required", requestId, null);
            }

            List<Instance> instances = serviceRegistry.instancesFor(request.service());
            if (instances == null || instances.isEmpty()) {
                return deterministicError(404, "service not found", requestId, request.service());
            }

            Request forwarded = toForwardedRequest(request, requestId);
            String previousInstanceId = null;
            Response lastTransientResponse = null;
            int maxAttempts = retryPolicy.maxAttempts();

            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                List<Instance> healthyInstances = serviceRegistry.instancesFor(request.service());
                if (healthyInstances.isEmpty()) {
                    break;
                }

                Instance selectedInstance = selectNextCandidate(requestId, request.service(), previousInstanceId, healthyInstances);
                if (selectedInstance == null) {
                    System.out.printf(
                            "event=routing-aborted requestId=%s service=%s reason=no-candidate-after-selection%n",
                            requestId,
                            request.service()
                    );
                    break;
                }

                logRetryAttempt(requestId, request.service(), attempt, maxAttempts, selectedInstance, healthyInstances.size());
                Response response = transport.send(forwarded, selectedInstance);

                if (response == null) {
                    response = deterministicError(502, "downstream returned null response", requestId, request.service());
                }

                if (!isTransientFailure(response)) {
                    logRetryFinalResult(requestId, request.service(), response.statusCode(), attempt, false);
                    return response;
                }

                lastTransientResponse = response;
                logRetryFailure(requestId, request.service(), attempt, maxAttempts, selectedInstance, response);
                previousInstanceId = selectedInstance.instanceId();

                if (attempt < maxAttempts) {
                    logRetryFallback(requestId, request.service(), selectedInstance, attempt + 1);
                    backoff();
                }
            }

            logRetryFinalResult(
                    requestId,
                    request.service(),
                    lastTransientResponse == null ? 503 : lastTransientResponse.statusCode(),
                    maxAttempts,
                    true
            );
            return deterministicError(503, "service unavailable", requestId, request.service());
        } catch (Exception exception) {
            EventLog.printlnWithStackTrace(
                    String.format(
                            "event=routing-failure requestId=%s service=%s error=\"%s\"",
                            requestId,
                            request == null || request.service() == null ? "UNKNOWN" : request.service(),
                            exception.getMessage()
                    ),
                    exception
            );
            return deterministicError(500, "gateway routing failure", requestId, request == null ? null : request.service());
        }
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

    private Request toForwardedRequest(Request request, String requestId) {
        Map<String, Object> forwardedPayload = new LinkedHashMap<>();
        if (request.payload() != null) {
            forwardedPayload.putAll(request.payload());
        }
        forwardedPayload.putIfAbsent("requestId", requestId);
        forwardedPayload.put(GATEWAY_FORWARDED_KEY, true);
        forwardedPayload.put(SOURCE_SERVICE_KEY, ServiceName.GATEWAY.name());
        return new Request(requestId, request.service(), request.action(), forwardedPayload);
    }

    private static String normalizeRequestId(Request request) {
        if (request == null || request.requestId() == null || request.requestId().isBlank()) {
            return UUID.randomUUID().toString();
        }
        return request.requestId();
    }

    /**
     * First attempt for a client request uses strict round-robin ({@link GatewayServiceRegistry#selectNextInstance}).
     * Retries after a transient (5xx) failure pick the <strong>next</strong> instance in the sorted healthy list
     * (circular walk), without consuming the RR counter — so concurrent traffic cannot starve failover or leave
     * {@code selectNextCandidate} with no candidate while other instances are healthy.
     */
    private Instance selectNextCandidate(
            String requestId,
            ServiceName service,
            String previousInstanceId,
            List<Instance> healthyInstances
    ) {
        int healthyPoolSize = healthyInstances.size();
        if (healthyPoolSize == 0) {
            return null;
        }

        if (previousInstanceId == null) {
            Instance candidate = serviceRegistry.selectNextInstance(service);
            if (candidate != null) {
                logRoutingDecision(requestId, service, candidate, healthyPoolSize);
            }
            return candidate;
        }

        int prevIdx = indexOfInstanceId(healthyInstances, previousInstanceId);
        if (prevIdx < 0) {
            Instance candidate = serviceRegistry.selectNextInstance(service);
            if (candidate != null) {
                System.out.printf(
                        "event=retry-fallback-unknown-previous requestId=%s service=%s previousInstanceId=%s%n",
                        requestId,
                        service,
                        previousInstanceId
                );
                logRoutingDecision(requestId, service, candidate, healthyPoolSize);
            }
            return candidate;
        }

        if (healthyPoolSize == 1) {
            Instance only = healthyInstances.get(0);
            logRoutingDecision(requestId, service, only, healthyPoolSize);
            return only;
        }

        Instance candidate = healthyInstances.get((prevIdx + 1) % healthyPoolSize);
        System.out.printf(
                "event=retry-instance-rotate requestId=%s service=%s failedInstanceId=%s nextInstanceId=%s%n",
                requestId,
                service,
                previousInstanceId,
                candidate.instanceId()
        );
        logRoutingDecision(requestId, service, candidate, healthyPoolSize);
        return candidate;
    }

    private static int indexOfInstanceId(List<Instance> instances, String instanceId) {
        for (int i = 0; i < instances.size(); i++) {
            if (instances.get(i).instanceId().equals(instanceId)) {
                return i;
            }
        }
        return -1;
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
