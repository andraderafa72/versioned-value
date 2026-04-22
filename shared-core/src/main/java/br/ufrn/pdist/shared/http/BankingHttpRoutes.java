package br.ufrn.pdist.shared.http;

import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map;
import java.util.UUID;

/**
 * Maps public HTTP paths (RFC 7540 + JSON body) to internal {@link Request} (service + domain action).
 */
public final class BankingHttpRoutes {

    public static final String API_PREFIX = "/api/v1";

    private BankingHttpRoutes() {
    }

    public static Response unknownRouteResponse(Request httpRequest) {
        String rid = httpRequest.requestId();
        if (rid == null || rid.isBlank()) {
            rid = UUID.randomUUID().toString();
        }
        return new Response(404, "not found", Map.of("requestId", rid, "message", "Unknown route"));
    }

    public record OutboundHttp(String method, String path, Map<String, Object> jsonBody) {
    }

    /**
     * Gateway: HTTP {@code Request(requestId, "METHOD path", jsonPayload)} to routed internal request.
     */
    public static Request parseGatewayRequest(Request httpRequest) {
        String[] mp = splitMethodPath(httpRequest.action());
        String method = mp[0];
        String path = mp[1];
        Map<String, Object> body = copyPayload(httpRequest.payload());
        String requestId = resolveRequestId(httpRequest.requestId(), body);

        if ("POST".equals(method) && path.equals(API_PREFIX + "/accounts")) {
            return new Request(requestId, ServiceName.ACCOUNT, "createAccount", body);
        }
        if ("GET".equals(method) && path.startsWith(API_PREFIX + "/accounts/") && path.endsWith("/balance")) {
            String accountId = extractSegment(path, API_PREFIX + "/accounts/", "/balance");
            body.put("accountId", accountId);
            return new Request(requestId, ServiceName.ACCOUNT, "getBalance", body);
        }
        if ("POST".equals(method) && path.contains("/credit")) {
            String accountId = extractBetween(path, API_PREFIX + "/accounts/", "/credit");
            body.put("accountId", accountId);
            return new Request(requestId, ServiceName.ACCOUNT, "credit", body);
        }
        if ("POST".equals(method) && path.contains("/debit")) {
            String accountId = extractBetween(path, API_PREFIX + "/accounts/", "/debit");
            body.put("accountId", accountId);
            return new Request(requestId, ServiceName.ACCOUNT, "debit", body);
        }
        if ("POST".equals(method) && path.equals(API_PREFIX + "/transactions/deposit")) {
            return new Request(requestId, ServiceName.TRANSACTION, "deposit", body);
        }
        if ("POST".equals(method) && path.equals(API_PREFIX + "/transactions/withdraw")) {
            return new Request(requestId, ServiceName.TRANSACTION, "withdraw", body);
        }
        if ("POST".equals(method) && path.equals(API_PREFIX + "/transactions/transfer")) {
            return new Request(requestId, ServiceName.TRANSACTION, "transfer", body);
        }
        // JMeter / legacy alias
        if ("POST".equals(method) && "/accounts/deposit".equals(path)) {
            return new Request(requestId, ServiceName.TRANSACTION, "deposit", body);
        }
        return null;
    }

    /**
     * Account / transaction service: same HTTP mapping to domain {@link Request}.
     */
    public static Request parseServiceRequest(ServiceName service, Request httpRequest) {
        Request r = parseGatewayRequest(httpRequest);
        if (r == null || r.service() != service) {
            return null;
        }
        return r;
    }

    /**
     * Build outbound HTTP target for {@link br.ufrn.pdist.shared.transport.HttpTransport} client hop.
     */
    public static OutboundHttp toOutboundRequest(Request internal) {
        if (internal.service() == null || internal.action() == null) {
            throw new IllegalArgumentException("internal request needs service and domain action");
        }
        String rid = internal.requestId();
        Map<String, Object> body = copyPayload(internal.payload());
        body.putIfAbsent("requestId", rid);

        return switch (internal.service()) {
            case ACCOUNT -> outboundAccount(internal.action(), body);
            case TRANSACTION -> outboundTransaction(internal.action(), body);
            default -> throw new IllegalArgumentException("unsupported service: " + internal.service());
        };
    }

    private static OutboundHttp outboundAccount(String action, Map<String, Object> body) {
        return switch (action) {
            case "createAccount" -> new OutboundHttp("POST", API_PREFIX + "/accounts", body);
            case "getBalance" -> {
                String id = requiredString(body, "accountId");
                Map<String, Object> b = new LinkedHashMap<>();
                copyForwardingMeta(body, b);
                b.put("accountId", id);
                yield new OutboundHttp("GET", API_PREFIX + "/accounts/" + id + "/balance", b);
            }
            case "credit" -> {
                String id = requiredString(body, "accountId");
                Map<String, Object> b = new LinkedHashMap<>();
                copyForwardingMeta(body, b);
                b.put("accountId", id);
                b.put("amount", body.get("amount"));
                yield new OutboundHttp("POST", API_PREFIX + "/accounts/" + id + "/credit", b);
            }
            case "debit" -> {
                String id = requiredString(body, "accountId");
                Map<String, Object> b = new LinkedHashMap<>();
                copyForwardingMeta(body, b);
                b.put("accountId", id);
                b.put("amount", body.get("amount"));
                yield new OutboundHttp("POST", API_PREFIX + "/accounts/" + id + "/debit", b);
            }
            default -> throw new IllegalArgumentException("unsupported account action: " + action);
        };
    }

    private static OutboundHttp outboundTransaction(String action, Map<String, Object> body) {
        return switch (action) {
            case "deposit" -> new OutboundHttp("POST", API_PREFIX + "/transactions/deposit", body);
            case "withdraw" -> new OutboundHttp("POST", API_PREFIX + "/transactions/withdraw", body);
            case "transfer" -> new OutboundHttp("POST", API_PREFIX + "/transactions/transfer", body);
            default -> throw new IllegalArgumentException("unsupported transaction action: " + action);
        };
    }

    private static void copyForwardingMeta(Map<String, Object> from, Map<String, Object> to) {
        if (from.containsKey("requestId")) {
            to.put("requestId", from.get("requestId"));
        }
        if (from.containsKey("gatewayForwarded")) {
            to.put("gatewayForwarded", from.get("gatewayForwarded"));
        }
        if (from.containsKey("sourceService")) {
            to.put("sourceService", from.get("sourceService"));
        }
    }

    private static String[] splitMethodPath(String action) {
        if (action == null || action.isBlank()) {
            return new String[] {"POST", "/"};
        }
        String[] p = action.trim().split(" ", 2);
        if (p.length == 2) {
            return new String[] {p[0].toUpperCase(Locale.ROOT), p[1]};
        }
        return new String[] {"POST", "/" + action.trim()};
    }

    private static Map<String, Object> copyPayload(Map<String, Object> payload) {
        if (payload == null) {
            return new LinkedHashMap<>();
        }
        return new LinkedHashMap<>(payload);
    }

    private static String resolveRequestId(String headerRequestId, Map<String, Object> body) {
        Object fromBody = body.get("requestId");
        if (fromBody != null && !fromBody.toString().isBlank()) {
            return fromBody.toString();
        }
        if (headerRequestId != null && !headerRequestId.isBlank()) {
            return headerRequestId;
        }
        String gen = UUID.randomUUID().toString();
        body.put("requestId", gen);
        return gen;
    }

    private static String extractBetween(String path, String prefix, String suffix) {
        if (!path.startsWith(prefix) || !path.endsWith(suffix)) {
            throw new IllegalArgumentException("invalid path: " + path);
        }
        return path.substring(prefix.length(), path.length() - suffix.length());
    }

    private static String extractSegment(String path, String prefix, String suffix) {
        return extractBetween(path, prefix, suffix);
    }

    private static String requiredString(Map<String, Object> body, String key) {
        Object v = body.get(key);
        if (v == null || v.toString().isBlank()) {
            throw new IllegalArgumentException(key + " is required");
        }
        return v.toString();
    }

}
