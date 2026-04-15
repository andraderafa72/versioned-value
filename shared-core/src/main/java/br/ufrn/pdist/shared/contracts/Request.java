package br.ufrn.pdist.shared.contracts;

import java.util.Map;

public record Request(String requestId, ServiceName service, String action, Map<String, Object> payload) {

    public Request(String requestId, String action, Map<String, Object> payload) {
        this(requestId, null, action, payload);
    }
}
