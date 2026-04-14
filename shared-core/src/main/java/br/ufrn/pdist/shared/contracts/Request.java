package br.ufrn.pdist.shared.contracts;

import java.util.Map;

public record Request(String requestId, String action, Map<String, Object> payload) {
}
