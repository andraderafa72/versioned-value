package br.ufrn.pdist.shared.contracts;

import java.util.Map;

public record Response(int statusCode, String message, Map<String, Object> payload) {
}
