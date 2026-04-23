package br.ufrn.pdist.account.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

public record AccountVersionEntry(
        long version,
        BigDecimal balance,
        String operation,
        String requestId,
        Instant timestamp,
        Map<String, String> metadata
) {
}
