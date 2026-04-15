package br.ufrn.pdist.transaction.domain;

import java.math.BigDecimal;

public record TransactionState(
        TransactionStatus status,
        String operation,
        String accountId,
        String fromAccountId,
        String toAccountId,
        BigDecimal amount,
        String detail
) {
}
