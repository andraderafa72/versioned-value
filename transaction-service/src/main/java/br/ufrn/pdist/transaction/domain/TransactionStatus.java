package br.ufrn.pdist.transaction.domain;

public enum TransactionStatus {
    STARTED,
    DEBIT_APPLIED,
    CREDIT_APPLIED,
    COMPLETED,
    FAILED,
    COMPENSATED
}
