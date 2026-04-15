package br.ufrn.pdist.transaction.domain;

import br.ufrn.pdist.shared.model.VersionedValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class TransactionRecord {
    private final String requestId;
    private final List<VersionedValue<TransactionState>> history;

    public TransactionRecord(String requestId, List<VersionedValue<TransactionState>> history) {
        this.requestId = Objects.requireNonNull(requestId, "requestId must not be null");
        this.history = List.copyOf(history);
    }

    public String requestId() {
        return requestId;
    }

    public List<VersionedValue<TransactionState>> history() {
        return history;
    }

    public VersionedValue<TransactionState> latest() {
        if (history.isEmpty()) {
            throw new IllegalStateException("transaction history cannot be empty");
        }
        return history.get(history.size() - 1);
    }

    public TransactionRecord appendState(long version, TransactionState state) {
        List<VersionedValue<TransactionState>> newHistory = new ArrayList<>(history);
        newHistory.add(new VersionedValue<>(version, state));
        return new TransactionRecord(requestId, newHistory);
    }
}
