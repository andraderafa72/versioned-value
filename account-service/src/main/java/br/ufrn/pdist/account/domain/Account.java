package br.ufrn.pdist.account.domain;

import br.ufrn.pdist.shared.model.VersionedValue;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class Account {
    private final String id;
    private final List<VersionedValue<BigDecimal>> history;

    public Account(String id, List<VersionedValue<BigDecimal>> history) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.history = List.copyOf(history);
    }

    public String id() {
        return id;
    }

    public List<VersionedValue<BigDecimal>> history() {
        return history;
    }

    public BigDecimal latestBalance() {
        if (history.isEmpty()) {
            throw new IllegalStateException("account history cannot be empty");
        }
        return history.get(history.size() - 1).value();
    }

    public Account appendBalance(long version, BigDecimal value) {
        List<VersionedValue<BigDecimal>> newHistory = new ArrayList<>(history);
        newHistory.add(new VersionedValue<>(version, value));
        return new Account(id, newHistory);
    }
}
