package br.ufrn.pdist.transaction.application;

import br.ufrn.pdist.shared.model.VersionedValue;
import br.ufrn.pdist.shared.version.VersionClock;
import br.ufrn.pdist.transaction.domain.TransactionRecord;
import br.ufrn.pdist.transaction.domain.TransactionState;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class TransactionVersioningService {
    private final ConcurrentMap<String, TransactionRecord> recordsByRequestId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, VersionClock> clocksByRequestId = new ConcurrentHashMap<>();

    VersionedValue<TransactionState> appendState(String requestId, TransactionState state) {
        VersionClock clock = clocksByRequestId.computeIfAbsent(requestId, key -> new VersionClock());
        long nextVersion = clock.nextVersion();
        TransactionRecord record = recordsByRequestId.compute(requestId, (key, current) -> {
            if (current == null) {
                return new TransactionRecord(key, List.of(new VersionedValue<>(nextVersion, state)));
            }
            return current.appendState(nextVersion, state);
        });
        return record.latest();
    }

    VersionedValue<TransactionState> latestState(String requestId) {
        TransactionRecord record = recordsByRequestId.get(requestId);
        if (record == null) {
            return null;
        }
        return record.latest();
    }
}
