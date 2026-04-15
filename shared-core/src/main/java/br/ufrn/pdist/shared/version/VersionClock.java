package br.ufrn.pdist.shared.version;

import java.util.concurrent.atomic.AtomicLong;

public final class VersionClock {
    private final AtomicLong clock;

    public VersionClock() {
        this(0L);
    }

    public VersionClock(long initialValue) {
        this.clock = new AtomicLong(initialValue);
    }

    public long nextVersion() {
        return clock.incrementAndGet();
    }

    public long currentVersion() {
        return clock.get();
    }
}
