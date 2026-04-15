package br.ufrn.pdist.shared.model;

import java.util.Objects;

public record VersionedValue<T>(long version, T value) {

    public VersionedValue {
        if (version <= 0) {
            throw new IllegalArgumentException("version must be greater than zero");
        }
        Objects.requireNonNull(value, "value must not be null");
    }
}
