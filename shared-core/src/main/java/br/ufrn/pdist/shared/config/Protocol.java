package br.ufrn.pdist.shared.config;

public enum Protocol {
    HTTP,
    GRPC,
    TCP,
    UDP;

    public static Protocol from(String raw) {
        if (raw == null || raw.isBlank()) {
            return TCP;
        }
        return Protocol.valueOf(raw.trim().toUpperCase());
    }
}
