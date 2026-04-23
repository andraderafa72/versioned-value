package br.ufrn.pdist.shared.transport;

/**
 * Shared defaults for raw-socket gateways (TCP/HTTP JSON front door).
 * Tune via system properties if you need different limits without code changes.
 */
public final class TransportServerDefaults {

    /**
     * Backlog passed to {@link java.net.ServerSocket#ServerSocket(int, int)} — pending TCP accepts.
     */
    public static final int ACCEPT_BACKLOG = Integer.getInteger(
            "pdist.transport.acceptBacklog",
            300
    );

    /**
     * Worker threads that handle accepted connections (one request per connection in current protocol).
     */
    public static final int WORKER_POOL_SIZE = Integer.getInteger(
            "pdist.transport.workerPoolSize",
            300
    );

    private TransportServerDefaults() {
    }
}
