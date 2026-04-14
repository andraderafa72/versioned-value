package br.ufrn.pdist.shared.config;

public record StartupConfig(Protocol protocol, String host, int port, String instanceId) {

    public static StartupConfig fromArgs(String[] args, int defaultPort, String defaultInstanceId) {
        Protocol protocol = Protocol.TCP;

        for (String arg : args) {
            if (arg.startsWith("--protocol=")) {
                protocol = Protocol.from(arg.substring("--protocol=".length()));
            }
        }

        return new StartupConfig(protocol, "127.0.0.1", defaultPort, defaultInstanceId);
    }

    @Override
    public String toString() {
        return "StartupConfig{" +
                "protocol=" + protocol +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", instanceId='" + instanceId + '\'' +
                '}';
    }
}
