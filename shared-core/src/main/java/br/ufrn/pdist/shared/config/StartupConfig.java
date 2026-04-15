package br.ufrn.pdist.shared.config;

public record StartupConfig(Protocol protocol, String host, int port, String instanceId) {

    public static StartupConfig fromArgs(String[] args, int defaultPort, String defaultInstanceId) {
        Protocol protocol = Protocol.TCP;
        int port = defaultPort;
        String instanceId = defaultInstanceId;

        for (String arg : args) {
            if (arg.startsWith("--protocol=")) {
                protocol = Protocol.from(arg.substring("--protocol=".length()));
                continue;
            }
            if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
                continue;
            }
            if (arg.startsWith("--instance-id=")) {
                instanceId = arg.substring("--instance-id=".length());
            }
        }

        return new StartupConfig(protocol, "127.0.0.1", port, instanceId);
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
