package br.ufrn.pdist.shared.discovery;

import br.ufrn.pdist.shared.config.StartupConfig;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class UdpRegistrationClient {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DEFAULT_GATEWAY_HOST = "127.0.0.1";
    private static final int DEFAULT_GATEWAY_REGISTRATION_PORT = 8090;

    private UdpRegistrationClient() {
    }

    public static void registerAtGateway(ServiceName serviceName, StartupConfig startupConfig, String[] args) {
        RegistrationTarget target = RegistrationTarget.fromArgs(args);
        Map<String, Object> registerMessage = new LinkedHashMap<>();
        registerMessage.put("type", "REGISTER");
        registerMessage.put("service", serviceName.name());
        registerMessage.put("host", startupConfig.host());
        registerMessage.put("port", startupConfig.port());
        registerMessage.put("instanceId", startupConfig.instanceId());

        try (DatagramSocket socket = new DatagramSocket()) {
            byte[] payload = OBJECT_MAPPER.writeValueAsBytes(registerMessage);
            InetAddress gatewayAddress = InetAddress.getByName(target.host());
            DatagramPacket packet = new DatagramPacket(payload, payload.length, gatewayAddress, target.port());
            socket.send(packet);
            System.out.printf(
                    "event=instance-registration-sent service=%s instanceId=%s host=%s port=%d gatewayHost=%s gatewayPort=%d%n",
                    serviceName,
                    startupConfig.instanceId(),
                    startupConfig.host(),
                    startupConfig.port(),
                    target.host(),
                    target.port()
            );
        } catch (IOException exception) {
            System.err.printf(
                    "event=instance-registration-error service=%s instanceId=%s message=%s%n",
                    serviceName,
                    startupConfig.instanceId(),
                    exception.getMessage()
            );
        }
    }

    private record RegistrationTarget(String host, int port) {

        private static final String GATEWAY_HOST_ARG = "--gateway.registration.host=";
        private static final String GATEWAY_PORT_ARG = "--gateway.registration.port=";
        private static final String GATEWAY_HOST_ENV = "GATEWAY_REGISTRATION_HOST";
        private static final String GATEWAY_PORT_ENV = "GATEWAY_REGISTRATION_PORT";

        static RegistrationTarget fromArgs(String[] args) {
            String host = null;
            Integer port = null;
            for (String arg : args) {
                if (arg.startsWith(GATEWAY_HOST_ARG)) {
                    host = arg.substring(GATEWAY_HOST_ARG.length()).trim();
                    continue;
                }
                if (arg.startsWith(GATEWAY_PORT_ARG)) {
                    port = parsePort(arg.substring(GATEWAY_PORT_ARG.length()).trim());
                }
            }

            if (host == null || host.isBlank()) {
                String envHost = System.getenv(GATEWAY_HOST_ENV);
                host = (envHost == null || envHost.isBlank()) ? DEFAULT_GATEWAY_HOST : envHost.trim();
            }

            if (port == null) {
                String envPort = System.getenv(GATEWAY_PORT_ENV);
                port = (envPort == null || envPort.isBlank()) ? DEFAULT_GATEWAY_REGISTRATION_PORT : parsePort(envPort.trim());
            }

            return new RegistrationTarget(host, port);
        }

        private static int parsePort(String rawPort) {
            try {
                return Integer.parseInt(rawPort);
            } catch (NumberFormatException exception) {
                return DEFAULT_GATEWAY_REGISTRATION_PORT;
            }
        }
    }
}
