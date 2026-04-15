package br.ufrn.pdist.gateway;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.ServiceName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.Map;

final class UdpRegistrationListener {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> REGISTER_TYPE = new TypeReference<>() { };
    private static final int BUFFER_SIZE = 4096;
    private static final int DEFAULT_REGISTRATION_PORT = 8090;

    private final GatewayServiceRegistry serviceRegistry;

    UdpRegistrationListener(GatewayServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    void start(String[] args) {
        int registrationPort = resolveRegistrationPort(args);
        Thread listenerThread = new Thread(() -> listen(registrationPort), "gateway-udp-registration-listener");
        listenerThread.setDaemon(true);
        listenerThread.start();
        System.out.printf("event=udp-registration-listener-started port=%d%n", registrationPort);
    }

    private void listen(int registrationPort) {
        byte[] buffer = new byte[BUFFER_SIZE];
        try (DatagramSocket socket = new DatagramSocket(registrationPort)) {
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                handle(packet);
            }
        } catch (IOException exception) {
            System.err.printf(
                    "event=udp-registration-listener-error port=%d message=%s%n",
                    registrationPort,
                    exception.getMessage()
            );
        }
    }

    private void handle(DatagramPacket packet) {
        String content = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);
        try {
            Map<String, Object> message = OBJECT_MAPPER.readValue(content, REGISTER_TYPE);
            ParsedDiscoveryMessage parsedMessage = parseDiscoveryMessage(message);
            if (parsedMessage == null) {
                logMalformed(content);
                return;
            }
            if ("REGISTER".equals(parsedMessage.type())) {
                serviceRegistry.register(parsedMessage.instance());
                return;
            }
            serviceRegistry.heartbeat(parsedMessage.instance(), parsedMessage.timestampMillis());
        } catch (IOException exception) {
            logMalformed(content);
        }
    }

    private static ParsedDiscoveryMessage parseDiscoveryMessage(Map<String, Object> message) {
        Object type = message.get("type");
        Object service = message.get("service");
        Object host = message.get("host");
        Object port = message.get("port");
        Object instanceId = message.get("instanceId");
        if (!(type instanceof String rawType) || (!"REGISTER".equals(rawType) && !"HEARTBEAT".equals(rawType))) {
            return null;
        }
        if (!(service instanceof String rawService) || !(host instanceof String rawHost)
                || !(instanceId instanceof String rawInstanceId)) {
            return null;
        }

        Integer parsedPort = parsePort(port);
        if (parsedPort == null) {
            return null;
        }

        try {
            ServiceName serviceName = ServiceName.valueOf(rawService.trim().toUpperCase());
            Instance instance = new Instance(rawInstanceId.trim(), serviceName, rawHost.trim(), parsedPort);
            long timestamp = parseTimestamp(message.get("timestamp"));
            return new ParsedDiscoveryMessage(rawType, instance, timestamp);
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private static long parseTimestamp(Object timestampValue) {
        if (timestampValue instanceof Number number) {
            return number.longValue();
        }
        if (timestampValue instanceof String rawTimestamp) {
            try {
                return Long.parseLong(rawTimestamp.trim());
            } catch (NumberFormatException ignored) {
                return System.currentTimeMillis();
            }
        }
        return System.currentTimeMillis();
    }

    private static Integer parsePort(Object portValue) {
        if (portValue instanceof Integer port) {
            return port;
        }
        if (portValue instanceof String rawPort) {
            try {
                return Integer.parseInt(rawPort.trim());
            } catch (NumberFormatException exception) {
                return null;
            }
        }
        return null;
    }

    private static void logMalformed(String content) {
        System.err.printf("event=udp-registration-malformed payload=%s%n", content);
    }

    private static int resolveRegistrationPort(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--gateway.registration.port=")) {
                try {
                    return Integer.parseInt(arg.substring("--gateway.registration.port=".length()).trim());
                } catch (NumberFormatException ignored) {
                    return DEFAULT_REGISTRATION_PORT;
                }
            }
        }

        String registrationPortEnv = System.getenv("GATEWAY_REGISTRATION_PORT");
        if (registrationPortEnv != null && !registrationPortEnv.isBlank()) {
            try {
                return Integer.parseInt(registrationPortEnv.trim());
            } catch (NumberFormatException ignored) {
                return DEFAULT_REGISTRATION_PORT;
            }
        }
        return DEFAULT_REGISTRATION_PORT;
    }

    private record ParsedDiscoveryMessage(String type, Instance instance, long timestampMillis) {
    }
}
