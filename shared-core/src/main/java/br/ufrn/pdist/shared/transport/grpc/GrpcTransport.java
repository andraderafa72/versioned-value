package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Instance;
import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.RequestHandler;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import br.ufrn.pdist.shared.logging.EventLog;
import br.ufrn.pdist.shared.transport.grpc.v1.TransportGatewayGrpc;
import br.ufrn.pdist.shared.transport.grpc.v1.TransportRequest;
import br.ufrn.pdist.shared.transport.grpc.v1.TransportResponse;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class GrpcTransport implements TransportLayer {
    private static final long CLIENT_DEADLINE_MILLIS = 2_000;
    private final Map<Integer, Server> runningServers = new ConcurrentHashMap<>();

    @Override
    public void startServer(int port, RequestHandler handler) {
        if (runningServers.containsKey(port)) {
            return;
        }
        try {
            Server server = ServerBuilder.forPort(port)
                    .addService(new TransportGatewayGrpc.TransportGatewayImplBase() {
                        @Override
                        public void handle(TransportRequest request, StreamObserver<TransportResponse> responseObserver) {
                            Request domainRequest = null;
                            try {
                                domainRequest = toDomainRequest(request);
                                Response domainResponse = handler.handle(domainRequest);
                                if (domainResponse == null) {
                                    domainResponse = transportError(500, "Handler returned null response", domainRequest.requestId());
                                }
                                responseObserver.onNext(toProtoResponse(domainResponse, domainRequest.requestId()));
                                responseObserver.onCompleted();
                            } catch (Exception exception) {
                                String requestId = domainRequest == null ? fallbackRequestId(request.getRequestId()) : domainRequest.requestId();
                                Response error = transportError(500, "Handler failure: " + exception.getMessage(), requestId);
                                responseObserver.onNext(toProtoResponse(error, requestId));
                                responseObserver.onCompleted();
                            }
                        }
                    })
                    .build()
                    .start();
            runningServers.put(port, server);
            System.out.printf("transport=grpc event=server-started port=%d%n", port);
            Thread awaitThread = new Thread(() -> awaitServer(server, port), "grpc-server-" + port);
            awaitThread.setDaemon(false);
            awaitThread.start();
        } catch (IOException ioException) {
            EventLog.printlnWithStackTrace(
                    String.format(
                            "transport=grpc event=server-failure port=%d error=\"%s\"",
                            port,
                            ioException.getMessage()
                    ),
                    ioException
            );
        }
    }

    @Override
    public Response send(Request request, Instance target) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(target.host(), target.port())
                .usePlaintext()
                .build();
        try {
            TransportGatewayGrpc.TransportGatewayBlockingStub stub = TransportGatewayGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(CLIENT_DEADLINE_MILLIS, TimeUnit.MILLISECONDS);
            TransportResponse response = stub.handle(toProtoRequest(request));
            return toDomainResponse(response, request.requestId());
        } catch (StatusRuntimeException statusRuntimeException) {
            return grpcTransportError(statusRuntimeException.getStatus(), request.requestId());
        } finally {
            channel.shutdownNow();
        }
    }

    private static TransportRequest toProtoRequest(Request request) {
        return TransportRequest.newBuilder()
                .setRequestId(request.requestId() == null ? "" : request.requestId())
                .setService(request.service() == null ? "" : request.service().name())
                .setAction(request.action() == null ? "" : request.action())
                .setPayload(toProtoStruct(request.payload()))
                .build();
    }

    private static TransportResponse toProtoResponse(Response response, String requestId) {
        Map<String, Object> payload = new LinkedHashMap<>();
        if (response.payload() != null) {
            payload.putAll(response.payload());
        }
        payload.putIfAbsent("requestId", requestId);
        payload.putIfAbsent("message", response.message());
        return TransportResponse.newBuilder()
                .setStatusCode(response.statusCode())
                .setMessage(response.message() == null ? "" : response.message())
                .setPayload(toProtoStruct(payload))
                .build();
    }

    private static Request toDomainRequest(TransportRequest request) {
        ServiceName serviceName = null;
        if (!request.getService().isBlank()) {
            serviceName = ServiceName.valueOf(request.getService());
        }
        String requestId = fallbackRequestId(request.getRequestId());
        return new Request(
                requestId,
                serviceName,
                request.getAction(),
                toMap(request.getPayload())
        );
    }

    private static Response toDomainResponse(TransportResponse response, String fallbackRequestId) {
        Map<String, Object> payload = toMap(response.getPayload());
        payload.putIfAbsent("requestId", fallbackRequestId);
        String message = response.getMessage().isBlank() ? "gRPC " + response.getStatusCode() : response.getMessage();
        return new Response(response.getStatusCode(), message, payload);
    }

    private static Struct toProtoStruct(Map<String, Object> payload) {
        Struct.Builder builder = Struct.newBuilder();
        if (payload == null) {
            return builder.build();
        }
        payload.forEach((key, value) -> builder.putFields(key, toProtoValue(value)));
        return builder.build();
    }

    private static Value toProtoValue(Object value) {
        Value.Builder builder = Value.newBuilder();
        if (value == null) {
            return builder.setNullValueValue(0).build();
        }
        if (value instanceof Boolean booleanValue) {
            return builder.setBoolValue(booleanValue).build();
        }
        if (value instanceof Number numberValue) {
            return builder.setNumberValue(numberValue.doubleValue()).build();
        }
        return builder.setStringValue(value.toString()).build();
    }

    private static Map<String, Object> toMap(Struct struct) {
        Map<String, Object> payload = new LinkedHashMap<>();
        if (struct == null) {
            return payload;
        }
        struct.getFieldsMap().forEach((key, value) -> payload.put(key, fromProtoValue(value)));
        return payload;
    }

    private static Object fromProtoValue(Value value) {
        return switch (value.getKindCase()) {
            case BOOL_VALUE -> value.getBoolValue();
            case NUMBER_VALUE -> value.getNumberValue();
            case STRING_VALUE -> value.getStringValue();
            case STRUCT_VALUE -> toMap(value.getStructValue());
            case LIST_VALUE -> value.getListValue().toString();
            case NULL_VALUE, KIND_NOT_SET -> null;
        };
    }

    private static Response grpcTransportError(io.grpc.Status status, String requestId) {
        String description = status.getDescription() == null ? status.getCode().name() : status.getDescription();
        return new Response(502, description, Map.of(
                "requestId", requestId,
                "grpc-status", status.getCode().name()
        ));
    }

    private static Response transportError(int statusCode, String message, String requestId) {
        return new Response(statusCode, message, Map.of(
                "requestId", requestId,
                "message", message
        ));
    }

    private static String fallbackRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return UUID.randomUUID().toString();
        }
        return requestId;
    }

    private void awaitServer(Server server, int port) {
        try {
            server.awaitTermination();
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            System.out.printf("transport=grpc event=server-stopped port=%d%n", port);
        }
    }
}
