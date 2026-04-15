package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ProtoCodec {
    private static final int WIRE_VARINT = 0;
    private static final int WIRE_LENGTH_DELIMITED = 2;

    private ProtoCodec() {
    }

    public static byte[] encodeRequest(Request request) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeString(out, 1, request.requestId());
        writeString(out, 2, request.service() == null ? "" : request.service().name());
        writeString(out, 3, request.action());
        writeMap(out, 4, request.payload());
        return out.toByteArray();
    }

    public static Request decodeRequest(byte[] bytes) {
        ProtoReader reader = new ProtoReader(bytes);
        String requestId = "";
        ServiceName service = null;
        String action = "";
        Map<String, Object> payload = new LinkedHashMap<>();
        while (reader.hasRemaining()) {
            ProtoField field = reader.readField();
            if (field.number() == 1 && field.wireType() == WIRE_LENGTH_DELIMITED) {
                requestId = reader.readString();
            } else if (field.number() == 2 && field.wireType() == WIRE_LENGTH_DELIMITED) {
                String rawService = reader.readString();
                if (!rawService.isBlank()) {
                    service = ServiceName.valueOf(rawService);
                }
            } else if (field.number() == 3 && field.wireType() == WIRE_LENGTH_DELIMITED) {
                action = reader.readString();
            } else if (field.number() == 4 && field.wireType() == WIRE_LENGTH_DELIMITED) {
                payload = reader.readMap();
            } else {
                reader.skipField(field.wireType());
            }
        }
        return new Request(requestId, service, action, payload);
    }

    public static byte[] encodeResponse(Response response) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(out, 1, response.statusCode());
        writeString(out, 2, response.message());
        writeMap(out, 3, response.payload());
        return out.toByteArray();
    }

    public static Response decodeResponse(byte[] bytes) {
        ProtoReader reader = new ProtoReader(bytes);
        int statusCode = 500;
        String message = "gRPC response";
        Map<String, Object> payload = new LinkedHashMap<>();
        while (reader.hasRemaining()) {
            ProtoField field = reader.readField();
            if (field.number() == 1 && field.wireType() == WIRE_VARINT) {
                statusCode = reader.readVarInt();
            } else if (field.number() == 2 && field.wireType() == WIRE_LENGTH_DELIMITED) {
                message = reader.readString();
            } else if (field.number() == 3 && field.wireType() == WIRE_LENGTH_DELIMITED) {
                payload = reader.readMap();
            } else {
                reader.skipField(field.wireType());
            }
        }
        return new Response(statusCode, message, payload);
    }

    private static void writeString(ByteArrayOutputStream out, int fieldNumber, String value) {
        if (value == null) {
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeTag(out, fieldNumber, WIRE_LENGTH_DELIMITED);
        writeVarInt(out, bytes.length);
        out.writeBytes(bytes);
    }

    private static void writeMap(ByteArrayOutputStream out, int fieldNumber, Map<String, Object> map) {
        Map<String, Object> source = map == null ? Map.of() : map;
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            ByteArrayOutputStream pair = new ByteArrayOutputStream();
            writeString(pair, 1, entry.getKey());
            writeString(pair, 2, entry.getValue() == null ? "null" : entry.getValue().toString());
            writeTag(payload, 1, WIRE_LENGTH_DELIMITED);
            byte[] pairBytes = pair.toByteArray();
            writeVarInt(payload, pairBytes.length);
            payload.writeBytes(pairBytes);
        }
        writeTag(out, fieldNumber, WIRE_LENGTH_DELIMITED);
        byte[] payloadBytes = payload.toByteArray();
        writeVarInt(out, payloadBytes.length);
        out.writeBytes(payloadBytes);
    }

    private static void writeInt(ByteArrayOutputStream out, int fieldNumber, int value) {
        writeTag(out, fieldNumber, WIRE_VARINT);
        writeVarInt(out, value);
    }

    private static void writeTag(ByteArrayOutputStream out, int fieldNumber, int wireType) {
        writeVarInt(out, (fieldNumber << 3) | wireType);
    }

    private static void writeVarInt(ByteArrayOutputStream out, int value) {
        long unsigned = value & 0xFFFFFFFFL;
        while ((unsigned & ~0x7FL) != 0L) {
            out.write((int) ((unsigned & 0x7F) | 0x80));
            unsigned >>>= 7;
        }
        out.write((int) unsigned);
    }

    private record ProtoField(int number, int wireType) { }

    private static final class ProtoReader {
        private final ByteBuffer buffer;

        private ProtoReader(byte[] bytes) {
            this.buffer = ByteBuffer.wrap(bytes);
        }

        private boolean hasRemaining() {
            return buffer.hasRemaining();
        }

        private ProtoField readField() {
            int tag = readVarInt();
            return new ProtoField(tag >>> 3, tag & 0x07);
        }

        private int readVarInt() {
            int shift = 0;
            int result = 0;
            while (shift < 32) {
                if (!buffer.hasRemaining()) {
                    throw new IllegalArgumentException("Malformed varint");
                }
                int value = Byte.toUnsignedInt(buffer.get());
                result |= (value & 0x7F) << shift;
                if ((value & 0x80) == 0) {
                    return result;
                }
                shift += 7;
            }
            throw new IllegalArgumentException("Malformed varint");
        }

        private String readString() {
            int length = readVarInt();
            if (length < 0 || buffer.remaining() < length) {
                throw new IllegalArgumentException("Malformed length-delimited field");
            }
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private Map<String, Object> readMap() {
            int size = readVarInt();
            if (size < 0 || buffer.remaining() < size) {
                throw new IllegalArgumentException("Malformed map payload");
            }
            ByteBuffer slice = ByteBuffer.wrap(buffer.array(), buffer.position(), size);
            buffer.position(buffer.position() + size);
            Map<String, Object> result = new LinkedHashMap<>();
            ProtoReader mapReader = new ProtoReader(copy(slice));
            while (mapReader.hasRemaining()) {
                ProtoField pairField = mapReader.readField();
                if (pairField.number() != 1 || pairField.wireType() != WIRE_LENGTH_DELIMITED) {
                    mapReader.skipField(pairField.wireType());
                    continue;
                }
                int pairLength = mapReader.readVarInt();
                if (pairLength < 0 || pairLength > mapReader.buffer.remaining()) {
                    throw new IllegalArgumentException("Malformed map entry");
                }
                byte[] pairBytes = new byte[pairLength];
                mapReader.buffer.get(pairBytes);
                ProtoReader pairReader = new ProtoReader(pairBytes);
                String key = "";
                String value = "";
                while (pairReader.hasRemaining()) {
                    ProtoField keyOrValue = pairReader.readField();
                    if (keyOrValue.number() == 1 && keyOrValue.wireType() == WIRE_LENGTH_DELIMITED) {
                        key = pairReader.readString();
                    } else if (keyOrValue.number() == 2 && keyOrValue.wireType() == WIRE_LENGTH_DELIMITED) {
                        value = pairReader.readString();
                    } else {
                        pairReader.skipField(keyOrValue.wireType());
                    }
                }
                if (!key.isBlank()) {
                    result.put(key, value);
                }
            }
            return result;
        }

        private void skipField(int wireType) {
            if (wireType == WIRE_VARINT) {
                readVarInt();
                return;
            }
            if (wireType == WIRE_LENGTH_DELIMITED) {
                int length = readVarInt();
                if (length < 0 || buffer.remaining() < length) {
                    throw new IllegalArgumentException("Malformed field length");
                }
                buffer.position(buffer.position() + length);
                return;
            }
            throw new IllegalArgumentException("Unsupported wire type: " + wireType);
        }

        private static byte[] copy(ByteBuffer source) {
            byte[] bytes = new byte[source.remaining()];
            source.get(bytes);
            return bytes;
        }
    }
}
