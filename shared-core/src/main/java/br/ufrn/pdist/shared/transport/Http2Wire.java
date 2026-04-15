package br.ufrn.pdist.shared.transport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public final class Http2Wire {

    public static final byte FRAME_DATA = 0x00;
    public static final byte FRAME_HEADERS = 0x01;
    public static final byte FRAME_TRAILERS = 0x03;

    private Http2Wire() {
    }

    public static void writeFrame(BufferedOutputStream output, int streamId, byte frameType, byte[] payload) throws IOException {
        output.write(frameType);
        output.write(intToBytes(streamId));
        output.write(intToBytes(payload.length));
        output.write(payload);
    }

    public static Frame readFrame(BufferedInputStream input) throws IOException {
        int type = input.read();
        if (type < 0) {
            throw new Http2ProtocolException("unexpected end of stream");
        }
        int streamId = ByteBuffer.wrap(readExactly(input, 4)).getInt();
        int payloadLength = ByteBuffer.wrap(readExactly(input, 4)).getInt();
        if (payloadLength < 0) {
            throw new Http2ProtocolException("invalid frame length");
        }
        byte[] payload = readExactly(input, payloadLength);
        return new Frame((byte) type, streamId, payload);
    }

    public static byte[] encodeHeaders(Map<String, String> headers) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            builder.append(entry.getKey()).append('\t').append(entry.getValue()).append('\n');
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static Map<String, String> decodeHeaders(byte[] payload) {
        String raw = new String(payload, StandardCharsets.UTF_8);
        Map<String, String> result = new LinkedHashMap<>();
        for (String line : raw.split("\n")) {
            if (line.isBlank()) {
                continue;
            }
            int separator = line.indexOf('\t');
            if (separator <= 0) {
                continue;
            }
            String name = line.substring(0, separator).trim().toLowerCase(Locale.ROOT);
            String value = line.substring(separator + 1).trim();
            result.put(name, value);
        }
        return result;
    }

    public static byte[] readExactly(BufferedInputStream input, int length) throws IOException {
        byte[] payload = new byte[length];
        int totalRead = 0;
        while (totalRead < length) {
            int read = input.read(payload, totalRead, length - totalRead);
            if (read < 0) {
                throw new Http2ProtocolException("unexpected end of stream");
            }
            totalRead += read;
        }
        return payload;
    }

    private static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public record Frame(byte type, int streamId, byte[] payload) {
    }

    public static final class Http2ProtocolException extends RuntimeException {
        public Http2ProtocolException(String message) {
            super(message);
        }
    }
}
