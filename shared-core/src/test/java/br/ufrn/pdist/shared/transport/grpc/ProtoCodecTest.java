package br.ufrn.pdist.shared.transport;

import br.ufrn.pdist.shared.contracts.Request;
import br.ufrn.pdist.shared.contracts.Response;
import br.ufrn.pdist.shared.contracts.ServiceName;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtoCodecTest {
    @Test
    void shouldEncodeAndDecodeRequest() {
        Request original = new Request("r1", ServiceName.ACCOUNT, "POST /accounts", Map.of("amount", "100", "requestId", "r1"));
        byte[] encoded = ProtoCodec.encodeRequest(original);
        Request decoded = ProtoCodec.decodeRequest(encoded);
        assertEquals("r1", decoded.requestId());
        assertEquals(ServiceName.ACCOUNT, decoded.service());
        assertEquals("POST /accounts", decoded.action());
        assertEquals("100", decoded.payload().get("amount"));
    }

    @Test
    void shouldEncodeAndDecodeResponse() {
        Response original = new Response(200, "ok", Map.of("message", "ok", "requestId", "r2"));
        byte[] encoded = ProtoCodec.encodeResponse(original);
        Response decoded = ProtoCodec.decodeResponse(encoded);
        assertEquals(200, decoded.statusCode());
        assertEquals("ok", decoded.message());
        assertEquals("r2", decoded.payload().get("requestId"));
    }
}
