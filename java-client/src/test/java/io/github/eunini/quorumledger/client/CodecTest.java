package io.github.eunini.quorumledger.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

class CodecTest {

    /**
     * Same request and expected bytes as {@code golden_client_request_bytes} in
     * crates/consensus/src/message.rs: proves the Java and Rust encoders agree byte for byte.
     */
    static final String GOLDEN_REQUEST_HEX = "7a000000"
            + "20"
            + "100f0e0d0c0b0a090807060504030201"
            + "0700000000000000"
            + "02"
            + "01000000"
            + "01000000000000000000000000000000"
            + "02000000000000000000000000000000"
            + "03000000000000000000000000000000"
            + "e8030000000000000000000000000000"
            + "00000000000000000000000000000000"
            + "48030000"
            + "0100"
            + "0100"
            + "1e000000";

    @Test
    void requestEncodingMatchesRustGoldenBytes() {
        BigInteger clientId = new BigInteger("0102030405060708090a0b0c0d0e0f10", 16);
        NewTransfer t = NewTransfer.hold(
                BigInteger.ONE, BigInteger.TWO, BigInteger.valueOf(3), BigInteger.valueOf(1000), 840, (short) 1, 30);
        byte[] wire = Codec.encodeRequest(clientId, 7, new Operation.CreateTransfers(List.of(t)));
        assertEquals(GOLDEN_REQUEST_HEX, HexFormat.of().formatHex(wire));
    }

    @Test
    void u128RoundTripsAtTheEdges() {
        BigInteger max = BigInteger.ONE.shiftLeft(128).subtract(BigInteger.ONE);
        for (BigInteger v : List.of(BigInteger.ZERO, BigInteger.ONE, BigInteger.ONE.shiftLeft(63), BigInteger.ONE.shiftLeft(64), max)) {
            ByteBuffer b = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            Codec.putU128(b, v);
            b.flip();
            assertEquals(v, Codec.getU128(b));
        }
        ByteBuffer b = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        assertThrows(IllegalArgumentException.class, () -> Codec.putU128(b, max.add(BigInteger.ONE)));
        assertThrows(IllegalArgumentException.class, () -> Codec.putU128(b, BigInteger.valueOf(-1)));
    }

    @Test
    void decodesResultsReply() {
        byte[] body = TestFrames.resultsReply(BigInteger.TEN, 3, List.of(ResultCode.OK, ResultCode.EXCEEDS_CREDITS));
        Reply r = Codec.decodeReply(body);
        assertEquals(BigInteger.TEN, r.clientId());
        assertEquals(3, r.requestNumber());
        assertEquals(new Reply.Results(List.of(ResultCode.OK, ResultCode.EXCEEDS_CREDITS)), r.body());
    }

    @Test
    void decodesNotLeaderWithAndWithoutHint() {
        assertEquals(new Reply.NotLeader(2), Codec.decodeReply(TestFrames.notLeader(BigInteger.ONE, 1, 2)).body());
        assertEquals(new Reply.NotLeader(-1), Codec.decodeReply(TestFrames.notLeader(BigInteger.ONE, 1, 0xFF)).body());
    }

    @Test
    void decodesAccountsReply() {
        Account a = new Account(BigInteger.valueOf(5), BigInteger.ONE, BigInteger.TWO, BigInteger.valueOf(3),
                BigInteger.ONE.shiftLeft(100), 840, (short) 7, AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS, 123456789L);
        Reply r = Codec.decodeReply(TestFrames.accountsReply(BigInteger.ONE, 9, List.of(a)));
        assertEquals(new Reply.Accounts(List.of(a)), r.body());
    }

    @Test
    void rejectsTruncatedAndTrailingBytes() {
        byte[] body = TestFrames.resultsReply(BigInteger.TEN, 3, List.of(ResultCode.OK));
        for (int cut = 0; cut < body.length; cut++) {
            byte[] shorter = java.util.Arrays.copyOf(body, cut);
            assertThrows(ProtocolException.class, () -> Codec.decodeReply(shorter), "cut at " + cut);
        }
        byte[] longer = java.util.Arrays.copyOf(body, body.length + 1);
        assertThrows(ProtocolException.class, () -> Codec.decodeReply(longer));
    }

    @Test
    void helloFrame() {
        assertArrayEquals(new byte[] {3, 0, 0, 0, 0x01, 1, 0}, Codec.hello());
    }
}
