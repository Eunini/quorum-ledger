package io.github.eunini.quorumledger.client;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/** Server-side reply encoders for tests (the client itself only decodes replies). */
final class TestFrames {
    private TestFrames() {}

    private static ByteBuffer header(int extra, BigInteger clientId, long requestNumber) {
        ByteBuffer b = ByteBuffer.allocate(1 + 16 + 8 + extra).order(ByteOrder.LITTLE_ENDIAN);
        b.put(Codec.TAG_CLIENT_REPLY);
        Codec.putU128(b, clientId);
        b.putLong(requestNumber);
        return b;
    }

    static byte[] resultsReply(BigInteger clientId, long requestNumber, List<ResultCode> codes) {
        ByteBuffer b = header(1 + 1 + 4 + 4 * codes.size(), clientId, requestNumber);
        b.put((byte) 0).put((byte) 1).putInt(codes.size());
        codes.forEach(c -> b.putInt(c.code()));
        return b.array();
    }

    static byte[] notLeader(BigInteger clientId, long requestNumber, int hint) {
        ByteBuffer b = header(2, clientId, requestNumber);
        b.put((byte) 1).put((byte) hint);
        return b.array();
    }

    static byte[] accountsReply(BigInteger clientId, long requestNumber, List<Account> accounts) {
        ByteBuffer b = header(1 + 1 + 4 + Codec.ACCOUNT_LEN * accounts.size(), clientId, requestNumber);
        b.put((byte) 0).put((byte) 2).putInt(accounts.size());
        for (Account a : accounts) {
            Codec.putU128(b, a.id());
            Codec.putU128(b, a.debitsPending());
            Codec.putU128(b, a.debitsPosted());
            Codec.putU128(b, a.creditsPending());
            Codec.putU128(b, a.creditsPosted());
            b.putInt(a.ledger()).putShort(a.code()).putShort(a.flags()).putLong(a.timestamp());
        }
        return b.array();
    }

    static byte[] withLength(byte[] body) {
        ByteBuffer b = ByteBuffer.allocate(4 + body.length).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(body.length).put(body);
        return b.array();
    }
}
