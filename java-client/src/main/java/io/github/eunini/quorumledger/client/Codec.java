package io.github.eunini.quorumledger.client;

import java.math.BigInteger;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Encoder/decoder for the quorum-ledger wire protocol (see docs/protocol.md). Every frame is
 * {@code u32 little-endian length || tag || body}; all integers are little-endian and 128-bit
 * integers are 16 bytes, least significant first.
 */
public final class Codec {
    static final byte TAG_HELLO = 0x01;
    static final byte TAG_CLIENT_REQUEST = 0x20;
    static final byte TAG_CLIENT_REPLY = 0x21;

    static final int NEW_ACCOUNT_LEN = 16 + 4 + 2 + 2;
    static final int ACCOUNT_LEN = 16 * 5 + 4 + 2 + 2 + 8;
    static final int NEW_TRANSFER_LEN = 16 * 5 + 4 + 2 + 2 + 4;
    static final int TRANSFER_LEN = NEW_TRANSFER_LEN + 8;

    private static final BigInteger U128_LIMIT = BigInteger.ONE.shiftLeft(128);
    private static final BigInteger U64_MASK = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    private Codec() {}

    /** Hello frame identifying this connection as a client. */
    public static byte[] hello() {
        ByteBuffer b = frame(3);
        b.put(TAG_HELLO).put((byte) 1).put((byte) 0);
        return b.array();
    }

    public static byte[] encodeRequest(BigInteger clientId, long requestNumber, Operation op) {
        if (op.eventCount() > Operation.MAX_EVENTS) {
            throw new IllegalArgumentException("at most " + Operation.MAX_EVENTS + " events per request");
        }
        int itemLen =
                switch (op) {
                    case Operation.CreateAccounts a -> NEW_ACCOUNT_LEN;
                    case Operation.CreateTransfers t -> NEW_TRANSFER_LEN;
                    case Operation.LookupAccounts l -> 16;
                    case Operation.LookupTransfers l -> 16;
                };
        int bodyLen = 1 + 16 + 8 + 1 + 4 + itemLen * op.eventCount();
        ByteBuffer b = frame(bodyLen);
        b.put(TAG_CLIENT_REQUEST);
        putU128(b, clientId);
        b.putLong(requestNumber);
        b.put(op.code());
        b.putInt(op.eventCount());
        switch (op) {
            case Operation.CreateAccounts a -> {
                for (NewAccount x : a.accounts()) {
                    putU128(b, x.id());
                    b.putInt(x.ledger());
                    b.putShort(x.code());
                    b.putShort(x.flags());
                }
            }
            case Operation.CreateTransfers t -> {
                for (NewTransfer x : t.transfers()) {
                    putU128(b, x.id());
                    putU128(b, x.debitAccountId());
                    putU128(b, x.creditAccountId());
                    putU128(b, x.amount());
                    putU128(b, x.pendingId());
                    b.putInt(x.ledger());
                    b.putShort(x.code());
                    b.putShort(x.flags());
                    b.putInt(x.timeoutSeconds());
                }
            }
            case Operation.LookupAccounts l -> l.ids().forEach(id -> putU128(b, id));
            case Operation.LookupTransfers l -> l.ids().forEach(id -> putU128(b, id));
        }
        if (b.hasRemaining()) {
            throw new IllegalStateException("encoded length mismatch");
        }
        return b.array();
    }

    /** Decodes a frame body (without the length prefix) that must be a ClientReply. */
    public static Reply decodeReply(byte[] body) {
        ByteBuffer b = ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN);
        try {
            if (b.get() != TAG_CLIENT_REPLY) {
                throw new ProtocolException("expected ClientReply frame");
            }
            BigInteger clientId = getU128(b);
            long requestNumber = b.getLong();
            byte status = b.get();
            Reply.Body replyBody;
            if (status == 0) {
                byte kind = b.get();
                int n = count(b, kind == 1 ? 4 : kind == 2 ? ACCOUNT_LEN : TRANSFER_LEN);
                replyBody =
                        switch (kind) {
                            case 1 -> {
                                List<ResultCode> r = new ArrayList<>(n);
                                for (int i = 0; i < n; i++) {
                                    r.add(ResultCode.fromCode(b.getInt()));
                                }
                                yield new Reply.Results(r);
                            }
                            case 2 -> {
                                List<Account> r = new ArrayList<>(n);
                                for (int i = 0; i < n; i++) {
                                    r.add(new Account(
                                            getU128(b), getU128(b), getU128(b), getU128(b), getU128(b),
                                            b.getInt(), b.getShort(), b.getShort(), b.getLong()));
                                }
                                yield new Reply.Accounts(r);
                            }
                            case 3 -> {
                                List<Transfer> r = new ArrayList<>(n);
                                for (int i = 0; i < n; i++) {
                                    r.add(new Transfer(
                                            getU128(b), getU128(b), getU128(b), getU128(b), getU128(b),
                                            b.getInt(), b.getShort(), b.getShort(), b.getInt(), b.getLong()));
                                }
                                yield new Reply.Transfers(r);
                            }
                            default -> throw new ProtocolException("unknown reply body kind " + kind);
                        };
            } else if (status == 1) {
                int hint = Byte.toUnsignedInt(b.get());
                replyBody = new Reply.NotLeader(hint == 0xFF ? -1 : hint);
            } else {
                throw new ProtocolException("unknown reply status " + status);
            }
            if (b.hasRemaining()) {
                throw new ProtocolException("trailing bytes in reply");
            }
            return new Reply(clientId, requestNumber, replyBody);
        } catch (BufferUnderflowException e) {
            throw new ProtocolException("truncated reply");
        }
    }

    private static int count(ByteBuffer b, int minItemLen) {
        long n = Integer.toUnsignedLong(b.getInt());
        if (n * minItemLen > b.remaining()) {
            throw new ProtocolException("element count exceeds frame");
        }
        return (int) n;
    }

    private static ByteBuffer frame(int bodyLen) {
        ByteBuffer b = ByteBuffer.allocate(4 + bodyLen).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(bodyLen);
        return b;
    }

    static void putU128(ByteBuffer b, BigInteger v) {
        if (v == null || v.signum() < 0 || v.compareTo(U128_LIMIT) >= 0) {
            throw new IllegalArgumentException("value out of u128 range: " + v);
        }
        b.putLong(v.and(U64_MASK).longValue());
        b.putLong(v.shiftRight(64).longValue());
    }

    static BigInteger getU128(ByteBuffer b) {
        long lo = b.getLong();
        long hi = b.getLong();
        return unsigned(hi).shiftLeft(64).or(unsigned(lo));
    }

    private static BigInteger unsigned(long v) {
        BigInteger x = BigInteger.valueOf(v);
        return v < 0 ? x.add(BigInteger.ONE.shiftLeft(64)) : x;
    }
}
