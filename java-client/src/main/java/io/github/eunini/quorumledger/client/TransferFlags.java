package io.github.eunini.quorumledger.client;

public final class TransferFlags {
    public static final short PENDING = 1;
    public static final short POST_PENDING = 2;
    public static final short VOID_PENDING = 4;

    private TransferFlags() {}
}
