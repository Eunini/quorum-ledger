package io.github.eunini.quorumledger.client;

import java.math.BigInteger;

/**
 * Request to create a transfer. For POST_PENDING / VOID_PENDING, {@code pendingId} names the hold and the
 * account ids, ledger and code may be zero (inherited from the hold).
 */
public record NewTransfer(
        BigInteger id,
        BigInteger debitAccountId,
        BigInteger creditAccountId,
        BigInteger amount,
        BigInteger pendingId,
        int ledger,
        short code,
        short flags,
        int timeoutSeconds) {

    public static NewTransfer single(BigInteger id, BigInteger debit, BigInteger credit, BigInteger amount, int ledger, short code) {
        return new NewTransfer(id, debit, credit, amount, BigInteger.ZERO, ledger, code, (short) 0, 0);
    }

    public static NewTransfer hold(
            BigInteger id, BigInteger debit, BigInteger credit, BigInteger amount, int ledger, short code, int timeoutSeconds) {
        return new NewTransfer(id, debit, credit, amount, BigInteger.ZERO, ledger, code, TransferFlags.PENDING, timeoutSeconds);
    }

    /** Captures {@code amount} of the hold; zero captures the full held amount. */
    public static NewTransfer capture(BigInteger id, BigInteger pendingId, BigInteger amount) {
        return new NewTransfer(
                id, BigInteger.ZERO, BigInteger.ZERO, amount, pendingId, 0, (short) 0, TransferFlags.POST_PENDING, 0);
    }

    public static NewTransfer voidHold(BigInteger id, BigInteger pendingId) {
        return new NewTransfer(
                id, BigInteger.ZERO, BigInteger.ZERO, BigInteger.ZERO, pendingId, 0, (short) 0, TransferFlags.VOID_PENDING, 0);
    }
}
