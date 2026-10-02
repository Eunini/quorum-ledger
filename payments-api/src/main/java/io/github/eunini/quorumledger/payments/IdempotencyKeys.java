package io.github.eunini.quorumledger.payments;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Maps an HTTP Idempotency-Key to a ledger object id.
 *
 * <p>The id is the first 128 bits of SHA-256(scope || 0x00 || key). Because the ledger treats "same id,
 * same body" as success and "same id, different body" as a conflict, a deterministic id is all the API
 * needs for idempotency: no separate idempotency table, and it holds across API instances and restarts.
 * The scope keeps keys used on different endpoints (e.g. a hold and its capture) from colliding.
 */
public final class IdempotencyKeys {
    public static final int MAX_KEY_LENGTH = 255;

    private IdempotencyKeys() {}

    public static BigInteger idFor(String scope, String key) {
        if (key == null || key.isBlank() || key.length() > MAX_KEY_LENGTH) {
            throw new InvalidIdempotencyKeyException();
        }
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            sha.update(scope.getBytes(StandardCharsets.UTF_8));
            sha.update((byte) 0);
            sha.update(key.getBytes(StandardCharsets.UTF_8));
            BigInteger id = new BigInteger(1, Arrays.copyOf(sha.digest(), 16));
            // Zero is reserved by the ledger; astronomically unlikely, but keep the mapping total.
            return id.signum() == 0 ? BigInteger.ONE : id;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static class InvalidIdempotencyKeyException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public InvalidIdempotencyKeyException() {
            super("Idempotency-Key header must be 1-" + MAX_KEY_LENGTH + " characters");
        }
    }
}
