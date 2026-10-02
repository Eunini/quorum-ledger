package io.github.eunini.quorumledger.payments;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class IdempotencyKeysTest {
    @Test
    void sameKeySameId() {
        assertEquals(IdempotencyKeys.idFor("transfer", "order-42"), IdempotencyKeys.idFor("transfer", "order-42"));
    }

    @Test
    void scopesAndKeysSeparateIds() {
        assertNotEquals(IdempotencyKeys.idFor("transfer", "k"), IdempotencyKeys.idFor("hold", "k"));
        assertNotEquals(IdempotencyKeys.idFor("transfer", "k1"), IdempotencyKeys.idFor("transfer", "k2"));
        // The separator prevents ("ab","c") and ("a","bc") from colliding.
        assertNotEquals(IdempotencyKeys.idFor("ab", "c"), IdempotencyKeys.idFor("a", "bc"));
    }

    @Test
    void idsAreNonZeroUnsigned128Bit() {
        for (int i = 0; i < 1000; i++) {
            BigInteger id = IdempotencyKeys.idFor("transfer", "key-" + i);
            assertTrue(id.signum() > 0);
            assertTrue(id.bitLength() <= 128);
        }
    }

    @Test
    void rejectsBlankOrHugeKeys() {
        assertThrows(IdempotencyKeys.InvalidIdempotencyKeyException.class, () -> IdempotencyKeys.idFor("t", " "));
        assertThrows(IdempotencyKeys.InvalidIdempotencyKeyException.class, () -> IdempotencyKeys.idFor("t", "x".repeat(256)));
    }
}
