package io.github.eunini.quorumledger.client;

import java.math.BigInteger;

/** Stored transfer with resolved fields (post/void transfers inherit accounts from the hold). */
public record Transfer(
        BigInteger id,
        BigInteger debitAccountId,
        BigInteger creditAccountId,
        BigInteger amount,
        BigInteger pendingId,
        int ledger,
        short code,
        short flags,
        int timeoutSeconds,
        long timestamp) {}
