package io.github.eunini.quorumledger.client;

import java.math.BigInteger;

/** Stored account. All amounts are unsigned 128-bit integers. */
public record Account(
        BigInteger id,
        BigInteger debitsPending,
        BigInteger debitsPosted,
        BigInteger creditsPending,
        BigInteger creditsPosted,
        int ledger,
        short code,
        short flags,
        long timestamp) {}
