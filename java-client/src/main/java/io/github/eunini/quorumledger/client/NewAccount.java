package io.github.eunini.quorumledger.client;

import java.math.BigInteger;

/** Request to create an account; balances start at zero. Ids are unsigned 128-bit integers. */
public record NewAccount(BigInteger id, int ledger, short code, short flags) {}
