package io.github.eunini.quorumledger.client;

public final class AccountFlags {
    /** debits_pending + debits_posted must never exceed credits_posted. */
    public static final short DEBITS_MUST_NOT_EXCEED_CREDITS = 1;
    /** credits_pending + credits_posted must never exceed debits_posted. */
    public static final short CREDITS_MUST_NOT_EXCEED_DEBITS = 2;

    private AccountFlags() {}
}
