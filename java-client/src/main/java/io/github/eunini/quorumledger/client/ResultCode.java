package io.github.eunini.quorumledger.client;

/** Per-event result of create_accounts / create_transfers. Numeric values are part of the wire protocol. */
public enum ResultCode {
    OK(0),
    EXISTS(1),
    EXISTS_WITH_DIFFERENT_FIELDS(2),
    ID_MUST_NOT_BE_ZERO(3),
    LEDGER_MUST_NOT_BE_ZERO(4),
    FLAGS_INVALID(5),
    ACCOUNTS_MUST_BE_DIFFERENT(6),
    DEBIT_ACCOUNT_NOT_FOUND(7),
    CREDIT_ACCOUNT_NOT_FOUND(8),
    ACCOUNTS_MUST_HAVE_SAME_LEDGER(9),
    TRANSFER_MUST_HAVE_SAME_LEDGER_AS_ACCOUNTS(10),
    AMOUNT_MUST_NOT_BE_ZERO(11),
    EXCEEDS_CREDITS(12),
    EXCEEDS_DEBITS(13),
    OVERFLOW(14),
    PENDING_ID_MUST_BE_ZERO(15),
    PENDING_ID_MUST_NOT_BE_ZERO(16),
    PENDING_ID_MUST_BE_DIFFERENT(17),
    PENDING_TRANSFER_NOT_FOUND(18),
    PENDING_TRANSFER_NOT_PENDING(19),
    PENDING_TRANSFER_ALREADY_POSTED(20),
    PENDING_TRANSFER_ALREADY_VOIDED(21),
    PENDING_TRANSFER_EXPIRED(22),
    PENDING_TRANSFER_FIELD_MISMATCH(23),
    POST_AMOUNT_EXCEEDS_PENDING_AMOUNT(24),
    VOID_AMOUNT_MUST_MATCH_PENDING_AMOUNT(25),
    TIMEOUT_RESERVED_FOR_PENDING_TRANSFER(26);

    private static final ResultCode[] BY_CODE = new ResultCode[27];

    static {
        for (ResultCode c : values()) {
            BY_CODE[c.code] = c;
        }
    }

    private final int code;

    ResultCode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** OK and EXISTS both mean the object is stored exactly as requested. */
    public boolean isSuccess() {
        return this == OK || this == EXISTS;
    }

    public static ResultCode fromCode(int code) {
        if (code < 0 || code >= BY_CODE.length) {
            throw new ProtocolException("unknown result code " + code);
        }
        return BY_CODE[code];
    }
}
