package io.github.eunini.quorumledger.payments;

import io.github.eunini.quorumledger.client.ResultCode;

/** The ledger refused the operation with a non-success result code. */
public class LedgerRejectedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final ResultCode code;

    public LedgerRejectedException(ResultCode code) {
        super("ledger rejected the operation: " + code);
        this.code = code;
    }

    public ResultCode code() {
        return code;
    }
}
