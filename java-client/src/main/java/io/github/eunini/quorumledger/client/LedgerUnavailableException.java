package io.github.eunini.quorumledger.client;

/** No replica answered the request before the request timeout. */
public class LedgerUnavailableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public LedgerUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
