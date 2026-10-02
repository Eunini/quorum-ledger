package io.github.eunini.quorumledger.client;

/** The peer sent bytes that do not follow the wire protocol. */
public class ProtocolException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ProtocolException(String message) {
        super(message);
    }
}
