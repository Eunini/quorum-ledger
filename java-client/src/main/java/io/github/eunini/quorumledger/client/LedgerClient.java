package io.github.eunini.quorumledger.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Blocking client for a quorum-ledger cluster.
 *
 * <p>One instance is one <em>session</em>: a random 128-bit client id and a request counter. At most one
 * request is in flight per session (calls are serialized). When an attempt times out, the connection
 * fails, or the replica answers "not leader", the <em>same</em> request (same client id and request
 * number) is sent to another replica. The cluster records the last request number and reply per client
 * id in its replicated state, so a retry of an already-executed request returns the original reply
 * instead of executing twice. Transfer ids give a second, application-level layer of idempotency.
 *
 * <p>Use a {@link LedgerClientPool} for concurrency.
 */
public final class LedgerClient implements AutoCloseable {
    private static final int MAX_FRAME_BYTES = 64 << 20;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final List<InetSocketAddress> replicas;
    private final BigInteger clientId;
    private final Duration attemptTimeout;
    private final Duration requestTimeout;
    private long requestNumber;
    private int leaderGuess;
    private Socket socket;
    private int socketReplica = -1;
    private DataInputStream in;
    private OutputStream out;

    public LedgerClient(List<InetSocketAddress> replicas) {
        this(replicas, Duration.ofMillis(1000), Duration.ofSeconds(30));
    }

    public LedgerClient(List<InetSocketAddress> replicas, Duration attemptTimeout, Duration requestTimeout) {
        if (replicas.isEmpty()) {
            throw new IllegalArgumentException("at least one replica address required");
        }
        this.replicas = List.copyOf(replicas);
        this.attemptTimeout = Objects.requireNonNull(attemptTimeout);
        this.requestTimeout = Objects.requireNonNull(requestTimeout);
        byte[] id = new byte[16];
        RANDOM.nextBytes(id);
        this.clientId = new BigInteger(1, id);
    }

    public BigInteger clientId() {
        return clientId;
    }

    /** Replica index this session currently believes to be the leader. */
    public synchronized int leaderGuess() {
        return leaderGuess;
    }

    public List<ResultCode> createAccounts(List<NewAccount> accounts) {
        return ((Reply.Results) request(new Operation.CreateAccounts(accounts))).results();
    }

    public List<ResultCode> createTransfers(List<NewTransfer> transfers) {
        return ((Reply.Results) request(new Operation.CreateTransfers(transfers))).results();
    }

    public List<Account> lookupAccounts(List<BigInteger> ids) {
        return ((Reply.Accounts) request(new Operation.LookupAccounts(ids))).accounts();
    }

    public List<Transfer> lookupTransfers(List<BigInteger> ids) {
        return ((Reply.Transfers) request(new Operation.LookupTransfers(ids))).transfers();
    }

    /** Sends one request, retrying across replicas until a reply arrives or the request timeout passes. */
    public synchronized Reply.Body request(Operation op) {
        requestNumber++;
        byte[] frame = Codec.encodeRequest(clientId, requestNumber, op);
        long deadline = System.nanoTime() + requestTimeout.toNanos();
        int notLeaderStreak = 0;
        Exception last = null;
        while (System.nanoTime() < deadline) {
            try {
                Reply.Body body = attempt(frame);
                if (!(body instanceof Reply.NotLeader nl)) {
                    return body;
                }
                redirect(nl.leaderHint());
                notLeaderStreak++;
                if (notLeaderStreak >= replicas.size()) {
                    // Probably mid-election: back off briefly.
                    sleep(50);
                }
            } catch (IOException | ProtocolException e) {
                last = e;
                disconnect();
                leaderGuess = (leaderGuess + 1) % replicas.size();
                sleep(20);
            }
        }
        throw new LedgerUnavailableException(
                "request " + requestNumber + " not answered within " + requestTimeout, last);
    }

    private Reply.Body attempt(byte[] frame) throws IOException {
        connect();
        out.write(frame);
        out.flush();
        while (true) {
            Reply reply;
            try {
                reply = Codec.decodeReply(readFrame());
            } catch (SocketTimeoutException e) {
                throw new IOException("attempt timed out on replica " + socketReplica, e);
            }
            if (!reply.clientId().equals(clientId) || reply.requestNumber() != requestNumber) {
                continue; // late reply to an earlier attempt
            }
            return reply.body();
        }
    }

    private void redirect(int hint) {
        disconnect();
        if (hint >= 0 && hint < replicas.size() && hint != leaderGuess) {
            leaderGuess = hint;
        } else {
            leaderGuess = (leaderGuess + 1) % replicas.size();
        }
    }

    private void connect() throws IOException {
        if (socket != null && socketReplica == leaderGuess) {
            return;
        }
        disconnect();
        Socket s = new Socket();
        try {
            s.setTcpNoDelay(true);
            s.connect(replicas.get(leaderGuess), 500);
            s.setSoTimeout((int) attemptTimeout.toMillis());
            OutputStream o = new BufferedOutputStream(s.getOutputStream(), 1 << 16);
            o.write(Codec.hello());
            o.flush();
            InputStream i = new BufferedInputStream(s.getInputStream(), 1 << 16);
            socket = s;
            socketReplica = leaderGuess;
            in = new DataInputStream(i);
            out = o;
        } catch (IOException e) {
            s.close();
            throw e;
        }
    }

    private byte[] readFrame() throws IOException {
        int len = Integer.reverseBytes(in.readInt());
        if (len <= 0 || len > MAX_FRAME_BYTES) {
            throw new ProtocolException("bad frame length " + len);
        }
        byte[] body = new byte[len];
        in.readFully(body);
        return body;
    }

    private void disconnect() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing a broken socket
            }
        }
        socket = null;
        socketReplica = -1;
        in = null;
        out = null;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LedgerUnavailableException("interrupted", e);
        }
    }

    @Override
    public synchronized void close() {
        disconnect();
    }
}
