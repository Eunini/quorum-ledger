package io.github.eunini.quorumledger.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Exercises retry and redirect logic against scripted fake replicas. */
class LedgerClientTest {

    /** A request as seen by a fake replica. */
    record Seen(int replica, BigInteger clientId, long requestNumber) {}

    /** Fake replica: answers each request with the reply bodies returned by {@code behaviour} (none = silent). */
    static final class FakeReplica implements AutoCloseable {
        final ServerSocket server;
        final Thread thread;

        FakeReplica(int index, List<Seen> seen, BiFunction<Seen, Integer, List<byte[]>> behaviour) throws IOException {
            server = new ServerSocket(0);
            thread = Thread.ofVirtual().start(() -> {
                int n = 0;
                while (!server.isClosed()) {
                    try (Socket s = server.accept()) {
                        DataInputStream in = new DataInputStream(s.getInputStream());
                        OutputStream out = s.getOutputStream();
                        readFrame(in); // hello
                        while (true) {
                            ByteBuffer req = ByteBuffer.wrap(readFrame(in)).order(ByteOrder.LITTLE_ENDIAN);
                            req.get();
                            Seen r = new Seen(index, Codec.getU128(req), req.getLong());
                            seen.add(r);
                            for (byte[] reply : behaviour.apply(r, n++)) {
                                out.write(TestFrames.withLength(reply));
                            }
                            out.flush();
                        }
                    } catch (IOException e) {
                        // connection closed by client or server shut down
                    }
                }
            });
        }

        InetSocketAddress address() {
            return new InetSocketAddress("127.0.0.1", server.getLocalPort());
        }

        private static byte[] readFrame(DataInputStream in) throws IOException {
            int len = Integer.reverseBytes(in.readInt());
            byte[] b = new byte[len];
            in.readFully(b);
            return b;
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }

    private final List<AutoCloseable> resources = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanup() throws Exception {
        for (AutoCloseable c : resources) {
            c.close();
        }
    }

    private FakeReplica replica(int index, List<Seen> seen, BiFunction<Seen, Integer, List<byte[]>> behaviour) throws IOException {
        FakeReplica r = new FakeReplica(index, seen, behaviour);
        resources.add(r);
        return r;
    }

    @Test
    void followsLeaderHintAndRetriesWithTheSameRequestNumber() throws Exception {
        List<Seen> seen = new CopyOnWriteArrayList<>();
        // Replica 0 is a follower pointing at 2. Replica 2 drops the first attempt
        // (simulating a lost reply), then answers. Replica 1 is a follower with no hint.
        FakeReplica r0 = replica(0, seen, (req, n) -> List.of(TestFrames.notLeader(req.clientId(), req.requestNumber(), 2)));
        FakeReplica r1 = replica(1, seen, (req, n) -> List.of(TestFrames.notLeader(req.clientId(), req.requestNumber(), 0xFF)));
        FakeReplica r2 = replica(2, seen, (req, n) ->
                n == 0 ? List.of() : List.of(TestFrames.resultsReply(req.clientId(), req.requestNumber(), List.of(ResultCode.OK))));

        try (LedgerClient client = new LedgerClient(
                List.of(r0.address(), r1.address(), r2.address()), Duration.ofMillis(200), Duration.ofSeconds(10))) {
            List<ResultCode> result = client.createAccounts(
                    List.of(new NewAccount(BigInteger.ONE, 1, (short) 1, (short) 0)));
            assertEquals(List.of(ResultCode.OK), result);
            assertEquals(2, client.leaderGuess());

            // Every attempt carried the same session id and request number 1.
            assertEquals(List.of(1L), seen.stream().map(Seen::requestNumber).distinct().toList());
            assertEquals(List.of(client.clientId()), seen.stream().map(Seen::clientId).distinct().toList());
            // 0 (redirect) -> 2 (silent, timeout) -> 0 (redirect) -> 2 (ok)
            assertEquals(List.of(0, 2, 0, 2), seen.stream().map(Seen::replica).toList());

            // The next call is request number 2 and goes straight to the leader.
            seen.clear();
            client.createAccounts(List.of(new NewAccount(BigInteger.TWO, 1, (short) 1, (short) 0)));
            assertEquals(List.of(new Seen(2, client.clientId(), 2)), seen);
        }
    }

    @Test
    void ignoresStaleRepliesForEarlierRequests() throws Exception {
        List<Seen> seen = new CopyOnWriteArrayList<>();
        // Before the real reply, the replica emits a late duplicate reply to the previous request number
        // and a reply addressed to a different session; the client must skip both.
        FakeReplica r0 = replica(0, seen, (req, n) -> List.of(
                TestFrames.resultsReply(req.clientId(), req.requestNumber() - 1, List.of(ResultCode.EXISTS)),
                TestFrames.resultsReply(req.clientId().add(BigInteger.ONE), req.requestNumber(), List.of(ResultCode.OVERFLOW)),
                TestFrames.resultsReply(req.clientId(), req.requestNumber(), List.of(ResultCode.OK))));
        try (LedgerClient client = new LedgerClient(List.of(r0.address()), Duration.ofMillis(500), Duration.ofSeconds(5))) {
            assertEquals(List.of(ResultCode.OK), client.createTransfers(List.of(NewTransfer.single(
                    BigInteger.ONE, BigInteger.ONE, BigInteger.TWO, BigInteger.TEN, 1, (short) 1))));
        }
    }

    @Test
    void givesUpAfterRequestTimeout() throws Exception {
        List<Seen> seen = new CopyOnWriteArrayList<>();
        FakeReplica silent = replica(0, seen, (req, n) -> List.of());
        try (LedgerClient client = new LedgerClient(List.of(silent.address()), Duration.ofMillis(100), Duration.ofMillis(600))) {
            assertThrows(LedgerUnavailableException.class, () -> client.lookupAccounts(List.of(BigInteger.ONE)));
        }
        // Retries happened, all with request number 1.
        assertEquals(List.of(1L), seen.stream().map(Seen::requestNumber).distinct().toList());
    }
}
