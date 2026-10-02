package io.github.eunini.quorumledger.client;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

/** Fixed pool of client sessions; each borrowed session runs one request at a time. */
public final class LedgerClientPool implements AutoCloseable {
    private final BlockingQueue<LedgerClient> idle;
    private final List<LedgerClient> all;

    public LedgerClientPool(List<InetSocketAddress> replicas, int size, Duration attemptTimeout, Duration requestTimeout) {
        if (size < 1) {
            throw new IllegalArgumentException("pool size must be positive");
        }
        idle = new ArrayBlockingQueue<>(size);
        all = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            LedgerClient c = new LedgerClient(replicas, attemptTimeout, requestTimeout);
            all.add(c);
            idle.add(c);
        }
    }

    public <T> T with(Function<LedgerClient, T> work) {
        LedgerClient c;
        try {
            c = idle.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LedgerUnavailableException("interrupted while waiting for a session", e);
        }
        try {
            return work.apply(c);
        } finally {
            idle.add(c);
        }
    }

    @Override
    public void close() {
        all.forEach(LedgerClient::close);
    }
}
