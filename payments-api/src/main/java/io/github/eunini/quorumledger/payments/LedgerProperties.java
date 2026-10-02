package io.github.eunini.quorumledger.payments;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param cluster replica addresses as host:port, ordered by replica id
 * @param poolSize number of client sessions (concurrent in-flight requests)
 */
@ConfigurationProperties("quorum-ledger")
public record LedgerProperties(List<String> cluster, int poolSize, Duration attemptTimeout, Duration requestTimeout) {
    public LedgerProperties {
        if (cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("quorum-ledger.cluster must list the replica addresses");
        }
        if (poolSize <= 0) {
            poolSize = 8;
        }
        if (attemptTimeout == null) {
            attemptTimeout = Duration.ofMillis(1000);
        }
        if (requestTimeout == null) {
            requestTimeout = Duration.ofSeconds(10);
        }
    }
}
