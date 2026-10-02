package io.github.eunini.quorumledger.payments;

import io.github.eunini.quorumledger.client.LedgerClientPool;
import java.net.InetSocketAddress;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LedgerConfig {
    @Bean(destroyMethod = "close")
    public ClusterLedgerGateway ledgerGateway(LedgerProperties props) {
        List<InetSocketAddress> addrs = props.cluster().stream().map(LedgerConfig::parse).toList();
        return new ClusterLedgerGateway(
                new LedgerClientPool(addrs, props.poolSize(), props.attemptTimeout(), props.requestTimeout()));
    }

    static InetSocketAddress parse(String hostPort) {
        int i = hostPort.lastIndexOf(':');
        if (i <= 0) {
            throw new IllegalArgumentException("expected host:port, got " + hostPort);
        }
        return new InetSocketAddress(hostPort.substring(0, i), Integer.parseInt(hostPort.substring(i + 1)));
    }
}
