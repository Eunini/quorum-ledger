package io.github.eunini.quorumledger.payments;

import io.github.eunini.quorumledger.client.Account;
import io.github.eunini.quorumledger.client.LedgerClientPool;
import io.github.eunini.quorumledger.client.NewAccount;
import io.github.eunini.quorumledger.client.NewTransfer;
import io.github.eunini.quorumledger.client.ResultCode;
import io.github.eunini.quorumledger.client.Transfer;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

/** Gateway backed by a pool of client sessions to the replicated cluster. */
public class ClusterLedgerGateway implements LedgerGateway, AutoCloseable {
    private final LedgerClientPool pool;

    public ClusterLedgerGateway(LedgerClientPool pool) {
        this.pool = pool;
    }

    @Override
    public ResultCode createAccount(NewAccount account) {
        return pool.with(c -> c.createAccounts(List.of(account)).getFirst());
    }

    @Override
    public ResultCode createTransfer(NewTransfer transfer) {
        return pool.with(c -> c.createTransfers(List.of(transfer)).getFirst());
    }

    @Override
    public Optional<Account> lookupAccount(BigInteger id) {
        return pool.with(c -> c.lookupAccounts(List.of(id)).stream().findFirst());
    }

    @Override
    public Optional<Transfer> lookupTransfer(BigInteger id) {
        return pool.with(c -> c.lookupTransfers(List.of(id)).stream().findFirst());
    }

    @Override
    public void close() {
        pool.close();
    }
}
