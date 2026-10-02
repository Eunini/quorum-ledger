package io.github.eunini.quorumledger.payments;

import io.github.eunini.quorumledger.client.Account;
import io.github.eunini.quorumledger.client.NewAccount;
import io.github.eunini.quorumledger.client.NewTransfer;
import io.github.eunini.quorumledger.client.ResultCode;
import io.github.eunini.quorumledger.client.Transfer;
import java.math.BigInteger;
import java.util.Optional;

/** The slice of the ledger the API needs; mocked in web-layer tests. */
public interface LedgerGateway {
    ResultCode createAccount(NewAccount account);

    ResultCode createTransfer(NewTransfer transfer);

    Optional<Account> lookupAccount(BigInteger id);

    Optional<Transfer> lookupTransfer(BigInteger id);
}
