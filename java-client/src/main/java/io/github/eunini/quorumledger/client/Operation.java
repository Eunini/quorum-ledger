package io.github.eunini.quorumledger.client;

import java.math.BigInteger;
import java.util.List;

/** One client request's operation. Each carries a batch of events. */
public sealed interface Operation {
    int MAX_EVENTS = 8_190;

    byte code();

    int eventCount();

    record CreateAccounts(List<NewAccount> accounts) implements Operation {
        public byte code() {
            return 1;
        }

        public int eventCount() {
            return accounts.size();
        }
    }

    record CreateTransfers(List<NewTransfer> transfers) implements Operation {
        public byte code() {
            return 2;
        }

        public int eventCount() {
            return transfers.size();
        }
    }

    record LookupAccounts(List<BigInteger> ids) implements Operation {
        public byte code() {
            return 3;
        }

        public int eventCount() {
            return ids.size();
        }
    }

    record LookupTransfers(List<BigInteger> ids) implements Operation {
        public byte code() {
            return 4;
        }

        public int eventCount() {
            return ids.size();
        }
    }
}
