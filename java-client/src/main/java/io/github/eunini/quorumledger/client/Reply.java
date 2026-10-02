package io.github.eunini.quorumledger.client;

import java.math.BigInteger;
import java.util.List;

/** Decoded ClientReply frame. */
public record Reply(BigInteger clientId, long requestNumber, Body body) {

    public sealed interface Body {}

    public record Results(List<ResultCode> results) implements Body {}

    public record Accounts(List<Account> accounts) implements Body {}

    public record Transfers(List<Transfer> transfers) implements Body {}

    /** The replica is not the leader; {@code leaderHint} is -1 when unknown. */
    public record NotLeader(int leaderHint) implements Body {}
}
