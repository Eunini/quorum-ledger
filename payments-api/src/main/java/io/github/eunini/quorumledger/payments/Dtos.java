package io.github.eunini.quorumledger.payments;

import io.github.eunini.quorumledger.client.Account;
import io.github.eunini.quorumledger.client.AccountFlags;
import io.github.eunini.quorumledger.client.Transfer;
import io.github.eunini.quorumledger.client.TransferFlags;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.math.BigInteger;

/** Request/response bodies. Ids are decimal strings (128-bit); amounts are integers in minor units. */
public final class Dtos {
    private Dtos() {}

    static final String ID = "^[0-9]{1,39}$";

    public record CreateAccountRequest(
            @Min(1) long ledger, @Min(0) @Max(65535) int code, boolean preventOverdraft) {}

    public record AccountResponse(
            String id,
            long ledger,
            int code,
            boolean preventOverdraft,
            BigInteger debitsPending,
            BigInteger debitsPosted,
            BigInteger creditsPending,
            BigInteger creditsPosted,
            // credits_posted - debits_posted - debits_pending: spendable balance of a customer account.
            BigInteger available) {

        static AccountResponse from(Account a) {
            return new AccountResponse(
                    a.id().toString(),
                    Integer.toUnsignedLong(a.ledger()),
                    Short.toUnsignedInt(a.code()),
                    (a.flags() & AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS) != 0,
                    a.debitsPending(),
                    a.debitsPosted(),
                    a.creditsPending(),
                    a.creditsPosted(),
                    a.creditsPosted().subtract(a.debitsPosted()).subtract(a.debitsPending()));
        }
    }

    public record TransferRequest(
            @NotNull @Pattern(regexp = ID) String debitAccountId,
            @NotNull @Pattern(regexp = ID) String creditAccountId,
            @NotNull @Positive BigInteger amount,
            @Min(1) long ledger,
            @Min(0) @Max(65535) int code) {}

    public record HoldRequest(
            @NotNull @Pattern(regexp = ID) String debitAccountId,
            @NotNull @Pattern(regexp = ID) String creditAccountId,
            @NotNull @Positive BigInteger amount,
            @Min(1) long ledger,
            @Min(0) @Max(65535) int code,
            // Seconds until the hold is released automatically; 0 = never.
            @Min(0) @Max(31_536_000) int timeoutSeconds) {}

    /** {@code amount} null or 0 captures the full held amount. */
    public record CaptureRequest(@Min(0) BigInteger amount) {}

    public record TransferResponse(
            String id,
            String type,
            String debitAccountId,
            String creditAccountId,
            BigInteger amount,
            String holdId,
            long ledger,
            int code,
            long timestamp,
            // True when this response replays an earlier identical request (same Idempotency-Key).
            boolean replayed) {

        static TransferResponse from(Transfer t, boolean replayed) {
            String type;
            if ((t.flags() & TransferFlags.PENDING) != 0) {
                type = "hold";
            } else if ((t.flags() & TransferFlags.POST_PENDING) != 0) {
                type = "capture";
            } else if ((t.flags() & TransferFlags.VOID_PENDING) != 0) {
                type = "void";
            } else {
                type = "transfer";
            }
            return new TransferResponse(
                    t.id().toString(),
                    type,
                    t.debitAccountId().toString(),
                    t.creditAccountId().toString(),
                    t.amount(),
                    t.pendingId().signum() == 0 ? null : t.pendingId().toString(),
                    Integer.toUnsignedLong(t.ledger()),
                    Short.toUnsignedInt(t.code()),
                    t.timestamp(),
                    replayed);
        }
    }
}
