package io.github.eunini.quorumledger.payments;

import io.github.eunini.quorumledger.client.AccountFlags;
import io.github.eunini.quorumledger.client.NewAccount;
import io.github.eunini.quorumledger.client.NewTransfer;
import io.github.eunini.quorumledger.client.ResultCode;
import io.github.eunini.quorumledger.payments.Dtos.AccountResponse;
import io.github.eunini.quorumledger.payments.Dtos.CaptureRequest;
import io.github.eunini.quorumledger.payments.Dtos.CreateAccountRequest;
import io.github.eunini.quorumledger.payments.Dtos.HoldRequest;
import io.github.eunini.quorumledger.payments.Dtos.TransferRequest;
import io.github.eunini.quorumledger.payments.Dtos.TransferResponse;
import java.math.BigInteger;
import java.security.SecureRandom;
import org.springframework.stereotype.Service;

/**
 * Translates API calls into ledger operations. All idempotency comes from deterministic ids derived from
 * the Idempotency-Key; the ledger itself decides "new", "replay" or "conflict".
 */
@Service
public class PaymentsService {
    private static final SecureRandom RANDOM = new SecureRandom();

    private final LedgerGateway ledger;

    public PaymentsService(LedgerGateway ledger) {
        this.ledger = ledger;
    }

    /** Result of a create call: the stored object plus whether it already existed. */
    public record Created<T>(T body, boolean replayed) {}

    public Created<AccountResponse> createAccount(CreateAccountRequest req, String idempotencyKey) {
        BigInteger id = idempotencyKey == null ? randomId() : IdempotencyKeys.idFor("account", idempotencyKey);
        short flags = req.preventOverdraft() ? AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS : 0;
        ResultCode r = ledger.createAccount(new NewAccount(id, (int) req.ledger(), (short) req.code(), flags));
        return new Created<>(getAccount(id.toString()), check(r));
    }

    public AccountResponse getAccount(String id) {
        return ledger.lookupAccount(parseId(id))
                .map(AccountResponse::from)
                .orElseThrow(() -> new NotFoundException("account " + id + " not found"));
    }

    public TransferResponse getTransfer(String id) {
        return ledger.lookupTransfer(parseId(id))
                .map(t -> TransferResponse.from(t, false))
                .orElseThrow(() -> new NotFoundException("transfer " + id + " not found"));
    }

    public Created<TransferResponse> transfer(TransferRequest req, String idempotencyKey) {
        BigInteger id = IdempotencyKeys.idFor("transfer", idempotencyKey);
        NewTransfer t = NewTransfer.single(
                id, parseId(req.debitAccountId()), parseId(req.creditAccountId()), req.amount(),
                (int) req.ledger(), (short) req.code());
        return submit(t);
    }

    public Created<TransferResponse> hold(HoldRequest req, String idempotencyKey) {
        BigInteger id = IdempotencyKeys.idFor("hold", idempotencyKey);
        NewTransfer t = NewTransfer.hold(
                id, parseId(req.debitAccountId()), parseId(req.creditAccountId()), req.amount(),
                (int) req.ledger(), (short) req.code(), req.timeoutSeconds());
        return submit(t);
    }

    public Created<TransferResponse> capture(String holdId, CaptureRequest req, String idempotencyKey) {
        BigInteger id = IdempotencyKeys.idFor("capture", idempotencyKey);
        BigInteger amount = req == null || req.amount() == null ? BigInteger.ZERO : req.amount();
        return submit(NewTransfer.capture(id, parseId(holdId), amount));
    }

    public Created<TransferResponse> voidHold(String holdId, String idempotencyKey) {
        BigInteger id = IdempotencyKeys.idFor("void", idempotencyKey);
        return submit(NewTransfer.voidHold(id, parseId(holdId)));
    }

    private Created<TransferResponse> submit(NewTransfer t) {
        boolean replayed = check(ledger.createTransfer(t));
        TransferResponse body = ledger.lookupTransfer(t.id())
                .map(stored -> TransferResponse.from(stored, replayed))
                .orElseThrow(() -> new IllegalStateException("transfer " + t.id() + " missing after create"));
        return new Created<>(body, replayed);
    }

    /** Returns true for an idempotent replay, throws for any rejection. */
    private static boolean check(ResultCode r) {
        return switch (r) {
            case OK -> false;
            case EXISTS -> true;
            default -> throw new LedgerRejectedException(r);
        };
    }

    static BigInteger parseId(String id) {
        if (id == null || !id.matches(Dtos.ID)) {
            throw new NotFoundException("malformed id " + id);
        }
        BigInteger v = new BigInteger(id);
        if (v.bitLength() > 128) {
            throw new NotFoundException("id out of range " + id);
        }
        return v;
    }

    private static BigInteger randomId() {
        byte[] b = new byte[16];
        RANDOM.nextBytes(b);
        return new BigInteger(1, b);
    }
}
