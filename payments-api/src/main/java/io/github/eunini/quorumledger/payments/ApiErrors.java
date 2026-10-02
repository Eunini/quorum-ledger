package io.github.eunini.quorumledger.payments;

import io.github.eunini.quorumledger.client.LedgerUnavailableException;
import io.github.eunini.quorumledger.client.ResultCode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Maps ledger outcomes to RFC 9457 problem responses. */
@RestControllerAdvice
public class ApiErrors {

    @ExceptionHandler(LedgerRejectedException.class)
    ProblemDetail rejected(LedgerRejectedException e) {
        HttpStatus status = statusFor(e.code());
        ProblemDetail p = ProblemDetail.forStatusAndDetail(status, describe(e.code()));
        p.setTitle(status.getReasonPhrase());
        p.setProperty("code", e.code().name());
        return p;
    }

    static HttpStatus statusFor(ResultCode code) {
        return switch (code) {
            case EXISTS_WITH_DIFFERENT_FIELDS,
                    PENDING_TRANSFER_ALREADY_POSTED,
                    PENDING_TRANSFER_ALREADY_VOIDED,
                    PENDING_TRANSFER_EXPIRED -> HttpStatus.CONFLICT;
            case DEBIT_ACCOUNT_NOT_FOUND, CREDIT_ACCOUNT_NOT_FOUND, PENDING_TRANSFER_NOT_FOUND -> HttpStatus.NOT_FOUND;
            default -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
    }

    static String describe(ResultCode code) {
        return switch (code) {
            case EXISTS_WITH_DIFFERENT_FIELDS -> "Idempotency-Key was already used with a different request body";
            case EXCEEDS_CREDITS -> "insufficient funds";
            case EXCEEDS_DEBITS -> "credit limit exceeded";
            case PENDING_TRANSFER_ALREADY_POSTED -> "hold was already captured";
            case PENDING_TRANSFER_ALREADY_VOIDED -> "hold was already voided";
            case PENDING_TRANSFER_EXPIRED -> "hold expired";
            case POST_AMOUNT_EXCEEDS_PENDING_AMOUNT -> "capture amount exceeds the held amount";
            default -> code.name().toLowerCase().replace('_', ' ');
        };
    }

    @ExceptionHandler(NotFoundException.class)
    ProblemDetail notFound(NotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(IdempotencyKeys.InvalidIdempotencyKeyException.class)
    ProblemDetail badKey(IdempotencyKeys.InvalidIdempotencyKeyException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    ProblemDetail missingHeader(MissingRequestHeaderException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "missing header " + e.getHeaderName());
    }

    @ExceptionHandler(LedgerUnavailableException.class)
    ProblemDetail unavailable(LedgerUnavailableException e) {
        // Safe to retry with the same Idempotency-Key: the operation ran at most once.
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "ledger cluster unavailable; retry with the same Idempotency-Key");
    }
}
