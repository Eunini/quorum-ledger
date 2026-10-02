package io.github.eunini.quorumledger.payments;

import io.github.eunini.quorumledger.payments.Dtos.AccountResponse;
import io.github.eunini.quorumledger.payments.Dtos.CaptureRequest;
import io.github.eunini.quorumledger.payments.Dtos.CreateAccountRequest;
import io.github.eunini.quorumledger.payments.Dtos.HoldRequest;
import io.github.eunini.quorumledger.payments.Dtos.TransferRequest;
import io.github.eunini.quorumledger.payments.Dtos.TransferResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST surface. Creates return 201 for a new object and 200 with {@code replayed: true} when the same
 * Idempotency-Key and body were already processed.
 */
@RestController
public class PaymentsController {
    static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    private final PaymentsService service;

    public PaymentsController(PaymentsService service) {
        this.service = service;
    }

    @PostMapping("/accounts")
    public ResponseEntity<AccountResponse> createAccount(
            @RequestHeader(value = IDEMPOTENCY_KEY, required = false) String key,
            @Valid @RequestBody CreateAccountRequest body) {
        return respond(service.createAccount(body, key));
    }

    @GetMapping("/accounts/{id}")
    public AccountResponse getAccount(@PathVariable("id") String id) {
        return service.getAccount(id);
    }

    @PostMapping("/transfers")
    public ResponseEntity<TransferResponse> transfer(
            @RequestHeader(IDEMPOTENCY_KEY) String key, @Valid @RequestBody TransferRequest body) {
        return respond(service.transfer(body, key));
    }

    @GetMapping("/transfers/{id}")
    public TransferResponse getTransfer(@PathVariable("id") String id) {
        return service.getTransfer(id);
    }

    @PostMapping("/holds")
    public ResponseEntity<TransferResponse> hold(
            @RequestHeader(IDEMPOTENCY_KEY) String key, @Valid @RequestBody HoldRequest body) {
        return respond(service.hold(body, key));
    }

    @PostMapping("/holds/{holdId}/capture")
    public ResponseEntity<TransferResponse> capture(
            @PathVariable("holdId") String holdId,
            @RequestHeader(IDEMPOTENCY_KEY) String key,
            @Valid @RequestBody(required = false) CaptureRequest body) {
        return respond(service.capture(holdId, body, key));
    }

    @PostMapping("/holds/{holdId}/void")
    public ResponseEntity<TransferResponse> voidHold(
            @PathVariable("holdId") String holdId, @RequestHeader(IDEMPOTENCY_KEY) String key) {
        return respond(service.voidHold(holdId, key));
    }

    private static <T> ResponseEntity<T> respond(PaymentsService.Created<T> created) {
        return ResponseEntity.status(created.replayed() ? HttpStatus.OK : HttpStatus.CREATED)
                .body(created.body());
    }
}
