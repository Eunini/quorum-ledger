package io.github.eunini.quorumledger.payments;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.eunini.quorumledger.client.Account;
import io.github.eunini.quorumledger.client.AccountFlags;
import io.github.eunini.quorumledger.client.LedgerUnavailableException;
import io.github.eunini.quorumledger.client.NewTransfer;
import io.github.eunini.quorumledger.client.ResultCode;
import io.github.eunini.quorumledger.client.Transfer;
import io.github.eunini.quorumledger.client.TransferFlags;
import java.math.BigInteger;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(PaymentsController.class)
@Import({PaymentsService.class, ApiErrors.class})
class PaymentsControllerTest {
    @Autowired
    MockMvc mvc;

    @MockitoBean
    LedgerGateway ledger;

    static final String TRANSFER = """
            {"debitAccountId":"1","creditAccountId":"2","amount":500,"ledger":840,"code":1}
            """;

    private static Transfer stored(NewTransfer t) {
        return new Transfer(t.id(), t.debitAccountId(), t.creditAccountId(), t.amount(), t.pendingId(),
                t.ledger(), t.code(), t.flags(), t.timeoutSeconds(), 42L);
    }

    @Test
    void transferRequiresIdempotencyKey() throws Exception {
        mvc.perform(post("/transfers").contentType(MediaType.APPLICATION_JSON).content(TRANSFER))
                .andExpect(status().isBadRequest());
    }

    @Test
    void newTransferIsCreatedAndReplayReturns200WithSameId() throws Exception {
        ArgumentCaptor<NewTransfer> sent = ArgumentCaptor.forClass(NewTransfer.class);
        when(ledger.createTransfer(sent.capture())).thenReturn(ResultCode.OK, ResultCode.EXISTS);
        when(ledger.lookupTransfer(any())).thenAnswer(inv -> Optional.of(stored(sent.getValue())));
        BigInteger expectedId = IdempotencyKeys.idFor("transfer", "pay-1");

        mvc.perform(post("/transfers").header("Idempotency-Key", "pay-1").contentType(MediaType.APPLICATION_JSON).content(TRANSFER))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(expectedId.toString()))
                .andExpect(jsonPath("$.type").value("transfer"))
                .andExpect(jsonPath("$.replayed").value(false));
        mvc.perform(post("/transfers").header("Idempotency-Key", "pay-1").contentType(MediaType.APPLICATION_JSON).content(TRANSFER))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(expectedId.toString()))
                .andExpect(jsonPath("$.replayed").value(true));
        // Both attempts carried the same ledger transfer id.
        assertEquals(sent.getAllValues().get(0).id(), sent.getAllValues().get(1).id());
    }

    @Test
    void keyReuseWithDifferentBodyIsConflict() throws Exception {
        when(ledger.createTransfer(any())).thenReturn(ResultCode.EXISTS_WITH_DIFFERENT_FIELDS);
        mvc.perform(post("/transfers").header("Idempotency-Key", "pay-1").contentType(MediaType.APPLICATION_JSON).content(TRANSFER))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("EXISTS_WITH_DIFFERENT_FIELDS"));
    }

    @Test
    void insufficientFundsIs422() throws Exception {
        when(ledger.createTransfer(any())).thenReturn(ResultCode.EXCEEDS_CREDITS);
        mvc.perform(post("/transfers").header("Idempotency-Key", "pay-2").contentType(MediaType.APPLICATION_JSON).content(TRANSFER))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.detail").value("insufficient funds"));
    }

    @Test
    void unknownAccountIs404() throws Exception {
        when(ledger.createTransfer(any())).thenReturn(ResultCode.DEBIT_ACCOUNT_NOT_FOUND);
        mvc.perform(post("/transfers").header("Idempotency-Key", "pay-3").contentType(MediaType.APPLICATION_JSON).content(TRANSFER))
                .andExpect(status().isNotFound());
    }

    @Test
    void clusterUnavailableIs503() throws Exception {
        when(ledger.createTransfer(any())).thenThrow(new LedgerUnavailableException("down", null));
        mvc.perform(post("/transfers").header("Idempotency-Key", "pay-4").contentType(MediaType.APPLICATION_JSON).content(TRANSFER))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void invalidBodyIs400() throws Exception {
        mvc.perform(post("/transfers").header("Idempotency-Key", "pay-5").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"debitAccountId\":\"x\",\"creditAccountId\":\"2\",\"amount\":-1,\"ledger\":840,\"code\":1}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void captureUsesHoldIdAndScopedKey() throws Exception {
        ArgumentCaptor<NewTransfer> sent = ArgumentCaptor.forClass(NewTransfer.class);
        when(ledger.createTransfer(sent.capture())).thenReturn(ResultCode.OK);
        when(ledger.lookupTransfer(any())).thenAnswer(inv -> Optional.of(stored(sent.getValue())));
        mvc.perform(post("/holds/77/capture").header("Idempotency-Key", "cap-1").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":200}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.type").value("capture"))
                .andExpect(jsonPath("$.holdId").value("77"));
        NewTransfer t = sent.getValue();
        assertEquals(TransferFlags.POST_PENDING, t.flags());
        assertEquals(BigInteger.valueOf(77), t.pendingId());
        assertEquals(IdempotencyKeys.idFor("capture", "cap-1"), t.id());
    }

    @Test
    void captureOfVoidedHoldIsConflict() throws Exception {
        when(ledger.createTransfer(any())).thenReturn(ResultCode.PENDING_TRANSFER_ALREADY_VOIDED);
        mvc.perform(post("/holds/77/capture").header("Idempotency-Key", "cap-2"))
                .andExpect(status().isConflict());
    }

    @Test
    void accountBalanceView() throws Exception {
        Account a = new Account(BigInteger.valueOf(9), BigInteger.valueOf(300), BigInteger.valueOf(200),
                BigInteger.ZERO, BigInteger.valueOf(10_000), 840, (short) 1, AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS, 5L);
        when(ledger.lookupAccount(BigInteger.valueOf(9))).thenReturn(Optional.of(a));
        mvc.perform(get("/accounts/9"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available").value(9_500))
                .andExpect(jsonPath("$.preventOverdraft").value(true));
        mvc.perform(get("/accounts/10")).andExpect(status().isNotFound());
        verify(ledger).lookupAccount(BigInteger.valueOf(10));
    }
}
