package io.github.eunini.quorumledger.payments;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Runs the payments API against a real 3-replica Rust cluster (separate OS processes talking TCP, each with
 * its own WAL). Skipped if the server binary has not been built: run {@code cargo build --release -p server}
 * first (CI does).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PaymentsApiClusterIT {
    static final Path BINARY = Path.of(System.getProperty(
            "quorumLedger.serverBinary", "../target/release/quorum-ledger-server"));
    static final List<Integer> PORTS = List.of(freePort(), freePort(), freePort());
    static final List<Process> PROCESSES = new ArrayList<>();
    static Path dataDir;

    @Autowired
    TestRestTemplate http;

    static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void cluster(DynamicPropertyRegistry registry) {
        registry.add("quorum-ledger.cluster", () -> String.join(",", PORTS.stream().map(p -> "127.0.0.1:" + p).toList()));
        registry.add("quorum-ledger.attempt-timeout", () -> "500ms");
    }

    @BeforeAll
    static void startCluster() throws IOException {
        assumeTrue(Files.isExecutable(BINARY), "server binary not built: " + BINARY.toAbsolutePath());
        dataDir = Files.createTempDirectory("ql-payments-it");
        for (int i = 0; i < 3; i++) {
            PROCESSES.add(start(i));
        }
    }

    static Process start(int id) throws IOException {
        String cluster = String.join(",", PORTS.stream().map(p -> "127.0.0.1:" + p).toList());
        return new ProcessBuilder(BINARY.toString(), "--id", Integer.toString(id), "--cluster", cluster,
                        "--data", dataDir.toString())
                .redirectErrorStream(true)
                .redirectOutput(dataDir.resolve("replica-" + id + ".log").toFile())
                .start();
    }

    @AfterAll
    static void stopCluster() throws IOException {
        PROCESSES.forEach(Process::destroyForcibly);
        if (dataDir != null) {
            try (Stream<Path> files = Files.walk(dataDir)) {
                for (Path p : files.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(p);
                }
            }
        }
    }

    ResponseEntity<JsonNode> post(String path, String key, Object body) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        if (key != null) {
            h.set("Idempotency-Key", key);
        }
        return http.exchange(path, HttpMethod.POST, new HttpEntity<>(body, h), JsonNode.class);
    }

    JsonNode account(String id) {
        ResponseEntity<JsonNode> r = http.getForEntity("/accounts/" + id, JsonNode.class);
        assertEquals(HttpStatus.OK, r.getStatusCode());
        return r.getBody();
    }

    String createAccount(String key, boolean preventOverdraft) {
        ResponseEntity<JsonNode> r = post("/accounts", key, Map.of("ledger", 840, "code", 1, "preventOverdraft", preventOverdraft));
        assertEquals(HttpStatus.CREATED, r.getStatusCode());
        return r.getBody().get("id").asText();
    }

    Map<String, Object> transfer(String from, String to, long amount) {
        return Map.of("debitAccountId", from, "creditAccountId", to, "amount", amount, "ledger", 840, "code", 1);
    }

    @Test
    void paymentsFlowAgainstRealCluster() throws Exception {
        String bank = createAccount("acct-bank", false);
        String customer = createAccount("acct-customer", true);
        String merchant = createAccount("acct-merchant", false);
        // Account creation is idempotent per key.
        ResponseEntity<JsonNode> again = post("/accounts", "acct-customer", Map.of("ledger", 840, "code", 1, "preventOverdraft", true));
        assertEquals(HttpStatus.OK, again.getStatusCode());
        assertEquals(customer, again.getBody().get("id").asText());

        // Fund the customer; replay and conflicting reuse of the key.
        ResponseEntity<JsonNode> fund = post("/transfers", "fund-1", transfer(bank, customer, 10_000));
        assertEquals(HttpStatus.CREATED, fund.getStatusCode());
        ResponseEntity<JsonNode> replay = post("/transfers", "fund-1", transfer(bank, customer, 10_000));
        assertEquals(HttpStatus.OK, replay.getStatusCode());
        assertTrue(replay.getBody().get("replayed").asBoolean());
        assertEquals(fund.getBody().get("id"), replay.getBody().get("id"));
        assertEquals(HttpStatus.CONFLICT, post("/transfers", "fund-1", transfer(bank, customer, 9_999)).getStatusCode());
        assertEquals(10_000, account(customer).get("available").asLong());

        // Overdraft protection.
        ResponseEntity<JsonNode> overdraft = post("/transfers", "pay-too-much", transfer(customer, merchant, 10_001));
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, overdraft.getStatusCode());
        assertEquals("EXCEEDS_CREDITS", overdraft.getBody().get("code").asText());

        // Authorize 3,000 then capture 2,000: the remaining 1,000 is released.
        Map<String, Object> holdBody = new java.util.HashMap<>(transfer(customer, merchant, 3_000));
        holdBody.put("timeoutSeconds", 3600);
        ResponseEntity<JsonNode> hold = post("/holds", "auth-1", holdBody);
        assertEquals(HttpStatus.CREATED, hold.getStatusCode());
        String holdId = hold.getBody().get("id").asText();
        assertEquals(3_000, account(customer).get("debitsPending").asLong());
        assertEquals(7_000, account(customer).get("available").asLong());
        ResponseEntity<JsonNode> capture = post("/holds/" + holdId + "/capture", "cap-1", Map.of("amount", 2_000));
        assertEquals(HttpStatus.CREATED, capture.getStatusCode());
        assertEquals("capture", capture.getBody().get("type").asText());
        assertEquals(HttpStatus.OK, post("/holds/" + holdId + "/capture", "cap-1", Map.of("amount", 2_000)).getStatusCode());
        JsonNode c = account(customer);
        assertEquals(0, c.get("debitsPending").asLong());
        assertEquals(2_000, c.get("debitsPosted").asLong());
        assertEquals(8_000, c.get("available").asLong());

        // Authorize then void; capturing a voided hold conflicts.
        String hold2 = post("/holds", "auth-2", holdBody).getBody().get("id").asText();
        assertEquals(HttpStatus.CREATED, post("/holds/" + hold2 + "/void", "void-2", null).getStatusCode());
        assertEquals(HttpStatus.CONFLICT, post("/holds/" + hold2 + "/capture", "cap-2", null).getStatusCode());
        assertEquals(8_000, account(customer).get("available").asLong());

        // Kill one replica (possibly the leader) and keep going. Then hammer with
        // concurrent requests where every key is submitted twice: each must apply once.
        PROCESSES.get(0).destroyForcibly().waitFor();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<HttpStatus>> results = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String key = "bulk-" + i;
                for (int copy = 0; copy < 2; copy++) {
                    results.add(pool.submit(() -> (HttpStatus) post("/transfers", key, transfer(bank, merchant, 1)).getStatusCode()));
                }
            }
            int created = 0;
            int replayed = 0;
            for (Future<HttpStatus> f : results) {
                HttpStatus s = f.get();
                if (s == HttpStatus.CREATED) {
                    created++;
                } else if (s == HttpStatus.OK) {
                    replayed++;
                }
            }
            assertEquals(100, created, "each key creates exactly one transfer");
            assertEquals(100, replayed, "the duplicate of each key is a replay");
        } finally {
            pool.shutdownNow();
        }

        JsonNode m = account(merchant);
        assertEquals(BigInteger.valueOf(2_000 + 100), new BigInteger(m.get("creditsPosted").asText()));
        JsonNode b = account(bank);
        BigInteger debits = new BigInteger(b.get("debitsPosted").asText());
        BigInteger credits = new BigInteger(account(customer).get("creditsPosted").asText()).add(new BigInteger(m.get("creditsPosted").asText()));
        // bank -> customer 10,000 and bank -> merchant 100; customer -> merchant 2,000 is internal.
        assertEquals(BigInteger.valueOf(10_100), debits);
        assertEquals(BigInteger.valueOf(12_100), credits);
    }
}
