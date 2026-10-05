# Wire protocol

All integers are little-endian. `u128` values are 16 bytes, least significant
byte first. Every message on a TCP connection is a frame:

```
u32 body_length | body
body = u8 tag | fields...
```

Frames larger than 64 MiB, unknown tags, truncated bodies and trailing bytes
are protocol errors and close the connection. The Rust encoder
(`crates/consensus/src/message.rs`) and the Java encoder
(`java-client/.../Codec.java`) are checked against the same golden bytes in
both test suites.

## Handshake

| tag  | name  | fields                                                 |
|------|-------|--------------------------------------------------------|
| 0x01 | Hello | `u8 kind` (0 = replica, 1 = client), `u8 replica_id`   |

The first frame on every connection. Replicas open one outbound connection to
each peer and only write on it; clients read and write on theirs.

## Client frames

| tag  | name          | fields                                                                 |
|------|---------------|------------------------------------------------------------------------|
| 0x20 | ClientRequest | `u128 client_id`, `u64 request_number`, `u8 op`, `u32 count`, `count` events |
| 0x21 | ClientReply   | `u128 client_id`, `u64 request_number`, `u8 status`, status body       |

Operations (`op`) and their event encodings:

| op | operation        | event                                                                                  | bytes |
|----|------------------|----------------------------------------------------------------------------------------|-------|
| 1  | create_accounts  | `u128 id, u32 ledger, u16 code, u16 flags`                                             | 24    |
| 2  | create_transfers | `u128 id, u128 debit_account_id, u128 credit_account_id, u128 amount, u128 pending_id, u32 ledger, u16 code, u16 flags, u32 timeout` | 92 |
| 3  | lookup_accounts  | `u128 id`                                                                              | 16    |
| 4  | lookup_transfers | `u128 id`                                                                              | 16    |

At most 8,190 events per request.

Reply status:

* `0` OK, followed by `u8 kind`, `u32 count` and the items:
  * kind 1 – results: `u32 result_code` per event (same order as the request)
  * kind 2 – accounts: `u128 id, u128 debits_pending, u128 debits_posted, u128 credits_pending, u128 credits_posted, u32 ledger, u16 code, u16 flags, u64 timestamp` (96 bytes)
  * kind 3 – transfers: the 92-byte transfer event followed by `u64 timestamp` (100 bytes)
* `1` NotLeader, followed by `u8 leader_hint` (0xFF = unknown)

Lookups return only the objects that exist.

Result codes are listed in `crates/ledger/src/types.rs` (`ResultCode`);
`0 = Ok`, `1 = Exists` (idempotent success).

### Sessions

A client picks a random `client_id` and numbers its requests 1, 2, 3, ...
with exactly one request outstanding. A retry (timeout, connection error,
redirect) reuses the same number. The cluster stores the last request number
and reply per `client_id` in replicated state, so a request that was already
executed is answered from that cache instead of executing again.

## Replica frames

| tag  | name                  | fields |
|------|-----------------------|--------|
| 0x10 | RequestVote           | `u64 term, u8 candidate, u64 last_log_index, u64 last_log_term` |
| 0x11 | RequestVoteResponse   | `u64 term, u8 granted` |
| 0x12 | AppendEntries         | `u64 term, u8 leader, u64 prev_log_index, u64 prev_log_term, u64 leader_commit, u32 count, entries` |
| 0x13 | AppendEntriesResponse | `u64 term, u8 success, u64 match_index, u64 conflict_index, u64 conflict_term` |

Log entry: `u64 term, u64 index, u64 timestamp, u8 kind` then for kind 0
(no-op) nothing, for kind 1 (batch) `u32 count` and `count` encoded
ClientRequest bodies (without tag).

## Write-ahead log record

```
u32 payload_len | u32 crc32(payload) | payload
payload = u8 kind | body
  1 HardState  u64 term, u8 voted_for (0xFF = none)
  2 Entry      encoded log entry
  3 Truncate   u64 from_index
```
