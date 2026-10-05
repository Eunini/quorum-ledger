# Hosted financial applications

The gateway hosts four complete application workflows over the Java and Rust services. Create an account once to use all four. Accounts, memberships, sessions, saved records, and activity history persist in a private SQLite database. Financial and case state remains in the native backends. Each write checks workspace ownership and CSRF, and uses a workspace-scoped operation key.

| Application | Public URL | Private services |
| --- | --- | --- |
| Quorum Ledger | https://leads.realalma.com/fintech/quorum-ledger/ | replicas 28100–28102, Java payments 28103 |
| Cross-Border Clearing | https://leads.realalma.com/fintech/cross-border-clearing/ | Rust netting 28110, Java clearing 28111 |
| Mule Ring Detector | https://leads.realalma.com/fintech/mule-ring-detector/ | Rust scoring 28120, Java cases 28121 |
| Card Authorization Switch | https://leads.realalma.com/fintech/card-auth-switch/ | Rust switch 28130, simulated HSM 28131, Java issuer 28132 |

The UI lives in each repository's `web/` directory. `shared.js` and `shared.css` provide the common shell, session management, forms, and searchable, sortable, paginated tables. `applications.mjs` implements ownership-checked workflows. `store.mjs` persists workspace data and allocates operation identities before backend writes.

## Workflows

Ledger: currency accounts, funding, transfers, pending holds, partial capture, release, archive, and CSV export. The application calls the live three-node ledger through the Java payments API.

Clearing: schema-validated ISO 20022 payment instructions, private payment book, tracker history, FX quotes, participant positions, liquidity runs, cycle closure, settlement plans, and camt.053 statement downloads. Aggregate network settlement is shared. An hourly feeder keeps the fictional network supplied with traffic.

Investigations: Rust transaction scores, editable model-scored evidence, case assignment, comments, investigation and escalation, account graphs, XML/HTML/PDF reports, and independent STR filing approval. The gateway signs individual workspace identities for Java; it does not impersonate one shared analyst. Only another invited supervisor can approve a filing.

Cards: persistent issuance, masked card records, funding and status controls, chip/PIN/magstripe authorization, EMV response verification, held purchases, reversals, presentments, dispute evidence, chargeback clearing, acquirer representment, and state-valid resolutions. Card secrets and terminal requests stay in private files outside the repository.

## Installation

Check out the four repositories as siblings. Build the Rust release binaries and Java jars first. This host needs Java 21, Node 24 (built-in SQLite), PostgreSQL 14, Nginx, and passwordless service administration. Prepare the detector model using its synthetic dataset workflow. `seed-cases.py` also prepares the model-scored alert fixture imported into each workspace.

```sh
python3 setup-runtime.py
python3 seed-cases.py
python3 install-nginx.py
```

Runtime data lives under `/var/lib/fintech-demo/`. Generated credentials and the gateway signing key remain in root-readable `/etc/fintech-demo/runtime.env`. Installation preserves existing credentials, PostgreSQL databases, SQLite workspaces, and ledger state. Java jars are copied into immutable releases before services start, so rebuilding source jars cannot corrupt running services. Systemd's `fintech-demo-*` units supervise all thirteen services and the hourly network feed. These existing private unit and directory names are retained for migration compatibility.

The Node gateway listens on loopback 28190. Nginx terminates HTTPS, caps bodies at 256 KiB, and rate limits requests. Backend APIs and arbitrary proxy routes are unavailable from the public host. Dedicated PostgreSQL databases use a separate localhost cluster on port 56486. Java heaps are capped at 512 MB.

The Nginx installer scopes its changes to `/fintech/`, saves the original configuration, validates it, and reloads. The existing host and TLS certificate must already be configured.

## Verification

```sh
npm ci
npx playwright install chromium
npm test
npm run test:public
```

`test-store.mjs` checks password hashing, guest upgrades, independent invitations, workspace isolation, operation retries, CSRF, and exact decimal amounts. `test-public.mjs` uses the public HTTPS URL to exercise actual browser forms and backend workflows across all four applications, including ledger reconciliation, payment settlement, independent filing approval, card settlement and disputes. It captures desktop/mobile screenshots and video, verifies reload persistence and browser errors, and writes `verification.json`. Generated evidence is ignored by Git. Set `FINTECH_PUBLIC_URL` for another deployment. On this administered VPS, `FINTECH_TEST_RESTART=1` also verifies that sessions and records survive a gateway service restart.

```sh
sudo systemctl status 'fintech-demo-*'
sudo journalctl -u fintech-demo-web -n 30
sudo nginx -t
```

The gateway logs only failure classes, never request credentials. Keep the runtime environment and private card files out of shell output, logs, and Git. Funds, cards, participants, and evidence are synthetic; the applications are not connected to financial networks.
