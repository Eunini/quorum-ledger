# Public fintech demos

This gateway runs the four portfolio backends behind one HTTPS host. The browser reads the real services; it never receives backend credentials. All accounts, cards, payments, and case evidence are synthetic.

| Project | Public demo | Private listeners |
| --- | --- | --- |
| Quorum Ledger | https://leads.realalma.com/fintech/quorum-ledger/ | replicas 28100–28102, Java API 28103 |
| Cross-Border Clearing | https://leads.realalma.com/fintech/cross-border-clearing/ | Rust engine 28110, Java gateway 28111 |
| Mule Ring Detector | https://leads.realalma.com/fintech/mule-ring-detector/ | Rust detector 28120, Java case service 28121 |
| Card Authorization Switch | https://leads.realalma.com/fintech/card-auth-switch/ | Rust switch 28130, HSM simulator 28131, Java issuer 28132 |

The Node gateway listens on loopback port 28190. Nginx terminates HTTPS. PostgreSQL 14 uses a separate cluster on loopback port 56486 and three separate databases. `fintech-demo-*` systemd services start at boot and restart after failure. Java heaps are capped at 512 MB. Each Java jar is copied to a file identified by its SHA-256 hash under the state directory before starting it, so rebuilding source jars cannot disrupt running services. Reinstalling restarts services whose release or unit changed.

## Behavior

Ledger runs use three fresh accounts to prove funding, an idempotent transfer, a hold, partial capture, and overdraft rejection. Card runs issue a fresh synthetic card, send ISO 8583 over TCP, verify the issuer response, replay an approved request, and reverse the hold. The four card scenarios cover approval, incorrect PIN, tampered ARQC, and insufficient funds.

Clearing is an operations dashboard with 40 fictional participants. An hourly feeder adds and settles 200 synthetic payments. The case desk is a public viewer of model-scored fixture evidence; graph, filtering, pagination, and audit browsing work without a sign-in. Case writes and report generation are not public.

The gateway exposes explicit route allowlists. Public run endpoints accept only fixed scenarios, allow one active run, wait 10 seconds between runs per visitor IP, and persist a 1,000-run daily quota. Nginx also limits request rate and body size. Other write routes and internal endpoints are rejected. This is a demonstration environment with public test HSM keys, not a payment processor.

## Installation on the demo VPS

Check out all four repositories as siblings under `/home/kamicode/aps/fintech/`. Build their Rust release binaries and Spring Boot jars. Java 21, Node, PostgreSQL 14, Nginx, and passwordless service administration are required on this host. The model must exist at `mule-ring-detector/.demo/models/gbdt.json`; train it on the checked-in synthetic fixture using the preparation and training commands in that project's demo script.

```sh
python3 setup-runtime.py
python3 seed-cases.py
python3 install-nginx.py
```

`setup-runtime.py` keeps generated passwords in root-readable `/etc/fintech-demo/runtime.env`. Runtime state lives under `/var/lib/fintech-demo/`; no credentials, database files, or card snapshots belong in Git. Installation reuses the protected environment file and existing databases. Seed cases once; the initial clearing seed can be reproduced with the load generator pointed at `http://127.0.0.1:28111`.

The Nginx installer adds a `/fintech/` location to the existing `leads.realalma.com` HTTPS server, saves the original configuration under `/etc/fintech-demo/`, validates configuration before reloading, and preserves the other application routes. The existing domain and certificate must already be configured.

## Verification

```sh
npm ci
npx playwright install chromium
npm run test:public
```

The browser check exercises the live ledger run, all four card scenarios, clearing tables, and case graphs on desktop and mobile. It also verifies that shared writes, internal card endpoints, and oversized case pages are rejected. Set `DEMO_URL` to test another public base URL. Screenshots, video, and verification JSON go to ignored `evidence/`.

```sh
sudo systemctl status 'fintech-demo-*'
sudo journalctl -u fintech-demo-web -n 30
sudo nginx -t
```

The gateway logs failures without recording request authorization headers. Keep the protected runtime environment out of logs and shell output. To remove public access, remove the fintech Nginx include, validate and reload Nginx, then stop the demo services and timer. Existing application services and databases are separate.
