#!/usr/bin/env python3
"""Install the isolated, loopback-only demo services. Secrets stay outside Git."""
from pathlib import Path
import subprocess, tempfile, os, secrets, shutil, hashlib
ROOT=Path(__file__).resolve().parents[3]
STATE=Path('/var/lib/fintech-demo')
changed_units=[]
def run(args, **kw): return subprocess.run(args, check=True, **kw)
def install(text, destination, mode='644'):
    with tempfile.NamedTemporaryFile('w', delete=False) as f: f.write(text); name=f.name
    try: run(['sudo','install','-m',mode,name,str(destination)])
    finally: os.unlink(name)
run(['sudo','install','-d','-o','kamicode','-g','kamicode','-m','750',str(STATE)])
run(['sudo','install','-d','-m','700','/etc/fintech-demo'])
for d in ['releases','pg-socket','ledger-0','ledger-1','ledger-2','card','results']: (STATE/d).mkdir(exist_ok=True)
env_path=Path('/etc/fintech-demo/runtime.env')
if not subprocess.run(['sudo','test','-f',str(env_path)]).returncode:
    pass
else:
    pgpw=secrets.token_urlsafe(32)
    env={'JAVA_TOOL_OPTIONS':'-Xms64m -Xmx512m -XX:ActiveProcessorCount=4',
         'DB_URL':'jdbc:postgresql://127.0.0.1:56486/clearing_demo?reWriteBatchedInserts=true','DB_USER':'fintechdemo','DB_PASSWORD':pgpw,
         'ISSUER_DB_URL':'jdbc:postgresql://127.0.0.1:56486/issuer_demo','ISSUER_DB_USER':'fintechdemo','ISSUER_DB_PASSWORD':pgpw,
         'SPRING_DATASOURCE_URL':'jdbc:postgresql://127.0.0.1:56486/mrd_demo','SPRING_DATASOURCE_USERNAME':'fintechdemo','SPRING_DATASOURCE_PASSWORD':pgpw,
         'MRD_ANALYST1_PASSWORD':secrets.token_urlsafe(32),'MRD_ANALYST2_PASSWORD':secrets.token_urlsafe(32),
         'MRD_SUPERVISOR1_PASSWORD':secrets.token_urlsafe(32),'MRD_ENGINE_PASSWORD':secrets.token_urlsafe(32)}
    env['MRD_PUSH_PASSWORD']=env['MRD_ENGINE_PASSWORD']
    install('\n'.join(f'{k}="{v}"' for k,v in env.items())+'\n',env_path,'600')
if not (STATE/'pg/PG_VERSION').exists():
    run(['/usr/lib/postgresql/14/bin/initdb','-D',str(STATE/'pg'),'--auth-local=peer','--auth-host=scram-sha-256'],stdout=subprocess.DEVNULL)
    with (STATE/'pg/postgresql.conf').open('a') as f:
        f.write("\nlisten_addresses = '127.0.0.1'\nport = 56486\nunix_socket_directories = '/var/lib/fintech-demo/pg-socket'\nmax_connections = 100\nshared_buffers = '128MB'\n")

def release_jar(source):
    digest=hashlib.sha256(source.read_bytes()).hexdigest()[:16]
    destination=STATE/'releases'/f'{source.parent.parent.name}-{digest}.jar'
    if not destination.exists(): shutil.copyfile(source,destination)
    return destination

def unit(name,command,cwd=ROOT,extra='',after='fintech-demo-postgres.service'):
    text=f'''[Unit]
Description=Fintech portfolio demo: {name}
After=network.target {after}
[Service]
Type=simple
User=kamicode
Group=kamicode
WorkingDirectory={cwd}
EnvironmentFile=/etc/fintech-demo/runtime.env
ExecStart={command}
Restart=on-failure
RestartSec=3
TimeoutStopSec=30
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/var/lib/fintech-demo
LimitNOFILE=65536
{extra}
[Install]
WantedBy=multi-user.target
'''
    destination=Path('/etc/systemd/system')/f'fintech-demo-{name}.service'
    if destination.exists() and destination.read_text()!=text: changed_units.append(destination.name)
    install(text,destination)
unit('postgres','/usr/lib/postgresql/14/bin/postgres -D /var/lib/fintech-demo/pg',after='')
for n in range(3):
    unit(f'ledger-{n}',f'{ROOT}/quorum-ledger/target/release/quorum-ledger-server --id {n} --cluster 127.0.0.1:28100,127.0.0.1:28101,127.0.0.1:28102 --data {STATE}/ledger-{n}')
unit('payments',f'/usr/bin/java -jar {release_jar(ROOT/"quorum-ledger/payments-api/target/payments-api-0.1.0-SNAPSHOT.jar")} --server.address=127.0.0.1 --server.port=28103 --quorum-ledger.cluster[0]=127.0.0.1:28100 --quorum-ledger.cluster[1]=127.0.0.1:28101 --quorum-ledger.cluster[2]=127.0.0.1:28102')
unit('netting',f'{ROOT}/cross-border-clearing/netting-engine/target/release/netting-engine',extra='Environment=BIND_ADDR=127.0.0.1:28110')
unit('clearing',f'/usr/bin/java -jar {release_jar(ROOT/"cross-border-clearing/clearing-gateway/target/clearing-gateway-0.1.0-SNAPSHOT.jar")} --server.address=127.0.0.1 --server.port=28111',extra='UnsetEnvironment=SPRING_DATASOURCE_URL SPRING_DATASOURCE_USERNAME SPRING_DATASOURCE_PASSWORD\nEnvironment=ENGINE_URL=http://127.0.0.1:28110\nEnvironment=DB_POOL_SIZE=12\nEnvironment=CYCLE_INTERVAL=60s')
unit('cases',f'/usr/bin/java -jar {release_jar(ROOT/"mule-ring-detector/case-service/target/case-service.jar")} --server.address=127.0.0.1 --server.port=28121 --spring.datasource.hikari.maximum-pool-size=12')
unit('detector',f'{ROOT}/mule-ring-detector/target/release/mrd serve --model {ROOT}/mule-ring-detector/.demo/models/gbdt.json --listen 127.0.0.1:28120 --push http://127.0.0.1:28121/api/alerts')
unit('hsm',f'{ROOT}/card-auth-switch/target/release/hsm serve --listen 127.0.0.1:28131 --lmk-file {ROOT}/card-auth-switch/config/lmk.test.hex')
unit('issuer',f'/usr/bin/java -jar {release_jar(ROOT/"card-auth-switch/issuer/target/issuer-backoffice-0.1.0.jar")} --server.address=127.0.0.1 --server.port=28132 --spring.datasource.hikari.maximum-pool-size=12',extra='UnsetEnvironment=SPRING_DATASOURCE_URL SPRING_DATASOURCE_USERNAME SPRING_DATASOURCE_PASSWORD\nEnvironment=ISSUER_PORT=28132')
config=(ROOT/'card-auth-switch/config/switch.toml').read_text().replace('127.0.0.1:27583','127.0.0.1:28130').replace('127.0.0.1:27910','127.0.0.1:28131').replace('127.0.0.1:27180','127.0.0.1:28132').replace('card_refresh_secs = 5','card_refresh_secs = 1').replace('.run/card-snapshot.json',str(STATE/'card/card-snapshot.json')).replace('.run/stip-journal.log',str(STATE/'card/stip-journal.log'))
(STATE/'card/switch.toml').write_text(config)
unit('switch',f'{ROOT}/card-auth-switch/target/release/card-switch --config {STATE}/card/switch.toml',after='fintech-demo-issuer.service fintech-demo-hsm.service')
unit('web',f'{shutil.which("node")} {ROOT}/quorum-ledger/ops/portfolio-demo/server.mjs',after='fintech-demo-payments.service fintech-demo-clearing.service fintech-demo-cases.service fintech-demo-switch.service')
run(['sudo','systemctl','daemon-reload'])
run(['sudo','systemctl','enable','--now','fintech-demo-postgres.service'])
import time
for _ in range(60):
    if subprocess.run(['/usr/lib/postgresql/14/bin/pg_isready','-h',str(STATE/'pg-socket'),'-p','56486'],stdout=subprocess.DEVNULL).returncode==0: break
    time.sleep(.5)
else: raise RuntimeError('Postgres did not start')
# Reuse protected credentials in memory; never emit their values.
env_text=subprocess.check_output(['sudo','cat',str(env_path)],text=True)
env={k:v.strip('"') for k,v in (line.split('=',1) for line in env_text.splitlines() if '=' in line)}
sql="DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'fintechdemo') THEN CREATE ROLE fintechdemo LOGIN PASSWORD '%s'; END IF; END $$;" % env['DB_PASSWORD']
run(['/usr/lib/postgresql/14/bin/psql','-h',str(STATE/'pg-socket'),'-p','56486','-d','postgres','-v','ON_ERROR_STOP=1'],input=sql,text=True,stdout=subprocess.DEVNULL)
for db in ['clearing_demo','issuer_demo','mrd_demo']:
    found=subprocess.check_output(['/usr/lib/postgresql/14/bin/psql','-h',str(STATE/'pg-socket'),'-p','56486','-d','postgres','-tAc',f"SELECT 1 FROM pg_database WHERE datname='{db}'"],text=True).strip()
    if not found: run(['/usr/lib/postgresql/14/bin/createdb','-h',str(STATE/'pg-socket'),'-p','56486','-O','fintechdemo',db])
unit('feed',f'/usr/bin/python3 {ROOT}/quorum-ledger/ops/portfolio-demo/feed-clearing.py',extra='Type=oneshot\nRestart=on-failure\nRestartSec=30\nTimeoutStartSec=120',after='fintech-demo-clearing.service')
install('[Unit]\nDescription=Keep fintech clearing demo supplied with synthetic traffic\n[Timer]\nOnBootSec=10min\nOnUnitActiveSec=1h\nPersistent=true\n[Install]\nWantedBy=timers.target\n',Path('/etc/systemd/system/fintech-demo-feed.timer'))
run(['sudo','systemctl','daemon-reload'])
run(['sudo','systemctl','enable','--now','fintech-demo-feed.timer'])
services=[f'fintech-demo-{n}.service'  for n in ['ledger-0','ledger-1','ledger-2','payments','netting','clearing','cases','detector','hsm','issuer','switch','web']]
run(['sudo','systemctl','enable','--now',*services])
if changed_units: run(['sudo','systemctl','restart',*changed_units])
print('Installed and started 13 persistent demo services; credentials kept outside the project.')
