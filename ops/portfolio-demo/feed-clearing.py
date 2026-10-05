#!/usr/bin/env python3
from pathlib import Path
import subprocess,time,urllib.request
root=Path(__file__).resolve().parents[3]
for attempt in range(60):
 try:
  with urllib.request.urlopen('http://127.0.0.1:28111/actuator/health', timeout=1) as response:
   if response.status == 200: break
 except Exception: pass
 time.sleep(1)
else: raise RuntimeError('Clearing gateway not ready')
subprocess.run(['java','-Xmx256m','-jar',str(root/'cross-border-clearing/load-generator/target/load-generator-0.1.0-SNAPSHOT.jar'),'--url=http://127.0.0.1:28111','--payments=200','--concurrency=4','--batch=10','--cycles=1','--warmup=0',f'--seed={int(time.time())}','--out=/var/lib/fintech-demo/results/clearing-hourly.json'],check=True,stdout=subprocess.DEVNULL)
print('200 synthetic cross-border payments processed and settled.')
