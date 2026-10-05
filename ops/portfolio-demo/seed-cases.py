from pathlib import Path
import subprocess,os
text=subprocess.check_output(['sudo','cat','/etc/fintech-demo/runtime.env'],text=True)
env={k:v.strip('"') for k,v in (s.split('=',1) for s in text.splitlines() if '=' in s)}
root=Path(__file__).resolve().parents[3]/'mule-ring-detector'
r=subprocess.run([str(root/'target/release/mrd'),'replay','--input',str(root/'.demo/sorted.csv'),'--model',str(root/'.demo/models/gbdt.json'),'--push','http://127.0.0.1:28121/api/alerts','--max-alerts','50','--alerts-out','/var/lib/fintech-demo/results/synthetic-alerts.jsonl'],env={**os.environ,**env},stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
if r.returncode: raise RuntimeError('Case seed failed; inspect service logs')
print('Seeded 50 synthetic alerts through the scoring model into the case desk.')
