from pathlib import Path
import subprocess, tempfile, os

def install(text,path,mode='644'):
 with tempfile.NamedTemporaryFile('w',delete=False) as f: f.write(text); temp=f.name
 try: subprocess.run(['sudo','install','-m',mode,temp,path],check=True)
 finally: os.unlink(temp)
config=Path('/etc/nginx/sites-available/realalma-leads')
current=config.read_text()
include=' include /etc/nginx/snippets/fintech-demo.conf;\n'
if include not in current:
 subprocess.run(['sudo','cp','-p',str(config),'/etc/fintech-demo/realalma-leads.before-fintech.conf'],check=True)
 current=current.replace(' location = /login {',include+' location = /login {')
 assert include in current
install('limit_req_zone $binary_remote_addr zone=fintech_demo:10m rate=10r/s;\n','/etc/nginx/conf.d/fintech-demo-limits.conf')
install('''location = /fintech { return 308 /fintech/; }
location ^~ /fintech/ {
 limit_req zone=fintech_demo burst=30 nodelay;
 limit_req_status 429;
 client_max_body_size 256k;
 proxy_pass http://127.0.0.1:28190;
 proxy_http_version 1.1;
 proxy_set_header Host $host;
 proxy_set_header X-Real-IP $remote_addr;
 proxy_set_header X-Forwarded-Proto https;
 proxy_read_timeout 30s;
 add_header X-Robots-Tag "noindex, nofollow" always;
 add_header X-Content-Type-Options "nosniff" always;
 add_header Strict-Transport-Security "max-age=31536000" always;
}
''','/etc/nginx/snippets/fintech-demo.conf')
install(current,str(config))
try: subprocess.run(['sudo','nginx','-t'],check=True)
except subprocess.CalledProcessError:
 subprocess.run(['sudo','cp','-p','/etc/fintech-demo/realalma-leads.before-fintech.conf',str(config)],check=True)
 raise
subprocess.run(['sudo','systemctl','reload','nginx'],check=True)
print('HTTPS application routes installed; existing application routes preserved.')
