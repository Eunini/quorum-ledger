import http from 'node:http';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import { readFile, writeFile, rename } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
const exec = promisify(execFile);
const ROOT=fileURLToPath(new URL('../../../',import.meta.url)).replace(/\/$/,''), STATE='/var/lib/fintech-demo';
const projects=['quorum-ledger','cross-border-clearing','mule-ring-detector','card-auth-switch'];
const prefix='/fintech/';
const calls=new Map();
let active=false;
let quota=await readFile(`${STATE}/quota.json`,'utf8').then(JSON.parse).catch(()=>({date:'',count:0}));
const basic=Buffer.from(`analyst1:${process.env.MRD_ANALYST1_PASSWORD}`).toString('base64');
async function save(path,value) { const temporary=`${path}.${randomUUID()}.tmp`; await writeFile(temporary,JSON.stringify(value)); await rename(temporary,path); }
function reply(res,status,value,type='application/json') {
  res.writeHead(status,{'Content-Type':type,'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'});
  res.end(Buffer.isBuffer(value)?value:type==='application/json'?JSON.stringify(value):value);
}
async function get(url,options={}) {
  const r=await fetch(url,{...options,signal:AbortSignal.timeout(12000)});
  const body=await r.json();
  if(!r.ok) throw Object.assign(new Error('Backend request failed'),{status:r.status,detail:body});
  return body;
}
const payment=(path,body,key)=>get(`http://127.0.0.1:28103${path}`,body?{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':key||randomUUID()},body:JSON.stringify(body)}:{});
function tcp(port) { return new Promise(resolve=>{ const s=net.connect({host:'127.0.0.1',port}); s.setTimeout(600); const finish=ok=>{s.destroy();resolve(ok)}; s.on('connect',()=>finish(true));s.on('error',()=>finish(false));s.on('timeout',()=>finish(false)); }); }
async function ledgerDemo() {
  const key=randomUUID(), steps=[];
  const treasury=await payment('/accounts',{ledger:840,code:1,preventOverdraft:false});
  const customer=await payment('/accounts',{ledger:840,code:2,preventOverdraft:true});
  const merchant=await payment('/accounts',{ledger:840,code:3,preventOverdraft:true});
  const transfer=(debit,credit,amount)=>({debitAccountId:debit,creditAccountId:credit,amount,ledger:840,code:1});
  await payment('/transfers',transfer(treasury.id,customer.id,100000));
  steps.push({label:'Fund synthetic account',detail:'Customer starts with $1,000.00',passed:true});
  const body=transfer(customer.id,merchant.id,2500);
  const first=await payment('/transfers',body,key);
  const replay=await payment('/transfers',body,key);
  steps.push({label:'Transfer and retry',detail:'$25.00 moves once; retry returns the same transfer',passed:replay.replayed&&first.id===replay.id,transferId:first.id});
  const hold=await payment('/holds',{...transfer(customer.id,merchant.id,4000),timeoutSeconds:60});
  const held=await payment(`/accounts/${customer.id}`);
  steps.push({label:'Reserve funds',detail:'$40.00 held; available balance is $935.00',passed:held.debitsPending===4000&&held.available===93500});
  await payment(`/holds/${hold.id}/capture`,{amount:2500});
  let rejected=false;
  try { await payment('/transfers',transfer(customer.id,merchant.id,100001)); } catch(e) { if(e.status>=400&&e.status<500) rejected=true; else throw e; }
  steps.push({label:'Overdraft prevention',detail:'A transfer above the available balance is rejected',passed:rejected});
  const accounts=await Promise.all([treasury.id,customer.id,merchant.id].map(id=>payment(`/accounts/${id}`)));
  const [t,c,m]=accounts;
  const balanced=accounts.reduce((n,a)=>n+a.debitsPosted-a.creditsPosted,0)===0;
  steps.push({label:'Partial capture and balance check',detail:'$25.00 captured, $15.00 released; debits equal credits',passed:c.available===95000&&c.debitsPending===0&&m.available===5000&&balanced});
  if(steps.some(s=>!s.passed)) throw new Error('Ledger invariant failed');
  return {at:new Date().toISOString(),passed:true,steps,accounts:accounts.map((a,i)=>({name:['Funding','Customer','Merchant'][i],...a})),replicas:await Promise.all([28100,28101,28102].map(async(p,i)=>({id:i,online:await tcp(p)})))};
}
async function cardDemo(scenario) {
  const {stdout}=await exec(`${ROOT}/card-auth-switch/target/release/termsim`,['--keys',`${ROOT}/card-auth-switch/config/test-keys.toml`,'web-demo','--switch','127.0.0.1:28130','--issuer','http://127.0.0.1:28132','--sequence',String(Date.now()%900000000+1000000),'--scenario',scenario],{timeout:20000,maxBuffer:1048576,env:{...process.env,RUST_LOG:'error'}});
  const result=JSON.parse(stdout.trim().split('\n').at(-1));
  const expected={approve:'00','wrong-pin':'55',tampered:'82',insufficient:'51'}[scenario];
  result.passed=result.responseCode===expected&&result.balanced&&result.balancesMatch;
  if(scenario==='approve') result.passed&&=result.arpcVerified&&result.replayCode==='00'&&result.replayHeldMinor===2500&&result.reversalCode==='00'&&result.finalAccount.heldMinor===0;
  if(!result.passed) throw new Error('Card scenario verification failed');
  return {at:new Date().toISOString(),...result};
}
async function proxy(res,port,path,auth=false) {
  const r=await fetch(`http://127.0.0.1:${port}${path}`,{headers:auth?{Authorization:`Basic ${basic}`}:{},signal:AbortSignal.timeout(12000)});
  const body=Buffer.from(await r.arrayBuffer());
  reply(res,r.status,body,r.headers.get('content-type')||'application/octet-stream');
}
const server=http.createServer(async(req,res)=>{
  try {
    const url=new URL(req.url,'http://localhost');
    if (req.method==='HEAD') req.method='GET';
    if(url.pathname==='/fintech' || projects.some(p=>url.pathname===prefix+p)) { res.writeHead(308,{Location:url.pathname+'/'});return res.end(); }
    if(url.pathname===prefix && req.method==='GET') return reply(res,200,`<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>Eunini · Fintech demos</title><style>body{font:18px system-ui;max-width:800px;margin:8vh auto;padding:24px;color:#14221f}li{margin:20px 0}a{color:#12644e}</style><h1>Fintech systems, running live.</h1><p>Java and Rust projects by Eunini. Synthetic data throughout.</p><ul>${projects.map(p=>`<li><a href="${prefix+p}/">${p}</a></li>`).join('')}</ul>`,'text/html');
    const match=url.pathname.match(/^\/fintech\/([^/]+)\/(.*)$/);
    if(!match||!projects.includes(match[1])) return reply(res,404,{error:'Not found'});
    const [,project,route]=match;
    if(req.method==='POST' && ['quorum-ledger','card-auth-switch'].includes(project) && route==='api/run') {
      const origin=req.headers.origin;
      if(origin && origin!==`https://${req.headers.host}`) return reply(res,403,{error:'Origin not allowed'});
      const ip=req.headers['x-real-ip']||req.socket.remoteAddress, now=Date.now(), day=new Date().toISOString().slice(0,10);
      if(active) return reply(res,429,{error:'A demo is running. Please try again in a few seconds.'});
      if(calls.has(ip)&&now-calls.get(ip)<10000) return reply(res,429,{error:'Please wait 10 seconds between demo runs.'});
      if(quota.date!==day) quota={date:day,count:0};
      if(quota.count>=1000) return reply(res,429,{error:'Today’s demo limit has been reached. Try again tomorrow.'});
      const scenario=url.searchParams.get('scenario')||'approve';
      if(project==='card-auth-switch'&&!['approve','wrong-pin','tampered','insufficient'].includes(scenario)) return reply(res,400,{error:'Unknown scenario'});
      active=true;calls.set(ip,now);quota.count++;
      try { await save(`${STATE}/quota.json`,quota); const result=await (project==='quorum-ledger'?ledgerDemo():cardDemo(scenario)); await save(`${STATE}/results/${project}.json`,result);return reply(res,200,result); }
      finally { active=false; }
    }
    if(req.method!=='GET'&&req.method!=='HEAD') return reply(res,405,{error:'This public demo is read only.'});
    if(['quorum-ledger','card-auth-switch'].includes(project)) {
      if(route==='api/status') {
        const last=await readFile(`${STATE}/results/${project}.json`,'utf8').then(JSON.parse).catch(()=>null);
        const ports=project==='quorum-ledger'?[28100,28101,28102,28103]:[28130,28131,28132];
        const online=(await Promise.all(ports.map(tcp))).every(Boolean);
        return reply(res,200,{online,last});
      }
      if(!['','index.html','app.js','style.css'].includes(route)) return reply(res,404,{error:'Not found'});
      const file=route||'index.html',types={'index.html':'text/html; charset=utf-8','app.js':'text/javascript; charset=utf-8','style.css':'text/css; charset=utf-8'};
      return reply(res,200,await readFile(`${ROOT}/${project}/web/${file}`),types[file]);
    }
    if(project==='cross-border-clearing') {
      if(!['','index.html','api/dashboard'].includes(route)) return reply(res,404,{error:'Not found'});
      return await proxy(res,28111,`/${route}${url.search}`);
    }
    if(project==='mule-ring-detector') {
      if(route==='demo-config.json') return reply(res,200,{enabled:true,readOnly:true});
      if(['app.js','style.css'].includes(route)) return reply(res,200,await readFile(`${ROOT}/${project}/case-service/src/main/resources/static/${route}`),route.endsWith('js')?'text/javascript':'text/css');
      if(!/^(?:|index\.html|api\/me|api\/cases(?:\/\d+(?:\/(?:audit|graph))?)?)$/.test(route)) return reply(res,404,{error:'Not found'});
      if(route==='api/cases' && (Number(url.searchParams.get('size')||20)>100 || Number(url.searchParams.get('page')||0)>100)) return reply(res,400,{error:'Page limit exceeded'});
      return await proxy(res,28121,`/${route}${url.search}`,route.startsWith('api/'));
    }
  } catch(error) {
    console.error('Public demo request failed:',error.message);
    if(!res.headersSent) reply(res,503,{error:'Demo temporarily unavailable. Please refresh in a moment.'});
    else res.end();
  }
});
server.requestTimeout=25000;server.headersTimeout=10000;
server.listen(28190,'127.0.0.1',()=>console.log('Portfolio demo gateway listening on loopback:28190'));
setInterval(()=>{const cutoff=Date.now()-60000;for(const [k,v] of calls)if(v<cutoff)calls.delete(k)},60000).unref();
