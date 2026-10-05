import { readFile } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID, createHmac, createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import * as S from './store.mjs';
const ROOT=fileURLToPath(new URL('../../../',import.meta.url)).replace(/\/$/,'');
const exec=promisify(execFile);
const curr={USD:840,EUR:978,GBP:826};
const now=()=>new Date().toISOString();
export async function backend(port,path,method='GET',body,headers={}) {
 const r=await fetch(`http://127.0.0.1:${port}${path}`,{method,headers:{...(body!==undefined?{'Content-Type':'application/json'}:{}),...headers},body:body===undefined?undefined:typeof body==='string'?body:JSON.stringify(body),signal:AbortSignal.timeout(30000)});
 const type=r.headers.get('content-type')||'';
 const data=type.includes('json')?await r.json():Buffer.from(await r.arrayBuffer());
 if(!r.ok)throw S.error(r.status,data.detail||data.message||data.error||data.title||'The operation could not be completed.');
 return data;
}
const ledger=(path,body,key)=>backend(28103,path,body===undefined?'GET':'POST',body,key?{'Idempotency-Key':key}:{});
function record(ctx,p,kind,id,data,action){S.put(ctx,p,kind,id,data);S.audit(ctx,p,action,{id,...data});return {id,...data};}
const privateCard=(ctx,id)=>`${S.STATE}/cards/${ctx.workspace}-${id}.json`;
const privateRequest=(ctx,id)=>`${S.STATE}/cards/${ctx.workspace}-${id}.request.json`;
async function terminal(args){try{const {stdout}=await exec(`${ROOT}/card-auth-switch/target/release/termsim`,['--keys',`${ROOT}/card-auth-switch/config/test-keys.toml`,...args],{timeout:30000,maxBuffer:1048576,env:{...process.env,RUST_LOG:'error'}});return JSON.parse(stdout.trim().split('\n').at(-1));}catch(e){throw S.error(e.killed?503:422,e.killed?'The card service timed out. Retry this request to check its existing result.':'The card operation failed. Check its status, available funds, and transaction details.');}}
function identity(ctx){const role=ctx.role==='SUPERVISOR'?'SUPERVISOR':'ANALYST';const data={actor:'u_'+ctx.id,role,time:Math.floor(Date.now()/1000),members:S.members(ctx).map(m=>({actor:'u_'+m.id,name:m.name,email:m.email,role:m.role==='SUPERVISOR'?'SUPERVISOR':'ANALYST'}))};const encoded=Buffer.from(JSON.stringify(data)).toString('base64url');return {'X-Workspace-Identity':encoded,'X-Workspace-Signature':createHmac('sha256',process.env.MRD_GATEWAY_SECRET).update(encoded).digest('hex')};}
const cases=(ctx,path,method='GET',body)=>backend(28121,path,method,body,identity(ctx));
const engineAuth={'Authorization':'Basic '+Buffer.from(`engine:${process.env.MRD_ENGINE_PASSWORD}`).toString('base64')};
function keepCases(ctx,result){for(const r of result.results||[])if(r.caseId)S.put(ctx,'mule-ring-detector','case',r.caseId,{reference:r.caseReference});}
const escape=s=>String(s).replace(/[<>&"']/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&apos;'}[c]));
const accountXML=s=>/^[A-Z]{2}/.test(s)?`<IBAN>${escape(s)}</IBAN>`:`<Othr><Id>${escape(s)}</Id></Othr>`;
function pacs(body,id,msg) {return `<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"><FIToFICstmrCdtTrf><GrpHdr><MsgId>${msg}</MsgId><CreDtTm>${now()}</CreDtTm><NbOfTxs>1</NbOfTxs><IntrBkSttlmDt>${now().slice(0,10)}</IntrBkSttlmDt><SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf><InstgAgt><FinInstnId><BICFI>${escape(body.from)}</BICFI></FinInstnId></InstgAgt></GrpHdr><CdtTrfTxInf><PmtId><EndToEndId>${escape(body.reference)}</EndToEndId><TxId>${msg}</TxId><UETR>${id}</UETR></PmtId><IntrBkSttlmAmt Ccy="${body.currency}">${escape(body.amount)}</IntrBkSttlmAmt><ChrgBr>SHAR</ChrgBr><Dbtr><Nm>${escape(body.sender)}</Nm></Dbtr><DbtrAcct><Id>${accountXML(body.senderAccount)}</Id></DbtrAcct><DbtrAgt><FinInstnId><BICFI>${escape(body.from)}</BICFI></FinInstnId></DbtrAgt><CdtrAgt><FinInstnId><BICFI>${escape(body.to)}</BICFI></FinInstnId></CdtrAgt><Cdtr><Nm>${escape(body.beneficiary)}</Nm></Cdtr><CdtrAcct><Id>${accountXML(body.beneficiaryAccount)}</Id></CdtrAcct></CdtTrfTxInf></FIToFICstmrCdtTrf></Document>`;}

export async function state(ctx,p){
 const activity=S.history(ctx,p);
 if(p==='quorum-ledger'){
  const accounts=await Promise.all(S.list(ctx,p,'account').filter(a=>!a.internal).map(async a=>({...a,...await ledger(`/accounts/${a.id}`)})));
  const transactions=S.list(ctx,p,'transaction');const holds=S.list(ctx,p,'hold').map(h=>({...h,status:h.status==='ACTIVE'&&h.expiresAt&&Date.parse(h.expiresAt)<Date.now()?'EXPIRED':h.status}));
  return {accounts,transactions,holds,activity};
 }
 if(p==='cross-border-clearing'){
  const [dashboard,participants,rates]=await Promise.all([backend(28111,'/api/dashboard'),backend(28111,'/api/participants'),backend(28111,'/api/fx/rates')]);
  const payments=await Promise.all(S.list(ctx,p,'payment').map(async o=>({...o,...await backend(28111,`/payments/${o.id}/status`)})));
  return {dashboard,participants,rates,payments,activity};
 }
 if(p==='mule-ring-detector'){
  const owned=await Promise.all(S.list(ctx,p,'case').map(async c=>await cases(ctx,`/api/cases/${c.id}`)));
  return {cases:owned.sort((a,b)=>b.updatedAt.localeCompare(a.updatedAt)),scores:S.list(ctx,p,'score'),activity};
 }
 if(p==='card-auth-switch'){
  const cards=await Promise.all(S.list(ctx,p,'card').map(async c=>({...c,account:await backend(28132,`/api/v1/accounts/${c.accountId}`)})));
  const transactions=S.list(ctx,p,'authorization'),disputes=await Promise.all(S.list(ctx,p,'dispute').map(d=>backend(28132,`/api/v1/disputes/${d.id}`)));
  const trial=await backend(28132,'/api/v1/ledger/trial-balance');
  return {cards,transactions,disputes,ledger:{balanced:trial.balanced,balancesMatch:trial.materialisedBalancesMatch},activity};
 }
}

export async function mutate(ctx,p,route,b,key){
 key=createHash('sha256').update(ctx.workspace+'|'+p+'|'+key).digest('hex');
 if(p==='quorum-ledger'){
  if(route==='accounts'){
   if(S.list(ctx,p,'account').length>=100)throw S.error(422,'The workspace account limit has been reached.');
   const name=S.text(b.name,'Account name',64),currency=b.currency||'USD';if(!curr[currency])throw S.error(400,'Choose USD, EUR, or GBP.');
   const a=await ledger('/accounts',{ledger:curr[currency],code:2,preventOverdraft:true},key);
   return record(ctx,p,'account',a.id,{name,currency,archived:false},'Account created');
  }
  const ac=route.match(/^accounts\/([^/]+)\/(fund|archive)$/);
  if(ac){const a=S.object(ctx,p,'account',ac[1]);if(a.internal)throw S.error(403,'Account unavailable.');if(ac[2]==='archive'){const balance=await ledger(`/accounts/${a.id}`);if(balance.available!==0||balance.debitsPending!==0)throw S.error(409,'An account must have no balance or holds before it can be archived.');return record(ctx,p,'account',a.id,{...a,archived:true},'Account archived');}
   if(a.archived)throw S.error(409,'Account is archived.');
   const n=S.amount(b.amount),reference=S.text(b.reference||'Funding','Reference',140);
   let funding=S.list(ctx,p,'account').find(v=>v.internal&&v.currency===a.currency);
   if(!funding){const t=await ledger('/accounts',{ledger:curr[a.currency],code:1,preventOverdraft:false},key+'-source');funding=S.put(ctx,p,'account',t.id,{name:'Funding source',currency:a.currency,internal:true});}
   const result=await ledger('/transfers',{debitAccountId:funding.id,creditAccountId:a.id,amount:n,ledger:curr[a.currency],code:1},key);
   return record(ctx,p,'transaction',result.id,{...result,currency:a.currency,memo:reference,status:'POSTED',fromName:'Funding source',toName:a.name},'Account funded');
  }
  if(route==='transfers'||route==='holds'){
   const from=S.object(ctx,p,'account',b.from),to=S.object(ctx,p,'account',b.to);
   if(from.internal||to.internal||from.archived||to.archived||from.id===to.id)throw S.error(400,'Choose two different active accounts.');
   if(from.currency!==to.currency)throw S.error(400,'Both accounts must use the same currency.');
   const n=S.amount(b.amount),seconds=route==='holds'?Number(b.timeoutSeconds||3600):0;
   if(!Number.isInteger(seconds)||seconds<1&&route==='holds'||seconds>31536000)throw S.error(400,'Invalid hold duration.');
   const result=await ledger('/'+route,{debitAccountId:from.id,creditAccountId:to.id,amount:n,ledger:curr[from.currency],code:1,...(route==='holds'?{timeoutSeconds:seconds}:{})},key);
   const value={...result,currency:from.currency,memo:String(b.memo||'').slice(0,140),status:route==='holds'?'ACTIVE':'POSTED',fromName:from.name,toName:to.name,...(route==='holds'?{expiresAt:new Date(Date.now()+seconds*1000).toISOString()}:{})};
   return record(ctx,p,route==='holds'?'hold':'transaction',result.id,value,route==='holds'?'Funds reserved':'Transfer posted');
  }
  const hold=route.match(/^holds\/([^/]+)\/(capture|void)$/);
  if(hold){const h=S.object(ctx,p,'hold',hold[1]);if(h.status!=='ACTIVE')throw S.error(409,'This hold has already been resolved.');const n=hold[2]==='capture'?(b.amount?S.amount(b.amount):h.amount):0;
   const result=await ledger(`/holds/${h.id}/${hold[2]}`,hold[2]==='capture'?{amount:n}:{},key);
   record(ctx,p,'hold',h.id,{...h,status:hold[2]==='capture'?'CAPTURED':'VOIDED',capturedMinor:n},hold[2]==='capture'?'Hold captured':'Hold released');
   return S.put(ctx,p,'transaction',result.id,{...result,currency:h.currency,memo:h.memo,status:'POSTED',fromName:h.fromName,toName:h.toName});
  }
 }
 if(p==='cross-border-clearing'){
  if(route==='payments'){
   S.amount(b.amount);const participants=await backend(28111,'/api/participants');const from=participants.find(x=>x.bic===b.from),to=participants.find(x=>x.bic===b.to);if(!from||!to||from.bic===to.bic)throw S.error(400,'Choose two different participants.');
   for(const field of ['sender','beneficiary','senderAccount','beneficiaryAccount'])b[field]=S.text(b[field],field,field.includes('Account')?34:140);
   b.reference=S.text(b.reference||'PAY-'+key.slice(0,20),'Reference',35);b.currency=from.currency;
   const {id,xml}=S.intent(ctx,p,key,()=>{const id=randomUUID();return {id,xml:pacs(b,id,'WEB'+key.slice(0,30))};});
   const report=await backend(28111,'/iso20022/pacs.008','POST',xml,{'Content-Type':'application/xml'});
   let status;try{status=await backend(28111,`/payments/${id}/status`);}catch{throw S.error(422,Buffer.from(report).toString().match(/<AddtlInf>(.*?)<\/AddtlInf>/)?.[1]||'Payment validation failed. Check the account format and participant country.');}
   return record(ctx,p,'payment',id,{sender:b.sender,beneficiary:b.beneficiary,reference:b.reference,xml,responseXml:Buffer.from(report).toString(),...status},'Payment submitted');
  }
  if(route==='quotes'){const result=await backend(28111,'/api/fx/quotes','POST',{sourceCurrency:b.sourceCurrency,targetCurrency:b.targetCurrency,amount:b.amount});S.audit(ctx,p,'FX quote requested',result);return result;}
  if(route==='cycles/close'){const result=await backend(28111,'/api/admin/cycles/close','POST');S.audit(ctx,p,'Settlement cycle closed',{cycleId:result.id,status:result.status});return result;}
  if(route==='liquidity/run'){const result=await backend(28111,'/api/admin/lsm/run','POST');S.audit(ctx,p,'Liquidity queue processed',result);return result;}
 }
 if(p==='mule-ring-detector'){
  if(route==='evidence/import'){
   if(S.list(ctx,p,'case').length)throw S.error(409,'Sample evidence is already loaded. You can score new transactions from the monitoring page.');
   const rows=(await readFile(`${S.STATE}/results/synthetic-alerts.jsonl`,'utf8')).trim().split('\n').map(JSON.parse);
   const prefix=ctx.workspace.replace(/-/g,'').slice(0,16)+'-';
   const acct=a=>prefix+a;
   for(const row of rows){row.alertId=prefix+row.alertId;row.fromAccount=acct(row.fromAccount);row.toAccount=acct(row.toAccount);row.ringId=prefix+row.ringId;row.ringAccounts=row.ringAccounts.map(acct);for(const d of row.detectors||[])for(const e of d.edges||[]){e.from=acct(e.from);e.to=acct(e.to);}}
   const result=await backend(28121,'/api/alerts','POST',rows,engineAuth);keepCases(ctx,result);S.audit(ctx,p,'Evidence imported',{alerts:result.accepted,cases:new Set(result.results.map(r=>r.caseId)).size});return result;
  }
  if(route==='transactions/score'){
   const from=S.text(b.fromAccount,'Source account',24),to=S.text(b.toAccount,'Destination account',24);const n=S.amount(b.amount),prefix=ctx.workspace.replace(/-/g,'').slice(0,16);
   const result=await backend(28120,'/v1/transactions','POST',{id:S.intent(ctx,p,key,()=>({id:S.nextId('score')})).id,timestamp:b.timestamp||now(),fromBank:prefix,fromAccount:from,toBank:prefix,toAccount:to,amount:n/100,currency:'US Dollar',paymentFormat:b.paymentFormat||'ACH'});
   const score=result[0];if(score.alertPayload){const ingested=await backend(28121,'/api/alerts','POST',score.alertPayload,engineAuth);keepCases(ctx,ingested);}
   return record(ctx,p,'score',score.txId,{...score,fromAccount:from,toAccount:to,amount:n/100,timestamp:b.timestamp||now()},'Transaction scored');
  }
  const action=route.match(/^cases\/(\d+)\/(assign|transition|comments|filing-request|filing-approval|filing-rejection)$/);
  if(action){S.object(ctx,p,'case',action[1]);if(action[2].includes('filing-approval')||action[2].includes('filing-rejection')){if(ctx.role!=='SUPERVISOR')throw S.error(403,'A different team member with the supervisor role must review this filing.');}
   if(action[2]==='assign')b={assignee:'u_'+ctx.id};const result=await cases(ctx,`/api/cases/${action[1]}/${action[2]}`,'POST',b);S.audit(ctx,p,action[2],{caseId:action[1]});return result;
  }
 }
 if(p==='card-auth-switch'){
  if(route==='cards'){
   if(S.list(ctx,p,'card').length>=50)throw S.error(422,'This workspace supports up to 50 cards.');
   const name=S.text(b.name,'Cardholder name',64),opening=b.openingBalance==='0'?0:S.amount(b.openingBalance||'0.01'),{id,sequence}=S.intent(ctx,p,key,()=>({id:randomUUID(),sequence:S.nextId('card')}));
   const result=await terminal(['issue-card','--issuer','http://127.0.0.1:28132','--sequence',String(sequence),'--name',name,'--opening-minor',String(opening),'--out',privateCard(ctx,id)]);
   return record(ctx,p,'card',id,result,'Card issued');
  }
  const cardAction=route.match(/^cards\/([^/]+)\/(status|deposit|authorize)$/);
  if(cardAction){const c=S.object(ctx,p,'card',cardAction[1]);
   if(cardAction[2]==='status'){if(!['ACTIVE','BLOCKED','LOST','STOLEN','CLOSED'].includes(b.status))throw S.error(400,'Invalid card status.');await backend(28132,`/api/v1/cards/${c.cardId}/status`,'POST',{status:b.status,reason:S.text(b.reason,'Reason',140)});return record(ctx,p,'card',c.id,{...c,status:b.status},'Card status changed');}
   if(cardAction[2]==='deposit'){const result=await backend(28132,`/api/v1/accounts/${c.accountId}/deposits`,'POST',{amountMinor:S.amount(b.amount),reference:key});S.audit(ctx,p,'Card account funded',{cardId:c.id,amount:b.amount});return result;}
   const amount=S.amount(b.amount),merchant=S.text(b.merchant,'Merchant',40);if(!/^[\x20-\x7e]+$/.test(merchant))throw S.error(400,'Use standard letters and numbers for the terminal merchant name.');
   if(!['chip-pin','chip','magstripe'].includes(b.mode)||!['valid','wrong-pin','tampered'].includes(b.verification))throw S.error(400,'Invalid entry mode or verification option.');
   const {id}=S.intent(ctx,p,key,()=>({id:randomUUID()}));const result=await terminal(['authorize-card','--switch','127.0.0.1:28130','--issuer','http://127.0.0.1:28132','--card',privateCard(ctx,c.id),'--request-file',privateRequest(ctx,id),'--amount-minor',String(amount),'--merchant',merchant,'--mode',b.mode,'--verification',b.verification]);
   const {account,...safe}=result;return record(ctx,p,'authorization',id,{...safe,cardId:c.id,cardLast4:c.last4},'Card authorization');
  }
  const auth=route.match(/^authorizations\/([^/]+)\/(reverse|settle)$/);
  if(auth){const a=S.object(ctx,p,'authorization',auth[1]);if(a.status!=='AUTHORIZED')throw S.error(409,'This authorization has already been resolved or was declined.');const c=S.object(ctx,p,'card',a.cardId);
   const result=auth[2]==='reverse'?await terminal(['reverse-card','--switch','127.0.0.1:28130','--request-file',privateRequest(ctx,a.id)]):await terminal(['settle-card','--issuer','http://127.0.0.1:28132','--card',privateCard(ctx,c.id),'--request-file',privateRequest(ctx,a.id),'--record-id','P-'+a.id]);
   return record(ctx,p,'authorization',a.id,{...a,...result},auth[2]==='reverse'?'Authorization reversed':'Presentment settled');
  }
  if(route==='disputes'){const a=S.object(ctx,p,'authorization',b.authorizationId);if(a.status!=='SETTLED'||!a.recordId)throw S.error(409,'Only settled transactions can be disputed.');const result=await backend(28132,'/api/v1/disputes','POST',{presentmentRecordId:a.recordId,reasonCode:b.reasonCode,amountMinor:S.amount(b.amount),note:S.text(b.note,'Dispute note',1000)});return record(ctx,p,'dispute',result.id,{authorizationId:a.id},'Dispute opened');}
  const network=route.match(/^disputes\/(\d+)\/(network-settlement|representment)$/);
  if(network){S.object(ctx,p,'dispute',network[1]);const d=await backend(28132,`/api/v1/disputes/${network[1]}`);
   const settlement=network[2]==='network-settlement';if(d.state!==(settlement?'CHARGEBACK_SENT':'CHARGEBACK_SETTLED'))throw S.error(409,'This network event is not available in the current dispute state.');
   const reason=settlement?d.reasonCode:S.text(b.reason,'Representment reason',140);if(/[|\r\n]/.test(reason))throw S.error(400,'Use a single-line reason without pipe characters.');
   const {file}=S.intent(ctx,p,key,()=>{const id=(settlement?'CB-':'RP-')+key.slice(0,30);return {file:`HDR|CASCLR|1|FILE-${id}|${now().slice(0,10).replaceAll('-','')}|APPLICATION\n${settlement?'CHBK':'REPR'}|${id}|${d.arn}|${d.disputeRef}|${d.amountMinor}|${d.currency}|${reason}\nTRL|1|${d.amountMinor}\n`};});
   const result=await backend(28132,'/api/v1/clearing/files','POST',file,{'Content-Type':'text/plain'});if(result.lines[0].outcome!==(settlement?'CHARGEBACK_SETTLED':'REPRESENTED'))throw S.error(422,'The network event did not match this dispute.');
   S.audit(ctx,p,settlement?'Chargeback settlement recorded':'Acquirer representment recorded',{disputeId:d.id});return backend(28132,`/api/v1/disputes/${d.id}`);
  }
  const dispute=route.match(/^disputes\/(\d+)\/(evidence|chargeback|resolve)$/);
  if(dispute){S.object(ctx,p,'dispute',dispute[1]);if(dispute[2]==='evidence')b.submittedBy=ctx.name;if(dispute[2]==='resolve')b.actor=ctx.name;const result=await backend(28132,`/api/v1/disputes/${dispute[1]}/${dispute[2]}`,'POST',b);S.audit(ctx,p,'Dispute '+dispute[2],{disputeId:dispute[1]});return result;}
 }
 throw S.error(404,'Operation not found.');
}
export async function detail(ctx,p,route){
 if(p==='mule-ring-detector'){
  const m=route.match(/^cases\/(\d+)(?:\/(audit|graph|str.xml|summary.html|summary.pdf))?$/);if(m){S.object(ctx,p,'case',m[1]);return cases(ctx,'/api/'+route);}
 }
 if(p==='cross-border-clearing'){
  const m=route.match(/^cycles\/(\d+)\/(positions|transfers|statements\/([A-Z0-9]{8,11}))$/);if(m)return backend(28111,'/api/'+route);
  const pm=route.match(/^payments\/([^/]+)\/(message|status)$/);if(pm){const o=S.object(ctx,p,'payment',pm[1]);return pm[2]==='message'?Buffer.from(o.xml):backend(28111,`/payments/${o.id}/status`);}
 }
 throw S.error(404,'Record not found.');
}
