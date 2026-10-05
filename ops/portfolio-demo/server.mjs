import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import * as S from './store.mjs';
import * as A from './applications.mjs';
const ROOT=fileURLToPath(new URL('../../../',import.meta.url)).replace(/\/$/,'');
const projects=['quorum-ledger','cross-border-clearing','mule-ring-detector','card-auth-switch'];
const rates=new Map();
const titles={'quorum-ledger':'Quorum Ledger','cross-border-clearing':'Cross-Border Clearing','mule-ring-detector':'Mule Ring Detector','card-auth-switch':'Card Authorization Switch'};
function respond(res,status,data,type='application/json'){res.writeHead(status,{'Content-Type':type,'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Referrer-Policy':'same-origin'});res.end(Buffer.isBuffer(data)||typeof data==='string'&&type!=='application/json'?data:JSON.stringify(data));}
function throttle(req,bucket,limit){const ip=req.headers['x-real-ip']||req.socket.remoteAddress,key=bucket+':'+ip;const current=rates.get(key)||{time:Date.now(),count:0};if(Date.now()-current.time>60000){current.time=Date.now();current.count=0;}if(++current.count>limit)throw S.error(429,'Too many requests. Please wait a minute and try again.');rates.set(key,current);}
async function body(req){let size=0,parts=[];for await(const part of req){size+=part.length;if(size>262144)throw S.error(413,'Request is too large.');parts.push(part);}if(!parts.length)return {};if(!(req.headers['content-type']||'').startsWith('application/json'))throw S.error(415,'Send a JSON request.');try{return JSON.parse(Buffer.concat(parts));}catch{throw S.error(400,'Request is not valid JSON.');}}
function checkOrigin(req){const origin=req.headers.origin;if(origin&&origin!==`https://${req.headers.host}`)throw S.error(403,'Request origin is not allowed.');}
const server=http.createServer(async(req,res)=>{
 try{
  const url=new URL(req.url,'http://localhost'),method=req.method==='HEAD'?'GET':req.method;
  if(method!=='GET')checkOrigin(req);
  if(url.pathname==='/fintech'||projects.some(p=>url.pathname==='/fintech/'+p)){res.writeHead(308,{Location:url.pathname+'/'});return res.end();}
  const auth=url.pathname.match(/^\/fintech\/(session|guest|register|signin|signout|workspace|invite|join)$/);
  if(auth){
   const route=auth[1];if(method==='GET'&&route==='session'){const ctx=S.session(req,false);return respond(res,200,ctx?S.user(ctx):{user:null});}
   if(method!=='POST')throw S.error(405,'Method not allowed.');throttle(req,route==='signin'||route==='register'?'auth':'session',route==='signin'||route==='register'?12:60);
   const b=await body(req);const ctx=S.session(req,false);
   if(route==='guest'){if(ctx)return respond(res,200,S.user(ctx));const created=S.guest(res);return respond(res,201,{id:created.user.id,workspace:created.workspace,csrf:created.csrf});}
   if(route==='register'){const value=await S.register(req,res,b);return respond(res,201,{id:value.user.id,workspace:value.workspace,csrf:value.csrf});}
   if(route==='signin'){const value=await S.signin(res,b);return respond(res,200,{id:value.user.id,workspace:value.workspace,csrf:value.csrf});}
   if(!ctx)throw S.error(401,'Sign in to continue.');S.guard(req,ctx);
   if(route==='signout'){S.logout(res,ctx);return respond(res,200,{signedOut:true});}
   if(route==='workspace'){S.switchWorkspace(ctx,b.id);return respond(res,200,{changed:true});}
   if(route==='invite')return respond(res,201,S.invite(ctx,b.role));
   if(route==='join')return respond(res,200,S.acceptInvite(ctx,b.code));
  }
  if(url.pathname==='/fintech/'&&method==='GET')return respond(res,200,await readFile(`${ROOT}/quorum-ledger/web/portfolio.html`),'text/html; charset=utf-8');
  const m=url.pathname.match(/^\/fintech\/([^/]+)\/(.*)$/);if(!m||!projects.includes(m[1]))throw S.error(404,'Page not found.');
  const [,project,route]=m;
  if(route.startsWith('api/')){
   const ctx=S.session(req);const endpoint=route.slice(4);throttle(req,'api',180);
   if(method==='GET'){
    if(endpoint==='state')return respond(res,200,await A.state(ctx,project));
    if(endpoint==='session')return respond(res,200,S.user(ctx));
    if(/\/(str\.xml|summary\.html|summary\.pdf)$/.test(endpoint))S.guard(req,ctx);
    const value=await A.detail(ctx,project,endpoint);
    const type=endpoint.endsWith('.pdf')?'application/pdf':endpoint.endsWith('.html')?'text/html; charset=utf-8':endpoint.endsWith('.xml')||endpoint.endsWith('/message')||endpoint.includes('/statements/')?'application/xml; charset=utf-8':'application/json';
    if(type!=='application/json')res.setHeader('Content-Disposition',`attachment; filename="${endpoint.split('/').at(-1).replace(/[^a-zA-Z0-9.]/g,'')||'statement'}${type.includes('xml')&&!endpoint.endsWith('.xml')?'.xml':''}"`);
    return respond(res,200,value,type);
   }
   if(method!=='POST')throw S.error(405,'Method not allowed.');S.guard(req,ctx);throttle(req,'write',90);
   const b=await body(req),key=req.headers['idempotency-key'];
   const value=await S.serialize(ctx,()=>S.once(ctx,project,key,{endpoint,...b},()=>A.mutate(ctx,project,endpoint,b,key)));
   return respond(res,200,value);
  }
  if(method!=='GET')throw S.error(405,'Method not allowed.');
  const file=route||'index.html';
  if(!['index.html','app.js','style.css','shared.js','shared.css'].includes(file))throw S.error(404,'Page not found.');
  const path=file.startsWith('shared.')?`${ROOT}/quorum-ledger/web/${file}`:`${ROOT}/${project}/web/${file}`;
  return respond(res,200,await readFile(path),file.endsWith('.js')?'text/javascript; charset=utf-8':file.endsWith('.css')?'text/css; charset=utf-8':'text/html; charset=utf-8');
 }catch(e){const status=e.status||503;if(status>=500)console.error('Application request failed:',e.name);if(!res.headersSent)respond(res,status,{error:e.status?e.message:'A service is unavailable. Your saved work is safe; please try again.'});else res.end();}
});
server.requestTimeout=35000;server.headersTimeout=10000;
server.listen(28190,'127.0.0.1',()=>console.log('Fintech applications listening on loopback:28190'));
setInterval(()=>{const cutoff=Date.now()-60000;for(const [key,v]of rates)if(v.time<cutoff)rates.delete(key);},60000).unref();
