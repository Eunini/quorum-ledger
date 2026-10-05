import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { test, after } from 'node:test';
const temporary=mkdtempSync(tmpdir()+'/fintech-store-');
process.env.FINTECH_STATE=temporary;
const S=await import('./store.mjs');
after(()=>rmSync(temporary,{recursive:true,force:true}));
function client(){let cookie;const res={setHeader:(name,value)=>{if(name==='Set-Cookie')cookie=value.split(';')[0];}};S.guest(res);const req={headers:{cookie}};let ctx=S.session(req);req.headers['x-csrf-token']=ctx.csrf;return {req,res,get ctx(){return S.session(req);},refresh(){req.headers.cookie=cookie;req.headers['x-csrf-token']=S.session(req).csrf;}};}
test('persistent objects and operation keys are isolated by workspace',async()=>{
 const a=client(),b=client();S.put(a.ctx,'quorum-ledger','account','123',{name:'Private account'});
 assert.equal(S.list(a.ctx,'quorum-ledger','account').length,1);assert.equal(S.list(b.ctx,'quorum-ledger','account').length,0);
 assert.throws(()=>S.object(b.ctx,'quorum-ledger','account','123'),{status:404});
 const key=randomUUID();let writes=0;
 const [first,repeated]=await Promise.all([1,2].map(()=>S.serialize(a.ctx,()=>S.once(a.ctx,'quorum-ledger',key,{amount:'5.00'},async()=>({number:++writes})))));
 assert.deepEqual(first,repeated);assert.equal(writes,1);
 await assert.rejects(S.once(a.ctx,'quorum-ledger',key,{amount:'6.00'},async()=>({})),{status:409});
 await S.once(b.ctx,'quorum-ledger',key,{amount:'5.00'},async()=>({number:++writes}));assert.equal(writes,2);
 const intent=S.intent(a.ctx,'card-auth-switch',key,()=>({id:randomUUID()}));assert.deepEqual(S.intent(a.ctx,'card-auth-switch',key,()=>({id:'wrong'})),intent);
});
test('saving a guest retains its records and invitations enforce independent identities',async()=>{
 const a=client(),b=client(),pwd=randomUUID();S.put(a.ctx,'quorum-ledger','account','saved',{name:'Retained'});
 await S.register(a.req,a.res,{name:'Analyst',email:randomUUID()+'@example.com',password:pwd});a.refresh();
 assert.equal(S.object(a.ctx,'quorum-ledger','account','saved').name,'Retained');
 await S.register(b.req,b.res,{name:'Reviewer',email:randomUUID()+'@example.com',password:randomUUID()});b.refresh();
 const invitation=S.invite(a.ctx,'SUPERVISOR');assert.throws(()=>S.acceptInvite(a.ctx,invitation.code),{status:403});
 S.acceptInvite(b.ctx,invitation.code);b.refresh();assert.equal(b.ctx.role,'SUPERVISOR');assert.equal(b.ctx.workspace,a.ctx.workspace);
 assert.throws(()=>S.acceptInvite(b.ctx,invitation.code),{status:404});assert.throws(()=>S.invite(b.ctx,'SUPERVISOR'),{status:403});
 assert.throws(()=>S.switchWorkspace(a.ctx,randomUUID()),{status:403});
 assert.throws(()=>S.guard({headers:{}},a.ctx),{status:403});S.guard(a.req,a.ctx);
 const stored=await S.password(pwd);assert.notEqual(stored,pwd);assert.equal(await S.verifyPassword(pwd,stored),true);assert.equal(await S.verifyPassword(randomUUID(),stored),false);
});
test('money accepts exact decimal minor units and rejects unsafe values',()=>{
 assert.equal(S.amount('0.01'),1);assert.equal(S.amount('123.45'),12345);
 for(const value of ['0','-1','0.001','NaN','1e4','999999999999'])assert.throws(()=>S.amount(value),{status:400});
});
