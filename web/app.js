'use strict';
const $=id=>document.getElementById(id);
const money=n=>new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(n/100);
function node(tag,text,cls){const n=document.createElement(tag);n.textContent=text;if(cls)n.className=cls;return n}
function render(r){
  $('available').textContent=money(r.accounts[1].available);
  $('received').textContent=money(r.accounts[2].available);
  $('replicas').textContent=r.replicas.filter(n=>n.online).length+' / 3';
  $('steps').replaceChildren(...r.steps.map(s=>{const row=node('div','','step'),body=node('div','');body.append(node('strong',s.label),node('p',s.detail));row.append(node('span',s.passed?'✓':'!','pass'),body);return row}));
  const tr=node('tr','');['Account','Posted balance','Reserved'].forEach(s=>tr.append(node('th',s)));$('head').replaceChildren(tr);
  $('rows').replaceChildren(...r.accounts.map(a=>{const row=node('tr','');row.append(node('td',a.name),node('td',money(a.creditsPosted-a.debitsPosted),'num'),node('td',money(a.debitsPending),'num'));return row}));
  $('timestamp').textContent='Verified '+new Date(r.at).toLocaleString();$('raw').textContent=JSON.stringify(r,null,2);
}
async function status(){try{const r=await(await fetch('api/status')).json();$('status').textContent=r.online?'Live · Three replicas and payments API online':'Services starting…';$('status').classList.toggle('live',r.online);if(r.last)render(r.last);else $('steps').replaceChildren(node('p','Run the scenario to inspect the live ledger.'));}catch{$('status').textContent='Unable to reach services. Refresh to try again.'}}
$('run').addEventListener('click',async()=>{const b=$('run');b.disabled=true;b.textContent='Committing transactions…';$('error').classList.add('hidden');try{const res=await fetch('api/run',{method:'POST'}),r=await res.json();if(!res.ok)throw Error(r.error);render(r);}catch(e){$('error').textContent=e.message;$('error').classList.remove('hidden');}finally{b.disabled=false;b.textContent='Run ledger scenario';}});status();
