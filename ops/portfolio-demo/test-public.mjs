const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || 'playwright');
import { mkdir, writeFile } from 'node:fs/promises';
import assert from 'node:assert/strict';
const base=process.env.DEMO_URL || 'https://leads.realalma.com/fintech/', out=new URL('./evidence',import.meta.url).pathname;
await mkdir(out,{recursive:true});
const browser=await chromium.launch({headless:true});
const result=[];
try {
  const context=await browser.newContext({viewport:{width:1440,height:1000},recordVideo:{dir:out}});
  for(const project of ['quorum-ledger','cross-border-clearing','mule-ring-detector','card-auth-switch']) {
    const page=await context.newPage(), errors=[], failures=[];
    page.on('pageerror',e=>errors.push(e.message));page.on('requestfailed',r=>failures.push(r.url()));
    await page.goto(base+project+'/',{waitUntil:'networkidle'});
    if(project==='quorum-ledger') {
      await page.waitForSelector('#replicas:has-text("3 / 3")');
      await page.click('#run');await page.waitForSelector('#run:has-text("Run ledger scenario"):not([disabled])');
      assert.equal(await page.locator('#error').isVisible(),false);
      assert.equal(await page.locator('#available').textContent(),'$950.00');
    } else if(project==='card-auth-switch') {
      await page.waitForSelector('#status:has-text("Live")');
      // Public fixed scenarios exercise the real TCP/HSM/issuer path.
      for(const [scenario,code] of [['approve','00'],['wrong-pin','55'],['tampered','82'],['insufficient','51']]) {
        await page.waitForTimeout(10500);
        await page.selectOption('#scenario',scenario);await page.click('#run');
        await page.waitForSelector('#run:has-text("Send authorization"):not([disabled])',{timeout:25000});
        assert.equal(await page.locator('#error').isVisible(),false,await page.locator('#error').textContent());
        assert.ok((await page.locator('#response').textContent()).startsWith(code));
        assert.equal(await page.locator('#balanced').textContent(),'Yes');
      }
    } else if(project==='cross-border-clearing') {
      await page.waitForSelector('#tiles .value');
      assert.ok(Number((await page.locator('#tiles .value').first().textContent()).replaceAll(',',''))>=2000);
      await page.click('summary');assert.ok(await page.locator('#cycleTable tr').count()>1);
    } else {
      await page.waitForSelector('#case-rows tr[data-id]');
      assert.ok((await page.locator('#session-user').textContent()).includes('Read only'));
      assert.equal(await page.locator('#login-form').isVisible(),false);
      await page.locator('#case-rows tr[data-id]').first().click();
      await page.waitForSelector('[data-f="reference"]');await page.waitForSelector('[data-slot="graph"] canvas');
      assert.equal(await page.locator('.actions').count(),0);assert.equal(await page.locator('.downloads').count(),0);
      await page.click('#next');await page.click('#prev');
      const authHeaders=await page.evaluate(async()=>{const r=await fetch('api/me');return r.status});assert.equal(authHeaders,200);
    }
    await page.screenshot({path:out+'/'+project+'-desktop.png',fullPage:true});
    await page.setViewportSize({width:390,height:844});await page.waitForTimeout(300);
    await page.screenshot({path:out+'/'+project+'-mobile.png',fullPage:true});
    const overflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+2);
    result.push({project,url:base+project+'/',errors,failures,mobileOverflow:overflow});
    assert.deepEqual(errors,[]);assert.deepEqual(failures,[]);assert.equal(overflow,false,project+' mobile overflow');
    await page.close();console.log(project+' browser checks passed');
  }
  await context.close();
  for(const [route,method,expected] of [
    ['mule-ring-detector/api/cases/1/assign','POST',405],
    ['mule-ring-detector/api/alerts','POST',405],
    ['cross-border-clearing/api/admin/cycles/close','POST',405],
    ['card-auth-switch/internal/v1/cards/snapshot','GET',404],
    ['quorum-ledger/accounts','POST',405],
    ['mule-ring-detector/api/cases?size=1000','GET',400]]) {
    const r=await fetch(base+route,{method});assert.equal(r.status,expected,route);
  }
  await writeFile(out+'/verification.json',JSON.stringify({verifiedAt:new Date().toISOString(),result,protectedRoutes:'passed'},null,2));
} finally { await browser.close(); }
