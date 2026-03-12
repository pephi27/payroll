  // === Dashboard Full Backup & Restore ===
  (function(){
    const statusEl = document.getElementById('dashBackupStatus');
    const logEl = document.getElementById('dashBackupLog');
    const btnBackup = document.getElementById('dashBackupNow');
    const btnRestoreFile = document.getElementById('dashRestoreFile');
    const inputRestore = document.getElementById('dashRestoreInput');
    // Only keep local backup and file-restore controls
    if(!btnBackup || !btnRestoreFile || !inputRestore) return;

    // Lazily resolve Supabase to avoid capturing `null` before the module loads
    const getSupa = () => (window.supabase || null);
    const KV_TABLE = window.SUPABASE_TABLE || 'kv_store';
    const DTR_TABLE = 'dtr_punches';
    const LEGACY_DTR_TABLE = 'dtr_records';
    const BUCKET = 'backups';

    function setStatus(msg, isError){ if(statusEl){ statusEl.textContent = msg; statusEl.style.color = isError?'#b91c1c':'#334155'; } }
    function log(msg){ try{ logEl.style.display='block'; const d=document.createElement('div'); d.textContent=msg; logEl.appendChild(d);}catch(e){} }
    function clearLog(){ try{ logEl.innerHTML=''; logEl.style.display='none'; }catch(e){} }
    function tryJSON(v){ try{ return JSON.parse(v); }catch{ return v; } }

    const FALLBACK_CRITICAL_KEYS = new Set([
      'att_employees_v2',
      'att_projects_v1',
      'att_schedules_v2',
      'att_schedules_default',
      'att_records_v2',
      'payroll_loan_tracker',
      'payroll_loan_sss',
      'payroll_loan_pagibig',
      'payroll_vale',
      'payroll_vale_wed',
      'payroll_contrib_flags',
      'payroll_lock_state',
      'payroll_rates',
      'payroll_hist',
      'payroll_other_deductions_details',
      'payroll_other_deductions_total',
      'payroll_additional_income_details',
      'payroll_additional_income_total',
    ]);

    function getCriticalKeySet(){
      const shared = window.__PAYROLL_CRITICAL_KEYS;
      if (shared && typeof shared.has === 'function') return shared;
      return FALLBACK_CRITICAL_KEYS;
    }


    async function fetchKVAll(){
      const supa = getSupa();
      if(!supa) return { rows:[], error:'No Supabase client' };
      try{
        const { data, error } = await supa.from(KV_TABLE).select('key,value');
        return { rows: data||[], error: error? error.message : null };
      }catch(e){ return { rows:[], error: String(e) } }
    }
    async function fetchDTR(){
      const supa = getSupa();
      if(!supa) return { rows:[], error:'No Supabase client' };
      // Prefer best-practice row-per-punch table, fallback to legacy single-row blob.
      async function fetchPunches(){
        const { data, error } = await supa.from(DTR_TABLE).select('id,data').range(0, 9999);
        if(error) throw error;
        if(Array.isArray(data) && data.length){
          // If this is the legacy single-row shape mistakenly stored here:
          if(data.length === 1 && data[0] && data[0].id === 'records'){
            const payload = data[0].data;
            if(Array.isArray(payload)) return payload;
            if(payload && typeof payload === 'object' && Array.isArray(payload.records)) return payload.records;
          }
          // Row-per-punch: each row's `data` is the punch object
          const recs = data.map(r=>r && r.data).filter(Boolean);
          return recs;
        }
        return [];
      }
      async function fetchLegacy(){
        const { data, error } = await supa.from(LEGACY_DTR_TABLE).select('data').eq('id','records').maybeSingle();
        if(error) throw error;
        if(!data) return [];
        const payload = data.data;
        if(Array.isArray(payload)) return payload;
        if(payload && typeof payload === 'object' && Array.isArray(payload.records)) return payload.records;
        return [];
      }
      try{
        const recs = await fetchPunches();
        return { rows: recs, error: null };
      }catch(e){
        const msg = (e && (e.message || e.details)) ? String(e.message || e.details) : String(e);
        // If punches table is missing, try legacy
        try{
          const recs = await fetchLegacy();
          return { rows: recs, error: null };
        }catch(e2){
          return { rows:[], error: msg };
        }
      }
    }


    function snapshotLocalStorage(){
      const kv = {};
      try{
        for(let i=0;i<localStorage.length;i++){
          const k = localStorage.key(i);
          if(!k || k.startsWith('__') || k.startsWith('vscode')) continue;
          kv[k] = tryJSON(localStorage.getItem(k));
        }
      }catch(e){}
      return kv;
    }

    async function buildBundle(){
      clearLog(); setStatus('Building backup...', false);
      const ls = snapshotLocalStorage();
      log('Collected localStorage keys: ' + Object.keys(ls).length);
      const kvRes = await fetchKVAll(); if(kvRes.error) log('KV fetch warning: ' + kvRes.error); else log('KV rows: ' + kvRes.rows.length);
      const dtrRes = await fetchDTR(); if(dtrRes.error) log('DTR fetch warning: ' + dtrRes.error); else log('DTR rows: ' + dtrRes.rows.length);
      const bundle = {
        schema: 'payrollhub.full.v1',
        created_at: new Date().toISOString(),
        localStorage: ls,
        kv: kvRes.rows||[],
        dtr: dtrRes.rows||[]
      };
      return bundle;
    }

    async function uploadBundleToCloud(bundle){
      const supa = getSupa();
      if(!supa || !supa.storage){ log('No Supabase storage client (skipping cloud upload)'); return null; }
      try{
        const json = JSON.stringify(bundle);
        const name = 'full_backup_' + new Date().toISOString().replace(/[:.]/g,'_') + '.json';
        const blob = new Blob([json], { type: 'application/json' });
        const { error } = await supa.storage.from(BUCKET).upload(name, blob, { upsert: true, contentType: 'application/json' });
        if(error){ log('Upload failed: ' + error.message); return null; }
        log('Uploaded to storage: ' + name);
        return name;
      }catch(e){ log('Upload failed: ' + String(e)); return null; }
    }

    // Cloud listing removed

    async function applyLocalStorage(ls){
      const criticalKeys = getCriticalKeySet();
      try {
        for (const k of Object.keys(ls || {})) {
          const value = ls[k];
          if (criticalKeys.has(k)) {
            try {
              if (typeof window.writeKV === 'function') {
                await window.writeKV(k, value);
              } else if (typeof window.sharedSet === 'function') {
                await window.sharedSet(k, value);
              } else {
                log('Skipped critical key restore without cloud writer: ' + k);
              }
            } catch (_) {
              log('Failed critical key restore via cloud writer: ' + k);
            }
            continue;
          }

          try { localStorage.setItem(k, JSON.stringify(value)); } catch (_) {}
        }
      } catch (e) {}
    }
    async function applyKV(kv){
      const supa = getSupa();
      if(!supa) return;
      try{
        for(const row of (kv||[])){
          if(!row || !row.key) continue;
          try{ await supa.from(KV_TABLE).upsert({ key: row.key, value: row.value }, { onConflict: 'key' }); }catch(_){}
        }
      }catch(e){ log('KV upsert error: ' + String(e)); }
    }
    async function applyDTR(rows){
      const supa = getSupa();
      if(!supa) return;
      try{
        await supa.from(DTR_TABLE).upsert({ id:'records', data: Array.isArray(rows)?rows:[] }, { onConflict:'id' });
        window.storedRecords = Array.isArray(rows)?rows:[];
        try{ if (typeof window.writeKV === 'function') window.writeKV('att_records_v2', window.storedRecords); else if (window.sharedSet) window.sharedSet('att_records_v2', window.storedRecords); }catch(_){ }
      }catch(e){ log('DTR upsert error: ' + String(e)); }
    }

    btnBackup.addEventListener('click', async ()=>{
      btnBackup.disabled = true; setStatus('Building backup...', false); clearLog();
      try{
        const bundle = await buildBundle();
        // Download locally
        try{
          const url = URL.createObjectURL(new Blob([JSON.stringify(bundle)],{type:'application/json'}));
          const a=document.createElement('a'); a.href=url; a.download='payroll_full_backup_'+ new Date().toISOString().slice(0,10)+'.json'; a.click(); setTimeout(()=>URL.revokeObjectURL(url), 1500);
          log('Downloaded local backup');
        }catch(e){ log('Local download failed: ' + String(e)); }
        // Upload to cloud
        await uploadBundleToCloud(bundle);
        setStatus('Backup complete', false);
      }catch(e){ setStatus('Backup error: ' + String(e), true); }
      finally{ btnBackup.disabled = false; }
    });

    btnRestoreFile.addEventListener('click', ()=> inputRestore.click());
    inputRestore.addEventListener('change', async (ev)=>{
      const f = ev.target.files && ev.target.files[0]; ev.target.value=''; if(!f) return;
      setStatus('Restoring from file...', false); clearLog();
      try{
        const text = await f.text(); const bundle = JSON.parse(text||'{}');
        if(!bundle || bundle.schema !== 'payrollhub.full.v1') throw new Error('Invalid bundle schema');
        await applyLocalStorage(bundle.localStorage || {}); log('Applied localStorage (critical keys routed to cloud writers)');
        await applyKV(bundle.kv || []); log('Applied KV table');
        await applyDTR(bundle.dtr || []); log('Applied DTR rows');
        setStatus('Restore complete', false); alert('Restore complete.');
      }catch(e){ setStatus('Restore failed: ' + String(e), true); alert('Restore failed: ' + String(e)); }
    });

    // Cloud restore removed
  })();
  // === / Dashboard Full Backup & Restore ===
  
