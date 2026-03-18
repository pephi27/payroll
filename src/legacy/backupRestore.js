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

    async function fetchKVAll(){
      const supa = getSupa();
      if(!supa) return { rows:[], error:'No Supabase client' };
      try{
        const { data, error } = await supa.from(KV_TABLE).select('key,value');
        return { rows: data||[], error: error? error.message : null };
      }catch(e){ return { rows:[], error: String(e) } }
    }
    function canonicalizeBundlePunchRow(row){
      if(!row || typeof row !== 'object') return null;
      const meta = row.meta && typeof row.meta === 'object' ? { ...row.meta } : (row.data && typeof row.data === 'object' ? { ...row.data } : {});
      const employeeId = String(row.employee_id ?? row.emp_id ?? meta.employee_id ?? meta.empId ?? '').trim();
      const punchAt = String(row.punch_at || `${row.date || meta.date || ''}T${row.time || meta.time || ''}:00`).trim();
      const periodId = String(row.payroll_period_id ?? meta.payroll_period_id ?? '').trim();
      if(!employeeId || !punchAt) return null;
      return {
        id: row.id || null,
        payroll_period_id: periodId || null,
        employee_id: employeeId,
        project_id: row.project_id ?? meta.project_id ?? meta.projectId ?? null,
        punch_at: punchAt,
        meta: { ...meta, ...(periodId ? { payroll_period_id: periodId } : {}), empId: employeeId },
        updated_by: row.updated_by || null,
        updated_at: row.updated_at || null,
        created_at: row.created_at || null,
      };
    }

    async function fetchDTR(){
      const supa = getSupa();
      if(!supa) return { rows:[], error:'No Supabase client' };
      // Prefer authoritative row-per-punch table, fallback to legacy single-row blob only for reading old backups.
      async function fetchPunches(){
        const { data, error } = await supa.from(DTR_TABLE).select('id,payroll_period_id,employee_id,project_id,punch_at,meta,updated_by,updated_at,created_at').range(0, 9999);
        if(error) throw error;
        if(Array.isArray(data) && data.length){
          return data.map(canonicalizeBundlePunchRow).filter(Boolean);
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

    function applyLocalStorage(ls){
      try{ Object.keys(ls||{}).forEach(k=>{ try{ localStorage.setItem(k, JSON.stringify(ls[k])); }catch(_){} }); }catch(e){}
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
        const payload = Array.isArray(rows) ? rows : [];
        if(typeof window.saveDtrToCloud !== 'function') throw new Error('Authoritative DTR save helper is unavailable.');
        const result = await window.saveDtrToCloud(payload, { requirePunches: true, throwOnError: true });
        if(!result || result.status !== 'ok' || result.storage !== 'punches') throw new Error('Authoritative DTR restore did not complete in dtr_punches.');
        if(typeof window.loadDtrFromCloud === 'function') {
          const refreshed = await window.loadDtrFromCloud();
          if(refreshed && refreshed.status === 'ok' && Array.isArray(refreshed.records)) {
            window.storedRecords = refreshed.records;
          }
        } else {
          window.storedRecords = payload.filter(r => r && !r.employee_id).length ? payload : (payload.map(r => ({
            id: r.id || null,
            empId: String(r.employee_id || r.emp_id || r.meta?.empId || ''),
            date: String((r.punch_at || '').slice(0,10) || r.date || r.meta?.date || ''),
            time: String((r.punch_at || '').slice(11,16) || r.time || r.meta?.time || ''),
          })).filter(r => r.empId && r.date && r.time));
        }
        try{ if (window.sharedSet) window.sharedSet('att_records_v2', window.storedRecords); else localStorage.setItem('att_records_v2', JSON.stringify(window.storedRecords)); }catch(_){ }
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
        applyLocalStorage(bundle.localStorage || {}); log('Applied localStorage');
        await applyKV(bundle.kv || []); log('Applied KV table');
        await applyDTR(bundle.dtr || []); log('Applied DTR rows');
        setStatus('Restore complete', false); alert('Restore complete.');
      }catch(e){ setStatus('Restore failed: ' + String(e), true); alert('Restore failed: ' + String(e)); }
    });

    // Cloud restore removed
  })();
  // === / Dashboard Full Backup & Restore ===
  
