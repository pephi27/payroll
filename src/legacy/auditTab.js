// === Audit tab (finalized periods): load + verify hash + view stored snapshot segments ===
(function(){
  function $(id){ return document.getElementById(id); }

  function setStatus(msg, isErr){
    const el = $('auditStatus');
    if (!el) return;
    el.textContent = msg || '';
    el.style.color = isErr ? '#b91c1c' : '#64748b';
  }

  function setInfo(html){
    const el = $('auditInfo');
    if (!el) return;
    el.innerHTML = html || '';
  }

  function resetAuditTabs(){
    const tabs = $('auditTabs');
    if (tabs) {
      tabs.innerHTML = '';
      tabs.style.display = 'none';
    }
  }

  function setActiveAuditSubtab(name){
    const cont = $('auditContainer');
    const tabs = $('auditTabs');
    if (!cont || !tabs) return;

    tabs.querySelectorAll('.tab-btn').forEach(btn=>{
      const active = btn.dataset.seg === name;
      btn.classList.toggle('active', active);
      btn.setAttribute('aria-selected', active ? 'true' : 'false');
    });

    cont.querySelectorAll('[data-audit-segment]').forEach(card=>{
      card.style.display = (card.dataset.auditSegment === name) ? '' : 'none';
    });
  }

  function renderAuditSubtabs(segNames){
    const tabs = $('auditTabs');
    if (!tabs) return;
    tabs.innerHTML = '';

    if (!Array.isArray(segNames) || !segNames.length) {
      tabs.style.display = 'none';
      return;
    }

    segNames.forEach((name, idx)=>{
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'tab-btn' + (idx === 0 ? ' active' : '');
      btn.dataset.seg = name;
      btn.setAttribute('aria-selected', idx === 0 ? 'true' : 'false');
      btn.textContent = name;
      btn.addEventListener('click', ()=>setActiveAuditSubtab(name));
      tabs.appendChild(btn);
    });

    tabs.style.display = 'flex';
  }

  function fmtStamp(iso){
    if (!iso) return '';
    try { return new Date(iso).toLocaleString(); } catch(e){ return String(iso); }
  }

  async function loadAuditIndex(){
    const sel = $('auditPeriodSelect');
    if (!sel) return [];
    sel.innerHTML = '';

    let idx = [];
    try {
      const revRows = (window.store && window.store.getJSON('payroll_period_snapshots_v1', [])) || [];
      idx = revRows.map(r => ({
        startDate: (r.period_id || '').split('|')[0] || '',
        endDate: (r.period_id || '').split('|')[1] || '',
        revision: r.revision || 1,
        status: r.status || 'FINALIZED',
        finalizedAt: r.created_at || '',
        hash: r.snapshot_hash || '',
        period_id: r.period_id || '',
        snapshot_data: r.snapshot_data || null
      }));
    } catch (e) {}

    if (!Array.isArray(idx) || !idx.length) {
      try { if (typeof window.readSnapshotIndex === 'function') idx = await window.readSnapshotIndex(); } catch (e) {}
    }
    if (!Array.isArray(idx)) idx = [];

    idx = idx.filter(it => it && (it.finalizedAt || it.lockedAt || it.created_at));

    idx.sort((a,b)=>{
      const as = (a.finalizedAt || a.lockedAt || a.createdAt || a.created_at || '');
      const bs = (b.finalizedAt || b.lockedAt || b.createdAt || b.created_at || '');
      return String(bs).localeCompare(String(as));
    });

    if (!idx.length){
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = 'No finalized snapshots found';
      sel.appendChild(opt);
      setStatus('No snapshots.');
      setInfo('');
      const pre = $('auditManifestJson'); if (pre) pre.textContent = '';
      const cont = $('auditContainer'); if (cont) cont.innerHTML = '';
      resetAuditTabs();
      return [];
    }

    idx.forEach(item=>{
      const s = item.startDate || '';
      const e = item.endDate || '';
      const opt = document.createElement('option');
      const rev = item.revision || 1;
      opt.value = [s,e,rev].join('|');
      const hash = item.auditHash || item.hash || '';
      const status = item.status || 'FINALIZED';
      opt.textContent = (s && e ? (s + ' to ' + e) : (s || e || 'Period')) + ` • Rev ${rev} • ${status}` + (hash ? (' • ' + String(hash).slice(0,10) + '…') : '');
      sel.appendChild(opt);
    });

    setStatus('Loaded ' + idx.length + ' snapshot(s).');
    return idx;
  }

  async function showSelectedManifest(){
    const sel = $('auditPeriodSelect');
    if (!sel) return;
    const val = sel.value || '';
    const parts = val.split('|');
    const startDate = parts[0] || '';
    const endDate = parts[1] || '';
    const revision = Number(parts[2] || '1');
    if (!startDate || !endDate) return;

    const pre  = $('auditManifestJson');
    const cont = $('auditContainer');
    if (cont) cont.innerHTML = '';
    resetAuditTabs();

    let manifest = null;
    try { if (typeof window.readSnapshotSegment === 'function') manifest = await window.readSnapshotSegment('manifest', startDate, endDate); } catch(e){}

    if (!manifest) {
      let idx = [];
      try { idx = await window.readSnapshotIndex(); } catch(e){}
      const entry = (Array.isArray(idx) ? idx : []).find(x => x && x.startDate === startDate && x.endDate === endDate && (!revision || Number(x.revision||1)===revision)) || {};
      manifest = {
        version: entry.version || 0,
        startDate, endDate,
        finalizedAt: entry.finalizedAt || entry.lockedAt || '',
        rootHash: entry.auditHash || entry.hash || '',
        note: 'Manifest not found for this period. (Older snapshot?)',
        auditHash: entry.auditHash || '',
        manifestHash: entry.manifestHash || '',
        segmentCount: entry.segmentCount || 0
      };
    }

    const hash = manifest.rootHash || manifest.auditHash || '';
    const when = manifest.finalizedAt ? fmtStamp(manifest.finalizedAt) : '';
    setInfo(
      '<div><b>Period:</b> ' + startDate + ' to ' + endDate + '</div>' +
      (when ? ('<div><b>Finalized:</b> ' + when + '</div>') : '') +
      (hash ? ('<div><b>Root Hash:</b> <code style="font-size:12px;">' + hash + '</code></div>') : '') +
      (manifest.segmentCount ? ('<div><b>Segments:</b> ' + manifest.segmentCount + '</div>') : '') +
      (manifest.note ? ('<div style="margin-top:6px;color:#64748b;">' + String(manifest.note) + '</div>') : '')
    );

    if (pre) pre.textContent = JSON.stringify(manifest, null, 2);
  }

  async function auditVerify(){
    const sel = $('auditPeriodSelect');
    if (!sel) return;
    const val = sel.value || '';
    const parts = val.split('|');
    const startDate = parts[0] || '';
    const endDate = parts[1] || '';
    const revision = Number(parts[2] || '1');
    if (!startDate || !endDate) return;

    setStatus('Verifying…');
    try {
      const rows = (window.store && window.store.getJSON('payroll_period_snapshots_v1', [])) || [];
      const periodId = `${startDate}|${endDate}`;
      const row = rows.find(r => r && r.period_id === periodId && Number(r.revision||1) === revision);
      if (row && row.snapshot_data) {
        const computed = await (window.computeSnapshotHash ? window.computeSnapshotHash(row.snapshot_data) : Promise.resolve(''));
        if (computed && computed === row.snapshot_hash) setStatus('✅ Verified');
        else setStatus('❌ Mismatch', true);
      } else if (typeof window.verifyAuditManifestForPeriod === 'function') {
        const res = await window.verifyAuditManifestForPeriod(startDate, endDate);
        if (res && res.ok) setStatus('✅ Hash OK (matches manifest)');
        else setStatus('❌ Hash FAILED: ' + ((res && res.reason) ? res.reason : 'Mismatch'), true);
      } else {
        setStatus('No verifiable snapshot found', true);
      }
      await showSelectedManifest();
    } catch (e) {
      setStatus('Verify error: ' + (e && e.message ? e.message : String(e)), true);
    }
  }

  function renderSegmentCard(name, value){
    const card = document.createElement('div');
    card.className = 'audit-segment-card';
    card.dataset.auditSegment = name;
    card.style.border = '1px solid #e2e8f0';
    card.style.borderRadius = '12px';
    card.style.padding = '10px';
    card.style.background = '#fff';

    const title = document.createElement('div');
    title.className = 'audit-segment-title';
    title.style.fontWeight = '700';
    title.style.marginBottom = '6px';
    title.textContent = name;
    card.appendChild(title);

    if (value && typeof value === 'object' && Array.isArray(value.headers) && Array.isArray(value.rows)) {
      const html = (typeof window.buildTableHtml === 'function')
        ? window.buildTableHtml(value.headers, value.rows, value.footerRow || [])
        : '';
      const wrap = document.createElement('div');
      wrap.className = 'audit-segment-wrap';
      wrap.style.overflow = 'auto';
      wrap.innerHTML = html || '<div class="audit-table-unavailable" style="color:#64748b;font-size:12px;">(Table renderer unavailable)</div>';
      card.appendChild(wrap);
      return card;
    }

    if (value && typeof value === 'object' && typeof value.html === 'string') {
      const det = document.createElement('details');
      det.open = false;
      const sum = document.createElement('summary');
      sum.textContent = 'View HTML';
      sum.style.cursor = 'pointer';
      det.appendChild(sum);
      const wrap = document.createElement('div');
      wrap.className = 'audit-segment-wrap';
      wrap.style.overflow = 'auto';
      wrap.innerHTML = value.html;
      det.appendChild(wrap);
      card.appendChild(det);
      return card;
    }

    const pre = document.createElement('pre');
    pre.className = 'audit-raw-pre';
    pre.style.whiteSpace = 'pre-wrap';
    pre.style.margin = '0';
    pre.style.fontSize = '12px';
    pre.textContent = (typeof value === 'string') ? value : JSON.stringify(value, null, 2);
    card.appendChild(pre);
    return card;
  }

  async function auditLoadSnapshot(){
    const sel = $('auditPeriodSelect');
    const cont = $('auditContainer');
    if (!sel || !cont) return;
    const val = sel.value || '';
    const parts = val.split('|');
    const startDate = parts[0] || '';
    const endDate = parts[1] || '';
    const revision = Number(parts[2] || '1');
    if (!startDate || !endDate) return;

    if (typeof window.readSnapshotSegment !== 'function') {
      setStatus('readSnapshotSegment not available', true);
      return;
    }

    setStatus('Loading snapshot…');
    cont.innerHTML = '';

    let manifest = null;
    try { manifest = await window.readSnapshotSegment('manifest', startDate, endDate); } catch(e){}
    const segNames = manifest && manifest.segmentHashes ? Object.keys(manifest.segmentHashes) : ['dtr','payroll','overtime','deductions','additionalIncome','otherDeductions','adjustments','reports','master'];
    const rendered = [];

    for (const name of segNames) {
      try {
        const seg = await window.readSnapshotSegment(name, startDate, endDate);
        if (seg == null) continue;
        cont.appendChild(renderSegmentCard(name, seg));
        rendered.push(name);
      } catch (e) {
        cont.appendChild(renderSegmentCard(name, { error: (e && e.message) ? e.message : String(e) }));
        rendered.push(name);
      }
    }

    resetAuditTabs();
    cont.querySelectorAll('[data-audit-segment]').forEach(card=>{ card.style.display = ''; });
    setStatus(rendered.length ? ('Loaded ' + rendered.length + ' segment(s).') : 'No stored segments found.');
  }

  window.renderAuditPanel = async function(){
    await loadAuditIndex();
    await showSelectedManifest();

    const sel = $('auditPeriodSelect');
    if (sel && !sel.__auditWired) {
      sel.addEventListener('change', showSelectedManifest);
      sel.__auditWired = true;
    }
    const b1 = $('auditRefreshBtn');
    const b2 = $('auditVerifyBtn');
    const b3 = $('auditLoadBtn');
    if (b1 && !b1.__auditWired) { b1.addEventListener('click', async()=>{ await loadAuditIndex(); await showSelectedManifest(); }); b1.__auditWired = true; }
    if (b2 && !b2.__auditWired) { b2.addEventListener('click', auditVerify); b2.__auditWired = true; }
    if (b3 && !b3.__auditWired) { b3.addEventListener('click', auditLoadSnapshot); b3.__auditWired = true; }
  };
})();
