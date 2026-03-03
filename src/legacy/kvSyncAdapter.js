import { getSupabaseClient } from "../config/supabaseClient.js";

if (window.__kvSyncAdapterLoaded) {
  // no-op guard against duplicate execution
} else {
  window.__kvSyncAdapterLoaded = true;

const TABLE = "kv_store";
const SHARED_KEYS = ["att_employees_v2","att_schedules_v2","att_schedules_default","att_projects_v1","att_records_v2","att_overrides_hours_v1","dtr_overrides_v1","payroll_rates","payroll_ot_multiplier","payroll_week_start","payroll_week_end","settings_payroll","payroll_deduction_divisor","payroll_sss_table","payroll_pagibig_table","payroll_philhealth_table","payroll_pagibig_rate","payroll_philhealth_rate","payroll_loan_sss","payroll_loan_pagibig","payroll_loan_tracker","payroll_vale","payroll_vale_wed","payroll_hist","payroll_other_deductions_details","payroll_other_deductions_total","payroll_additional_income_details","payroll_additional_income_total","payroll_adjustment_hours","payroll_bantay","payroll_bantay_proj","payroll_contrib_flags","payroll_lock_state","incomeTypeOptions","deductionTypeOptions","payroll_print_orientation"];
const SHARED_KEY_SET = new Set(SHARED_KEYS);
const PENDING_KEY = '__shared_pending_writes_v1';
const META_KEY = '__shared_meta_v1';
const DEVICE_KEY = '__device_id';

const supabase = getSupabaseClient();
window.SUPABASE_TABLE = TABLE;
window.SHARED_KEYS = SHARED_KEYS;
window.SHARED_KEY_SET = SHARED_KEY_SET;
window.__supabase_ready = true;
window.__sharedSyncState = window.__sharedSyncState || { hydrated:false, offline:false, lastSyncAt:0, conflict:false };
if (!window.__supabaseReadyDispatched) {
  window.__supabaseReadyDispatched = true;
  try { console.warn('[boot] supabase ready'); window.dispatchEvent(new Event('supabase-ready')); } catch (_) {}
}

const __origGetItem = window.localStorage.getItem.bind(window.localStorage);
const __origSetItem = window.localStorage.setItem.bind(window.localStorage);
const __origRemoveItem = window.localStorage.removeItem.bind(window.localStorage);

function cacheGet(key, fallback = null) {
  try {
    const raw = __origGetItem ? __origGetItem(key) : localStorage.getItem(key);
    if (raw == null) return fallback;
    try { return JSON.parse(raw); } catch (_) { return raw; }
  } catch (_) {
    return fallback;
  }
}
function cacheSet(key, value) {
  try {
    if (value === undefined) { __origRemoveItem(key); return; }
    __origSetItem(key, JSON.stringify(value));
  } catch (_) {}
}
window.cacheGet = cacheGet;
window.cacheSet = cacheSet;

function getDeviceId() {
  let id = localStorage.getItem(DEVICE_KEY) || '';
  if (!id) {
    id = (crypto?.randomUUID?.() || `dev_${Date.now()}_${Math.random().toString(36).slice(2)}`);
    __origSetItem(DEVICE_KEY, id);
  }
  return id;
}
function wrapForStore(value) {
  return { __data: value, __meta: { updatedAt: Date.now(), deviceId: getDeviceId() } };
}
function unwrapFromStore(value) {
  if (value && typeof value === 'object' && Object.prototype.hasOwnProperty.call(value, '__deleted') && value.__deleted) return undefined;
  if (value && typeof value === 'object' && Object.prototype.hasOwnProperty.call(value, '__data')) return value.__data;
  return value;
}
function metaFromStore(value) {
  if (value && typeof value === 'object' && value.__meta && typeof value.__meta === 'object') {
    return { updatedAt: Number(value.__meta.updatedAt || 0), deviceId: String(value.__meta.deviceId || '') };
  }
  return { updatedAt: 0, deviceId: '' };
}
function loadMetaMap(){ const m = cacheGet(META_KEY, {}); return (m && typeof m === 'object' && !Array.isArray(m)) ? m : {}; }
let metaMap = loadMetaMap();
function saveMetaMap(){ cacheSet(META_KEY, metaMap); }
function setMetaForKey(key, meta){ metaMap[key] = { updatedAt:Number(meta?.updatedAt||0), deviceId:String(meta?.deviceId||'') }; saveMetaMap(); }

async function kvReadCloud(key) {
  const { data, error } = await supabase.from(TABLE).select('value, updated_at').eq('key', key).maybeSingle();
  if (error) throw error;
  return data || null;
}
async function kvWriteCloud(key, wrapped) {
  const { error } = await supabase.from(TABLE).upsert({ key, value: wrapped }, { onConflict: 'key' });
  if (error) throw error;
}
async function kvDeleteCloud(key) {
  const { error } = await supabase.from(TABLE).delete().eq('key', key);
  if (error) throw error;
}

function getPending(){ const q = cacheGet(PENDING_KEY, {}); return (q && typeof q === 'object' && !Array.isArray(q)) ? q : {}; }
function setPending(v){ cacheSet(PENDING_KEY, v || {}); }

async function sharedGet(key, fallback = null) {
  if (!SHARED_KEY_SET.has(key)) return cacheGet(key, fallback);
  if (window.__sharedSyncState.hydrated) return cacheGet(key, fallback);
  try {
    const row = await kvReadCloud(key);
    if (!row || row.value === undefined) return cacheGet(key, fallback);
    const unwrapped = unwrapFromStore(row.value);
    cacheSet(key, unwrapped);
    setMetaForKey(key, metaFromStore(row.value));
    return unwrapped === undefined ? fallback : unwrapped;
  } catch (_) {
    return cacheGet(key, fallback);
  }
}

async function sharedSet(key, value) {
  if (!SHARED_KEY_SET.has(key)) { cacheSet(key, value); return true; }
  if (value === undefined) {
    const meta = { updatedAt: Date.now(), deviceId: getDeviceId() };
    const tomb = { __deleted: true, __meta: meta };
    cacheSet(key, undefined);
    setMetaForKey(key, meta);
    try {
      await kvWriteCloud(key, tomb);
      window.__sharedSyncState.offline = false;
      window.__sharedSyncState.lastSyncAt = Date.now();
      return true;
    } catch (_) {
      const pending = getPending();
      pending[key] = tomb;
      setPending(pending);
      window.__sharedSyncState.offline = true;
      return false;
    }
  }
  const wrapped = wrapForStore(value);
  cacheSet(key, value);
  setMetaForKey(key, wrapped.__meta);
  try {
    await kvWriteCloud(key, wrapped);
    window.__sharedSyncState.offline = false;
    window.__sharedSyncState.lastSyncAt = Date.now();
    return true;
  } catch (_) {
    const pending = getPending();
    pending[key] = wrapped;
    setPending(pending);
    window.__sharedSyncState.offline = true;
    return false;
  }
}

window.sharedGet = sharedGet;
window.sharedSet = sharedSet;
window.readKV = sharedGet;
window.writeKV = sharedSet;
window.writeKVBatch = async (pairs=[]) => {
  const out = await Promise.all((Array.isArray(pairs)?pairs:[]).map(p => sharedSet(p.key, p.value)));
  return out.every(Boolean);
};

let __rerenderTimer = null;
let __renderResultsTimer = null;
window.scheduleRenderResults = function scheduleRenderResults(reason = '', delay = 120){
  try { window.__lastRenderResultsReason = reason; } catch (_) {}
  try { if (__renderResultsTimer) clearTimeout(__renderResultsTimer); } catch (_) {}
  __renderResultsTimer = setTimeout(() => {
    __renderResultsTimer = null;
    try { if (typeof window.renderResults === 'function') window.renderResults(); } catch (_) {}
  }, Math.max(0, Number(delay) || 0));
};
function queueSharedRerender(){
  try { if (__rerenderTimer) clearTimeout(__rerenderTimer); } catch(_) {}
  __rerenderTimer = setTimeout(() => {
    __rerenderTimer = null;
    try { window.refreshCoreStoresFromStorage?.(); } catch(_) {}
    try { window.scheduleRenderResults?.('shared-realtime'); } catch(_) {}
    try { window.renderProjects?.(); } catch(_) {}
    try { window.renderEmployees?.(); } catch(_) {}
    try { window.renderTable?.(); } catch(_) {}
  }, 120);
}

async function flushPending() {
  const pending = getPending();
  const keys = Object.keys(pending);
  if (!keys.length) return;
  const remain = {};
  for (const key of keys) {
    const localWrapped = pending[key];
    try {
      const cloud = await kvReadCloud(key);
      const cloudMeta = metaFromStore(cloud && cloud.value);
      const localMeta = metaFromStore(localWrapped);
      if (Number(cloudMeta.updatedAt || 0) > Number(localMeta.updatedAt || 0)) {
        window.__sharedSyncState.conflict = true;
        const cloudUnwrapped = unwrapFromStore(cloud && cloud.value);
        cacheSet(key, cloudUnwrapped);
        setMetaForKey(key, cloudMeta);
        continue;
      }
      if (localWrapped && localWrapped.__deleted) {
        await kvWriteCloud(key, localWrapped);
        cacheSet(key, undefined);
        setMetaForKey(key, localMeta);
      } else {
        await kvWriteCloud(key, localWrapped);
        setMetaForKey(key, localMeta);
      }
      window.__sharedSyncState.lastSyncAt = Date.now();
      window.__sharedSyncState.offline = false;
    } catch (_) {
      remain[key] = localWrapped;
      window.__sharedSyncState.offline = true;
    }
  }
  setPending(remain);
}

async function sharedHydrateAll() {
  try {
    const { data, error } = await supabase.from(TABLE).select('key,value,updated_at').in('key', SHARED_KEYS);
    if (error) throw error;
    for (const row of (data || [])) {
      const key = row?.key;
      if (!key || !SHARED_KEY_SET.has(key)) continue;
      const m = metaFromStore(row.value);
      const unwrapped = unwrapFromStore(row.value);
      if (unwrapped === undefined) {
        cacheSet(key, undefined);
        delete metaMap[key];
        saveMetaMap();
      } else {
        cacheSet(key, unwrapped);
        setMetaForKey(key, m);
      }
    }
    await flushPending();
    window.__sharedSyncState.offline = false;
  } catch (e) {
    console.warn('sharedHydrateAll offline/failed:', e?.message || e);
  } finally {
    window.__sharedSyncState.hydrated = true;
    window.__sharedSyncState.lastSyncAt = Date.now();
    window.__kv_hydrated = true;
    window.__kv_cloud_ready = true;
    try { window.dispatchEvent(new Event('kv-hydrated')); window.dispatchEvent(new Event('kv-cloud-ready')); } catch (_) {}
  }
}
window.sharedHydrateAll = sharedHydrateAll;

const kvChannel = supabase.channel('kv_store_realtime')
  .on('postgres_changes', { event:'*', schema:'public', table: TABLE }, (payload) => {
    try {
      const eventType = payload?.eventType;
      const row = (eventType === 'DELETE') ? payload?.old : payload?.new;
      const key = row?.key;
      if (!key || !SHARED_KEY_SET.has(key)) return;

      if (eventType === 'DELETE') {
        cacheSet(key, undefined);
        delete metaMap[key];
        saveMetaMap();
        queueSharedRerender();
        return;
      }

      const incomingMeta = metaFromStore(row?.value);
      const localMeta = metaMap[key] || { updatedAt:0, deviceId:'' };
      if (Number(incomingMeta.updatedAt || 0) < Number(localMeta.updatedAt || 0)) return;

      const nextVal = unwrapFromStore(row.value);
      if (nextVal === undefined) {
        cacheSet(key, undefined);
        delete metaMap[key];
        saveMetaMap();
      } else {
        cacheSet(key, nextVal);
        setMetaForKey(key, incomingMeta);
      }
      window.__sharedSyncState.lastSyncAt = Date.now();
      queueSharedRerender();
    } catch (e) {
      console.warn('shared realtime apply failed', e);
    }
  });
kvChannel.subscribe();
window.__sharedKvChannel = kvChannel;

window.addEventListener('online', () => { flushPending().catch(() => {}); });

if (!window.__sharedStorageHooked) {
  window.__sharedStorageHooked = true;
  window.localStorage.setItem = function patchedSetItem(key, value) {
    if (SHARED_KEY_SET.has(key)) {
      let parsed = value;
      try { parsed = JSON.parse(String(value)); } catch (_) {}
      sharedSet(key, parsed);
      return;
    }
    return __origSetItem(key, value);
  };
  window.localStorage.removeItem = function patchedRemoveItem(key) {
    if (SHARED_KEY_SET.has(key)) {
      sharedSet(key, undefined);
      return;
    }
    return __origRemoveItem(key);
  };
}

}
