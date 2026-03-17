import { payrollService } from './services/payrollService.js';
import { getState, setCurrentPeriod, setSupabaseConnected, subscribe } from './state/store.js';
import { startRealtimeSubscriptions } from './realtime/subscriptions.js';
import { mountPayrollController } from './ui/payrollController.js';
import { waitForSupabaseClient } from './config/supabaseClient.js';

let cleanupUi = null;
let cleanupRealtime = null;
let bootstrapped = false;

const CRITICAL_LOCAL_KEYS = new Set([
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

function deprecateCriticalLocalAuthority() {
  if (window.__payrollLocalAuthorityDeprecated) return;
  window.__payrollLocalAuthorityDeprecated = true;

  const warnedKeys = new Set();
  const debugDeprecation = !!window.__PAYROLL_DEBUG_DEPRECATION;
  const requestedStrictDeprecation = !!window.__PAYROLL_STRICT_LOCAL_DEPRECATION;
  const isLocalDev = ['localhost', '127.0.0.1'].includes(window.location?.hostname || '');
  const strictDeprecation = requestedStrictDeprecation && isLocalDev;
  if (requestedStrictDeprecation && !isLocalDev) {
    console.warn('[payroll:deprecation] strict local authority blocking is ignored outside local dev hosts');
  }
  const warnOnce = (kind, key) => {
    if (!debugDeprecation) return;
    const marker = `${kind}:${key}`;
    if (warnedKeys.has(marker)) return;
    warnedKeys.add(marker);
    console.warn(`[payroll:deprecation] ${kind} for critical key`, key);
  };


  const originalReadKv = window.readKV;
  if (typeof originalReadKv === 'function') {
    window.readKV = async (key, fallback) => {
      if (CRITICAL_LOCAL_KEYS.has(key)) {
        warnOnce('KV read observed (legacy fallback candidate)', key);
      }
      return originalReadKv(key, fallback);
    };
  }

  const originalWriteKv = window.writeKV;
  if (typeof originalWriteKv === 'function') {
    window.writeKV = async (key, value) => {
      if (CRITICAL_LOCAL_KEYS.has(key)) {
        warnOnce('KV write observed (allowed for compatibility)', key);
        if (strictDeprecation) {
          throw new Error(`[payroll:deprecation] blocked critical KV write for ${key}`);
        }
      }
      return originalWriteKv(key, value);
    };
  }

  try {

    const originalGetItem = window.localStorage.getItem.bind(window.localStorage);
    window.localStorage.getItem = (key) => {
      if (CRITICAL_LOCAL_KEYS.has(key)) {
        warnOnce('localStorage read observed (legacy fallback candidate)', key);
      }
      return originalGetItem(key);
    };

    const originalSetItem = window.localStorage.setItem.bind(window.localStorage);
    window.localStorage.setItem = (key, value) => {
      if (CRITICAL_LOCAL_KEYS.has(key)) {
        warnOnce('localStorage write observed', key);
        if (strictDeprecation) {
          throw new Error(`[payroll:deprecation] blocked critical localStorage write for ${key}`);
        }
      }
      return originalSetItem(key, value);
    };
  } catch (error) {
    console.warn('localStorage interception failed', error);
  }
}

function mapToObject(mapLike) {
  const out = {};
  if (!(mapLike instanceof Map)) return out;
  mapLike.forEach((value, key) => {
    if (key == null) return;
    out[key] = { ...value };
  });
  return out;
}

function bridgeMigratedMasterDataToLegacyGlobals() {
  if (window.__payrollMasterDataBridgeReady) return;
  window.__payrollMasterDataBridgeReady = true;

  const apply = () => {
    try {
      const state = getState();
      const snapshot = {
        employees: mapToObject(state.employees),
        projects: mapToObject(state.projects),
        schedules: mapToObject(state.schedules),
      };
      if (typeof window.applyMasterDataSnapshotFromStore === 'function') {
        window.applyMasterDataSnapshotFromStore(snapshot);
      } else {
        window.storedEmployees = snapshot.employees;
        window.storedProjects = snapshot.projects;
        window.storedSchedules = snapshot.schedules;
      }
    } catch (error) {
      console.warn('[payroll:bridge] failed to sync master data globals', error);
    }
  };

  if (getState().employees.size || getState().projects.size || getState().schedules.size) {
    apply();
  }

  subscribe((_state, change) => {
    const isMasterDataChange = change?.tableKey === 'employees'
      || change?.tableKey === 'projects'
      || change?.tableKey === 'schedules'
      || (change?.type === 'batch'
        && Array.isArray(change?.changes)
        && change.changes.some((entry) => ['employees', 'projects', 'schedules'].includes(entry?.tableKey)));
    if (!isMasterDataChange) return;
    apply();
  });
}

function toLegacyDtrRecord(row) {
  if (!row) return null;
  const employeeId = row.employee_id ?? row.emp_id ?? row.empId;
  const stamp = row.punch_at || `${row.date || ''} ${row.time || ''}`;
  const str = String(stamp || '').trim();
  const match = str.match(/^(\d{4}-\d{2}-\d{2})[T\s](\d{2}:\d{2})/);
  if (!employeeId || !match) return null;
  return {
    id: row.id,
    empId: String(employeeId),
    date: match[1],
    time: match[2],
    source: row.meta?.source || row.source || null,
    manual: row.meta?.manual === true || row.manual === true,
  };
}

function bridgeDtrPunchesToLegacyRuntime() {
  if (window.__payrollDtrBridgeReady) return;
  window.__payrollDtrBridgeReady = true;

  const apply = () => {
    try {
      const rows = [];
      getState().dtrPunches.forEach((row) => {
        const normalized = toLegacyDtrRecord(row);
        if (normalized) rows.push(normalized);
      });
      rows.sort((a, b) => String(a.date).localeCompare(String(b.date)) || String(a.time).localeCompare(String(b.time)) || String(a.empId).localeCompare(String(b.empId)));

      if (typeof window.applyDtrRecordsSnapshotFromStore === 'function') {
        window.applyDtrRecordsSnapshotFromStore(rows);
      } else {
        window.storedRecords = rows;
      }
    } catch (error) {
      console.warn('[payroll:bridge] failed to sync dtr punch snapshot', error);
    }
  };

  if (getState().dtrPunches.size) apply();

  subscribe((_state, change) => {
    const isDtrChange = change?.tableKey === 'dtrPunches' || (change?.type === 'batch' && Array.isArray(change?.changes) && change.changes.some((entry) => entry?.tableKey === 'dtrPunches'));
    if (!isDtrChange) return;
    apply();
  });
}




async function bootstrapPayrollApp() {
  if (bootstrapped) return;
  bootstrapped = true;

  deprecateCriticalLocalAuthority();
  bridgeMigratedMasterDataToLegacyGlobals();
  bridgeDtrPunchesToLegacyRuntime();
  window.payrollService = payrollService;
  window.getPayrollStoreState = getState;

  const root = document.getElementById('panelPayroll') || document.body;
  cleanupUi = mountPayrollController(root);

  const supabase = await waitForSupabaseClient({ timeoutMs: 8000, intervalMs: 80 });
  if (!supabase) {
    setSupabaseConnected(false);
    console.error('Payroll bootstrap failed: Supabase client not available on window.supabase.');
    return;
  }

  try {
    const { error } = await supabase.from('payroll_periods').select('id').limit(1);
    setSupabaseConnected(!error);
    if (error) console.warn('Supabase connectivity check failed', error);
  } catch (error) {
    setSupabaseConnected(false);
    console.warn('Supabase connectivity check failed', error);
  }

  let currentPeriodId = null;

  try {
    const periods = await payrollService.loadPeriods();
    if (periods.length) {
      currentPeriodId = periods[0].id;
      setCurrentPeriod(currentPeriodId);
    }
  } catch (error) {
    console.error('Payroll periods load failed', error);
  }

  try {
    await payrollService.loadCoreReadModels({ periodId: currentPeriodId });
  } catch (error) {
    console.error('Payroll core read models load failed', error);
  }

  window.payrollDebugVerify = async (periodId = currentPeriodId) => {
    if (!periodId) {
      console.warn('[payroll:verify] no payroll period selected');
      return;
    }
    await payrollService.debugVerifyOptimizedLoad(periodId);
  };

  try {
    cleanupRealtime = startRealtimeSubscriptions();
  } catch (error) {
    console.error('Realtime bootstrap failed', error);
  }

  window.addEventListener('beforeunload', () => {
    if (typeof cleanupUi === 'function') cleanupUi();
    if (typeof cleanupRealtime === 'function') cleanupRealtime();
  });
}



try {
  deprecateCriticalLocalAuthority();
} catch (error) {
  console.warn('initial localStorage deprecation hook failed', error);
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', bootstrapPayrollApp);
} else {
  bootstrapPayrollApp().catch((error) => {
    console.error('Payroll bootstrap failed', error);
  });
}
