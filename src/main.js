import { payrollService } from './services/payrollService.js';
import { batch, getState, mergeRow, resetTable, setCurrentPeriod, setPeriodSwitchInFlight, setSupabaseConnected, subscribe } from './state/store.js';
import { startRealtimeSubscriptions } from './realtime/subscriptions.js';
import { mountPayrollController } from './ui/payrollController.js';
import { waitForSupabaseClient } from './config/supabaseClient.js';

let cleanupUi = null;
let cleanupRealtime = null;
let cleanupPeriodSync = null;
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

function bridgeDtrApprovalsToLegacyRuntime() {
  if (window.__payrollDtrApprovalsBridgeReady) return;
  window.__payrollDtrApprovalsBridgeReady = true;

  // Lock/approval gates for legacy row actions still render from index.html. Force a render
  // whenever approvals (row-level) or payroll periods (period lock state) change so buttons stay accurate.
  const scheduleRefresh = () => {
    try { window.scheduleRenderResults?.('dtr-approvals-store-bridge'); } catch (_) {}
  };

  const hasTableChange = (change, tableKey) => (
    change?.tableKey === tableKey
    || (change?.type === 'batch'
      && Array.isArray(change?.changes)
      && change.changes.some((entry) => entry?.tableKey === tableKey))
  );

  subscribe((_state, change) => {
    const isApprovalOrLockChange = hasTableChange(change, 'dtrApprovals') || hasTableChange(change, 'payrollPeriods');
    if (!isApprovalOrLockChange) return;
    scheduleRefresh();
  });
}

function bridgePeriodSwitchReadModels() {
  if (window.__payrollPeriodReadModelsBridgeReady) return;
  window.__payrollPeriodReadModelsBridgeReady = true;

  const normalizeDate = (value) => {
    const match = String(value || '').trim().match(/^(\d{4}-\d{2}-\d{2})/);
    return match ? match[1] : '';
  };

  const getPunchWorkDate = (row = {}) => {
    const fromDate = normalizeDate(row.date);
    if (fromDate) return fromDate;
    const fromStamp = String(row.punch_at || '').trim().match(/^(\d{4}-\d{2}-\d{2})[T\s]/);
    return fromStamp ? fromStamp[1] : '';
  };

  const isDateInsidePeriod = (period, date) => {
    const start = normalizeDate(period?.period_start);
    const end = normalizeDate(period?.period_end);
    const ymd = normalizeDate(date);
    if (!start || !end || !ymd) return false;
    return ymd >= start && ymd <= end;
  };

  const isPunchForPeriod = (row, periodId, period) => {
    if (!row || !periodId || !period) return false;
    const metaPeriodId = row?.data?.payroll_period_id == null ? '' : String(row.data.payroll_period_id).trim();
    const workDate = getPunchWorkDate(row);
    if (metaPeriodId) {
      if (metaPeriodId !== String(periodId)) return false;
      return isDateInsidePeriod(period, workDate);
    }
    return isDateInsidePeriod(period, workDate);
  };

  const isApprovalForPeriod = (row, periodId) => (
    !!row && String(row.payroll_period_id || '') === String(periodId || '')
  );

  let inflightToken = 0;
  cleanupPeriodSync = subscribe(async (_state, change) => {
    if (change?.type !== 'set_current_period') return;
    const periodId = getState().currentPeriodId;
    if (!periodId) return;
    const token = ++inflightToken;
    const stateVersionAtStart = Number(getState().diagnostics?.dtrStateVersion) || 0;
    try {
      let [punchRows, approvalRows] = await Promise.all([
        payrollService.fetchPunchesByPeriod(periodId),
        payrollService.fetchDtrApprovalsByPeriod(periodId),
      ]);
      if (token !== inflightToken) return;
      if (String(getState().currentPeriodId || '') !== String(periodId)) return;

      const stateVersionAfterFetch = Number(getState().diagnostics?.dtrStateVersion) || 0;
      if (stateVersionAfterFetch !== stateVersionAtStart) {
        [punchRows, approvalRows] = await Promise.all([
          payrollService.fetchPunchesByPeriod(periodId),
          payrollService.fetchDtrApprovalsByPeriod(periodId),
        ]);
        if (token !== inflightToken) return;
        if (String(getState().currentPeriodId || '') !== String(periodId)) return;
      }

      const state = getState();
      const activePeriod = state.payrollPeriods.get(periodId);
      if (!activePeriod) {
        console.warn('[payroll:bridge] active payroll period missing during period switch apply', { periodId });
        return;
      }

      const punchById = new Map();
      (Array.isArray(punchRows) ? punchRows : []).forEach((row) => {
        if (!row?.id) return;
        if (!isPunchForPeriod(row, periodId, activePeriod)) return;
        punchById.set(row.id, row);
      });

      const approvalsById = new Map();
      (Array.isArray(approvalRows) ? approvalRows : []).forEach((row) => {
        if (!row?.id) return;
        if (!isApprovalForPeriod(row, periodId)) return;
        approvalsById.set(row.id, row);
      });

      batch(() => {
        resetTable('dtrPunches');
        punchById.forEach((row) => mergeRow('dtrPunches', row));
        resetTable('dtrApprovals');
        approvalsById.forEach((row) => mergeRow('dtrApprovals', row));
      });

      try { window.scheduleRenderResults?.('dtr-period-switch'); } catch (error) {
        console.warn('[payroll:bridge] failed to schedule DTR render after period switch', error);
      }
    } catch (error) {
      if (token !== inflightToken) return;
      console.warn('[payroll:bridge] failed to refresh DTR read models on period switch', error);
    }
  });
}

function createPeriodSwitcher() {
  return async (nextPeriodId) => {
    const periodId = String(nextPeriodId || '').trim();
    if (!periodId) return;

    setPeriodSwitchInFlight(true);
    try {
      console.info('[payroll:dtr-debug] switching payroll period', { periodId });
      await payrollService.loadCoreReadModels({ periodId, resetPeriodTables: true });
      setCurrentPeriod(periodId);
      const punchCount = getState().dtrPunches.size;
      console.info('[payroll:dtr-debug] remote punches loaded for active period', { periodId, punchCount });
      try { window.scheduleRenderResults?.('payroll-period-switch'); } catch (_) {}
    } finally {
      setPeriodSwitchInFlight(false);
    }
  };
}

function wirePeriodSwitchUi(switchPayrollPeriod) {
  const syncValue = (el, value) => {
    if (!el) return;
    const normalized = value == null ? '' : String(value);
    if (el.value !== normalized) el.value = normalized;
  };

  const bind = (id) => {
    const el = document.getElementById(id);
    if (!el || el.dataset.payrollPeriodBound === 'true') return;
    el.dataset.payrollPeriodBound = 'true';
    syncValue(el, getState().currentPeriodId);
    el.addEventListener('change', async (event) => {
      const nextPeriodId = String(event?.target?.value || '').trim();
      if (!nextPeriodId || nextPeriodId === getState().currentPeriodId) return;
      try {
        await switchPayrollPeriod(nextPeriodId);
      } catch (error) {
        console.error('Payroll period switch failed', error);
        syncValue(el, getState().currentPeriodId);
      }
    });
  };

  subscribe((state, change) => {
    if (change?.type !== 'set_current_period' && change?.tableKey !== 'payrollPeriods') return;
    syncValue(document.getElementById('activePayrollSelect'), state.currentPeriodId);
    syncValue(document.getElementById('bpActivePayrollSelect'), state.currentPeriodId);
  });

  bind('activePayrollSelect');
  bind('bpActivePayrollSelect');
}


async function bootstrapPayrollApp() {
  if (bootstrapped) return;
  bootstrapped = true;

  deprecateCriticalLocalAuthority();
  bridgeMigratedMasterDataToLegacyGlobals();
  bridgeDtrPunchesToLegacyRuntime();
  bridgeDtrApprovalsToLegacyRuntime();
  window.payrollService = payrollService;
  window.getPayrollStoreState = getState;

  const switchPayrollPeriod = createPeriodSwitcher();
  window.switchPayrollPeriod = switchPayrollPeriod;

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
    }
  } catch (error) {
    console.error('Payroll periods load failed', error);
  }

  try {
    if (currentPeriodId) {
      await switchPayrollPeriod(currentPeriodId);
    }
  } catch (error) {
    console.error('Payroll core read models load failed', error);
  }

  bridgePeriodSwitchReadModels();
  wirePeriodSwitchUi(switchPayrollPeriod);

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
    if (typeof cleanupPeriodSync === 'function') cleanupPeriodSync();
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
