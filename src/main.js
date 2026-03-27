import { payrollService } from './services/payrollService.js';
import {
  clearPeriodSwitchError,
  getState,
  mergeRow,
  resetTable,
  setCurrentPeriod,
  setPeriodBootstrapPending,
  setPeriodSwitchError,
  setPeriodSwitchInFlight,
  setSupabaseConnected,
} from './state/store.js';
import {
  destroyRealtimeManager,
  initRealtimeManager,
  isDtrLiveActive,
  resumeDtrLive,
  subscribeDtrLive,
  subscribeOptionalModule,
  subscribePayrollCore,
  suspendDtrLive,
  unsubscribeDtrLive,
  unsubscribeOptionalModule,
} from './realtime/subscriptions.js';
import { mountPayrollController } from './ui/payrollController.js';
import { waitForSupabaseClient } from './config/supabaseClient.js';

let cleanupUi = null;
let bootstrapped = false;
let periodSwitchQueue = Promise.resolve();

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


async function bootstrapPayrollApp() {
  if (bootstrapped) return;
  bootstrapped = true;

  deprecateCriticalLocalAuthority();

  setPeriodBootstrapPending(true);
  clearPeriodSwitchError();

  try {
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
        window.currentPeriodId = currentPeriodId;
      }
    } catch (error) {
      console.error('Payroll periods load failed', error);
    }

    try {
      await payrollService.loadCoreReadModels({ periodId: currentPeriodId });
    } catch (error) {
      console.error('Payroll core read models load failed', error);
    }

    window.payrollService = payrollService;

    const root = document.getElementById('panelPayroll') || document.body;
    cleanupUi = mountPayrollController(root);
    window.__PAYROLL_MODULAR_LOCK_ACTIVE = true;
    window.getPayrollPeriodLockState = (periodId = getState().currentPeriodId) => {
      if (!periodId) return null;
      const period = getState().payrollPeriods.get(periodId);
      return period && typeof period.is_locked === 'boolean' ? !!period.is_locked : null;
    };

    const PERIOD_SCOPED_TABLES = ['dtrRecords', 'dtrPunches', 'loans', 'loanDeductions', 'contribFlags'];
    const snapshotTables = (tableKeys) => {
      const state = getState();
      return tableKeys.reduce((acc, key) => {
        const source = state[key] instanceof Map ? state[key] : new Map();
        acc[key] = new Map(source);
        return acc;
      }, {});
    };
    const restoreTables = (snapshot, tableKeys) => {
      tableKeys.forEach((tableKey) => {
        resetTable(tableKey);
        const rows = snapshot?.[tableKey] || new Map();
        rows.forEach((row) => mergeRow(tableKey, row));
      });
    };

    const applyPeriodSwitch = async (nextPeriodId) => {
      if (!nextPeriodId) return;
      if (nextPeriodId === getState().currentPeriodId) return;
      const previousPeriodId = getState().currentPeriodId;
      const previousTables = snapshotTables(PERIOD_SCOPED_TABLES);
      setPeriodSwitchInFlight(true);
      clearPeriodSwitchError();
      try {
        PERIOD_SCOPED_TABLES.forEach((tableKey) => resetTable(tableKey));
        setCurrentPeriod(nextPeriodId);
        await payrollService.loadCoreReadModels({ periodId: nextPeriodId });
        subscribePayrollCore({ periodId: nextPeriodId });
        currentPeriodId = nextPeriodId;
        window.currentPeriodId = nextPeriodId;
      } catch (error) {
        console.error('Payroll period switch failed', error);
        setPeriodSwitchError(error?.message || 'Period switch failed');
        restoreTables(previousTables, PERIOD_SCOPED_TABLES);
        setCurrentPeriod(previousPeriodId);
        window.currentPeriodId = previousPeriodId || null;
        if (previousPeriodId) {
          subscribePayrollCore({ periodId: previousPeriodId });
        }
      } finally {
        setPeriodSwitchInFlight(false);
      }
    };

    const queuePeriodSwitch = (nextPeriodId) => {
      periodSwitchQueue = periodSwitchQueue
        .catch(() => undefined)
        .then(() => applyPeriodSwitch(nextPeriodId));
      return periodSwitchQueue;
    };

    const resolvePeriodIdFromRange = (start, end) => {
      if (!start || !end) return null;
      for (const period of getState().payrollPeriods.values()) {
        if (period?.period_start === start && period?.period_end === end) return period.id;
      }
      return null;
    };

    const resolvePeriodIdFromSelect = (selectEl) => {
      if (!selectEl) return null;
      const selected = selectEl.options?.[selectEl.selectedIndex] || null;
      const dataPeriodId = selected?.dataset?.periodId || '';
      if (dataPeriodId) return dataPeriodId;

      const rawValue = String(selected?.value ?? selectEl.value ?? '').trim();
      if (rawValue && getState().payrollPeriods.has(rawValue)) return rawValue;
      if (rawValue.includes('|')) {
        const [start, end] = rawValue.split('|');
        const fromRange = resolvePeriodIdFromRange(start, end);
        if (fromRange) return fromRange;
      }

      const weekStart = document.getElementById('weekStart')?.value || '';
      const weekEnd = document.getElementById('weekEnd')?.value || '';
      return resolvePeriodIdFromRange(weekStart, weekEnd);
    };

    const onPeriodPickerChange = (event) => {
      const selectedPeriodId = resolvePeriodIdFromSelect(event?.target);
      if (!selectedPeriodId || selectedPeriodId === currentPeriodId) return;
      void queuePeriodSwitch(selectedPeriodId);
    };

    document.getElementById('activePayrollSelect')?.addEventListener('change', onPeriodPickerChange);
    document.getElementById('bpActivePayrollSelect')?.addEventListener('change', onPeriodPickerChange);

    window.payrollDebugVerify = async (periodId = currentPeriodId) => {
      if (!periodId) {
        console.warn('[payroll:verify] no payroll period selected');
        return;
      }
      await payrollService.debugVerifyOptimizedLoad(periodId);
    };

    try {
      initRealtimeManager();
      // Keep legacy DTR subscriptions enabled for now because the current DTR
      // UI still renders from `window.storedRecords`. Disabling legacy
      // subscriptions prevents manual DTR updates from propagating across
      // devices in real time.
      window.__DISABLE_LEGACY_DTR_SUBSCRIPTIONS = false;
      if (window.__ENABLE_DTR_LIVE_REALTIME == null) window.__ENABLE_DTR_LIVE_REALTIME = false;
      subscribePayrollCore({ periodId: currentPeriodId });
      window.payrollRealtimeManager = {
        subscribePayrollCore,
        subscribeDtrLive,
        unsubscribeDtrLive,
        suspendDtrLive,
        resumeDtrLive,
        subscribeOptionalModule,
        unsubscribeOptionalModule,
        isDtrLiveActive,
      };
    } catch (error) {
      console.error('Realtime bootstrap failed', error);
    }

    window.addEventListener('beforeunload', () => {
      if (typeof cleanupUi === 'function') cleanupUi();
      destroyRealtimeManager();
    });
  } finally {
    setPeriodBootstrapPending(false);
  }
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
