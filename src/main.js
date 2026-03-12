import { payrollService } from './services/payrollService.js';
import { setCurrentPeriod, setSupabaseConnected } from './state/store.js';
import { startRealtimeSubscriptions } from './realtime/subscriptions.js';
import { mountPayrollController } from './ui/payrollController.js';
import { waitForSupabaseClient } from './config/supabaseClient.js';

let cleanupUi = null;
let cleanupRealtime = null;
let bootstrapped = false;
let runtimeErrorListenersInstalled = false;

function installRuntimeErrorLogging() {
  if (runtimeErrorListenersInstalled) return;
  runtimeErrorListenersInstalled = true;

  window.addEventListener('error', (event) => {
    console.error('Payroll runtime error', {
      message: event?.message,
      source: event?.filename,
      line: event?.lineno,
      column: event?.colno,
      error: event?.error,
    });
  });

  window.addEventListener('unhandledrejection', (event) => {
    console.error('Payroll unhandled promise rejection', event?.reason);
  });
}

function installDevLocalStorageDetector() {
  if (window.__payrollLocalDetectorInstalled) return;
  window.__payrollLocalDetectorInstalled = true;

  const debugDeprecation = !!window.__PAYROLL_DEBUG_DEPRECATION;
  const strict = !!window.__PAYROLL_STRICT_LOCAL_DEPRECATION;
  if (!debugDeprecation && !strict) return;

  const CRITICAL_KEYS = new Set([
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

  const report = (kind, key) => {
    if (!CRITICAL_KEYS.has(key)) return;
    const msg = `[payroll:diagnostic] ${kind} critical key: ${key}`;
    if (debugDeprecation) console.warn(msg);
    if (strict) throw new Error(msg);
  };

  try {
    const originalGetItem = window.localStorage.getItem.bind(window.localStorage);
    const originalSetItem = window.localStorage.setItem.bind(window.localStorage);
    const originalRemoveItem = window.localStorage.removeItem.bind(window.localStorage);

    window.localStorage.getItem = (key) => {
      report('localStorage.getItem', key);
      return originalGetItem(key);
    };

    window.localStorage.setItem = (key, value) => {
      report('localStorage.setItem', key);
      return originalSetItem(key, value);
    };

    window.localStorage.removeItem = (key) => {
      report('localStorage.removeItem', key);
      return originalRemoveItem(key);
    };

    if (typeof window.readKV === 'function') {
      const originalReadKv = window.readKV;
      window.readKV = async (key, fallback) => {
        report('readKV', key);
        return originalReadKv(key, fallback);
      };
    }

    if (typeof window.writeKV === 'function') {
      const originalWriteKv = window.writeKV;
      window.writeKV = async (key, value) => {
        report('writeKV', key);
        return originalWriteKv(key, value);
      };
    }
  } catch (error) {
    console.warn('localStorage diagnostic hook failed', error);
  }
}

async function bootstrapPayrollApp() {
  if (bootstrapped) return;
  bootstrapped = true;

  installDevLocalStorageDetector();

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

function runBootstrap() {
  bootstrapPayrollApp().catch((error) => {
    console.error('Payroll bootstrap failed', error);
  });
}



try {
  installRuntimeErrorLogging();
} catch (error) {
  console.warn('runtime error listeners failed to install', error);
}

try {
  installDevLocalStorageDetector();
} catch (error) {
  console.warn('initial localStorage diagnostic hook failed', error);
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', runBootstrap);
} else {
  runBootstrap();
}
