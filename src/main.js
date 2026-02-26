import { payrollService } from './services/payrollService.js';
import { setCurrentPeriod, setSupabaseConnected } from './state/store.js';
import { startRealtimeSubscriptions } from './realtime/subscriptions.js';
import { mountPayrollController } from './ui/payrollController.js';
import { waitForSupabaseClient } from './config/supabaseClient.js';

let cleanupUi = null;
let cleanupRealtime = null;
let bootstrapped = false;

const CRITICAL_LOCAL_KEYS = new Set([
  'att_employees_v2',
  'att_projects_v1',
  'att_records_v2',
  'payroll_loan_tracker',
  'payroll_loan_sss',
  'payroll_loan_pagibig',
  'payroll_vale',
  'payroll_vale_wed',
]);

function deprecateCriticalLocalAuthority() {
  if (window.__payrollLocalAuthorityDeprecated) return;
  window.__payrollLocalAuthorityDeprecated = true;

  const warnedKeys = new Set();
  const warnOnce = (kind, key) => {
    const marker = `${kind}:${key}`;
    if (warnedKeys.has(marker)) return;
    warnedKeys.add(marker);
    console.warn(`[payroll:deprecation] ${kind} for critical key`, key);
  };

  const originalWriteKv = window.writeKV;
  if (typeof originalWriteKv === 'function') {
    window.writeKV = async (key, value) => {
      if (CRITICAL_LOCAL_KEYS.has(key)) {
        warnOnce('KV write observed (allowed for compatibility)', key);
      }
      return originalWriteKv(key, value);
    };
  }

  try {
    const originalSetItem = window.localStorage.setItem.bind(window.localStorage);
    window.localStorage.setItem = (key, value) => {
      if (CRITICAL_LOCAL_KEYS.has(key)) {
        warnOnce('localStorage write observed', key);
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

  const root = document.getElementById('panelPayroll') || document.body;
  cleanupUi = mountPayrollController(root);

  const supabase = await waitForSupabaseClient({ timeoutMs: 8000, intervalMs: 80 });
  if (!supabase) {
    setSupabaseConnected(false);
    console.error('Payroll bootstrap failed: Supabase client not available on window.supabase.');
    return;
  }

  deprecateCriticalLocalAuthority();

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

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', bootstrapPayrollApp);
} else {
  bootstrapPayrollApp().catch((error) => {
    console.error('Payroll bootstrap failed', error);
  });
}
