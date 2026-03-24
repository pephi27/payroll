import { payrollService } from './services/payrollService.js';
import { setCurrentPeriod, setSupabaseConnected } from './state/store.js';
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
    initRealtimeManager();
    window.__DISABLE_LEGACY_DTR_SUBSCRIPTIONS = true;
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
