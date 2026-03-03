import { getFeatureFlag } from '../config/featureFlags.js';
import { getSupabaseClient } from '../config/supabaseClient.js';
import { getState, mergeRow, removeRow, setLastRealtimeEvent, setRealtimeStatus } from '../state/store.js';

const TABLE_TO_STATE_KEY = {
  payroll_periods: 'payrollPeriods',
  payroll_period_snapshots: 'payrollSnapshots',
  pp_dtr_records: 'dtrRecords',
  dtr_punches: 'dtrPunches',
  pp_employees: 'employees',
  pp_projects: 'projects',
  pp_schedules: 'schedules',
  employee_loans: 'loans',
  loan_deductions: 'loanDeductions',
  pp_contrib_flags: 'contribFlags',
  profiles: 'profiles',
};

const PERIOD_SCOPED_TABLES = new Set([
  'payroll_period_snapshots',
  'pp_dtr_records',
  'dtr_punches',
  'employee_loans',
  'loan_deductions',
  'pp_contrib_flags',
]);
const DEBUG_REALTIME_FLAG = 'DEBUG_REALTIME';
const REALTIME_FLUSH_MS = 120;

const mutationQueue = [];
let flushTimer = null;

function logRealtimeDebug(message, details = undefined) {
  if (!getFeatureFlag(DEBUG_REALTIME_FLAG, false)) return;
  if (details === undefined) {
    console.info(`[payroll:realtime:debug] ${message}`);
    return;
  }
  console.info(`[payroll:realtime:debug] ${message}`, details);
}

function isEventForCurrentPeriod(table, payload) {
  if (!PERIOD_SCOPED_TABLES.has(table)) return true;

  const currentPeriodId = getState().currentPeriodId;
  if (!currentPeriodId) {
    return true;
  }

  const newPeriodId = payload?.new?.payroll_period_id;
  const oldPeriodId = payload?.old?.payroll_period_id;

  if (newPeriodId == null && oldPeriodId == null) {
    logRealtimeDebug('applied legacy row without payroll_period_id', { table, eventType: payload?.eventType });
    return true;
  }

  return newPeriodId === currentPeriodId || oldPeriodId === currentPeriodId;
}

function scheduleFlush() {
  if (flushTimer) return;
  flushTimer = window.setTimeout(() => {
    flushTimer = null;
    const pending = mutationQueue.splice(0, mutationQueue.length);
    for (const entry of pending) {
      if (entry.kind === 'delete') {
        removeRow(entry.stateKey, entry.id);
      } else {
        mergeRow(entry.stateKey, entry.row);
      }
    }
    if (pending.length > 1) {
      logRealtimeDebug('batched realtime mutations applied', { count: pending.length });
    }
  }, REALTIME_FLUSH_MS);
}

function enqueueMutation(entry) {
  mutationQueue.push(entry);
  scheduleFlush();
}

function handleChange(table, payload) {
  const event = {
    table,
    type: payload.eventType,
    timestamp: new Date().toISOString(),
  };
  setLastRealtimeEvent(event);
  console.info('[payroll:realtime:event]', event);

  const stateKey = TABLE_TO_STATE_KEY[table];
  if (!stateKey) return;

  if (!isEventForCurrentPeriod(table, payload)) {
    logRealtimeDebug('ignored event for non-active period', {
      table,
      eventType: payload?.eventType,
      currentPeriodId: getState().currentPeriodId,
      eventPeriodId: payload?.new?.payroll_period_id ?? payload?.old?.payroll_period_id ?? null,
    });
    return;
  }

  logRealtimeDebug('applied event for active period', {
    table,
    eventType: payload?.eventType,
    currentPeriodId: getState().currentPeriodId,
    eventPeriodId: payload?.new?.payroll_period_id ?? payload?.old?.payroll_period_id ?? null,
  });

  if (payload.eventType === 'DELETE') {
    enqueueMutation({ kind: 'delete', stateKey, id: payload.old?.id });
    return;
  }

  enqueueMutation({ kind: 'merge', stateKey, row: payload.new });
}

export function startRealtimeSubscriptions() {
  const supabase = getSupabaseClient();
  if (!supabase) {
    throw new Error('Supabase client is not ready for realtime subscriptions.');
  }

  setRealtimeStatus('connecting');

  const channels = Object.keys(TABLE_TO_STATE_KEY).map((table) => {
    return supabase
      .channel(`rt:${table}`)
      .on(
        'postgres_changes',
        { event: '*', schema: 'public', table },
        (payload) => handleChange(table, payload),
      )
      .subscribe((status) => {
        setRealtimeStatus(status);
      });
  });

  return () => {
    setRealtimeStatus('closed');
    if (flushTimer) {
      window.clearTimeout(flushTimer);
      flushTimer = null;
    }
    mutationQueue.splice(0, mutationQueue.length);
    channels.forEach((channel) => supabase.removeChannel(channel));
  };
}
