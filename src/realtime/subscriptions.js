import { getFeatureFlag } from '../config/featureFlags.js';
import { getSupabaseClient } from '../config/supabaseClient.js';
import { batch, getState, mergeRow, removeRow, setLastRealtimeEvent, setRealtimeStatus } from '../state/store.js';

const TABLE_TO_STATE_KEY = {
  payroll_periods: 'payrollPeriods',
  payroll_period_snapshots: 'payrollSnapshots',
  pp_dtr_records: 'dtrRecords',
  dtr_punches: 'dtrPunches',
  pp_dtr_approvals: 'dtrApprovals',
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
  'pp_dtr_approvals',
  'employee_loans',
  'loan_deductions',
  'pp_contrib_flags',
]);

const DEBUG_REALTIME_FLAG = 'DEBUG_REALTIME';
const REALTIME_FLUSH_MS = 120;

const mutationQueue = [];
let flushTimer = null;
let latestActiveEvent = null;

function logRealtimeDebug(message, details = undefined) {
  if (!getFeatureFlag(DEBUG_REALTIME_FLAG, false)) return;
  if (details === undefined) {
    console.info(`[payroll:realtime:debug] ${message}`);
    return;
  }
  console.info(`[payroll:realtime:debug] ${message}`, details);
}

function getEventPeriodInfo(payload) {
  const currentPeriodId = getState().currentPeriodId;
  const newPeriodId = payload?.new?.payroll_period_id ?? null;
  const oldPeriodId = payload?.old?.payroll_period_id ?? null;
  const hasPeriodId = newPeriodId != null || oldPeriodId != null;
  return { currentPeriodId, newPeriodId, oldPeriodId, hasPeriodId };
}

function isEventForCurrentPeriod(table, payload) {
  if (!PERIOD_SCOPED_TABLES.has(table)) return true;

  const info = getEventPeriodInfo(payload);
  if (!info.currentPeriodId) return true;

  if (!info.hasPeriodId) {
    logRealtimeDebug('applied legacy row without payroll_period_id', { table, eventType: payload?.eventType });
    return true;
  }

  return info.newPeriodId === info.currentPeriodId || info.oldPeriodId === info.currentPeriodId;
}

function isQueuedEntryStillRelevant(entry) {
  if (!PERIOD_SCOPED_TABLES.has(entry.table)) return true;

  const currentPeriodId = getState().currentPeriodId;
  if (!currentPeriodId) return true;
  if (entry.isLegacyPeriodRow) return true;

  return entry.newPeriodId === currentPeriodId || entry.oldPeriodId === currentPeriodId;
}

function getEntryDedupeKey(entry) {
  const id = entry.id ?? entry.row?.id;
  return id == null ? null : `${entry.stateKey}:${id}`;
}

function flushMutations() {
  flushTimer = null;
  const pending = mutationQueue.splice(0, mutationQueue.length);
  if (!pending.length) return;

  const deduped = [];
  const dedupeIndex = new Map();
  for (const entry of pending) {
    const key = getEntryDedupeKey(entry);
    if (!key) {
      deduped.push(entry);
      continue;
    }
    const existingIndex = dedupeIndex.get(key);
    if (existingIndex == null) {
      dedupeIndex.set(key, deduped.length);
      deduped.push(entry);
      continue;
    }
    deduped[existingIndex] = entry;
  }

  let applied = 0;
  let ignoredAfterDebounce = 0;

  batch(() => {
    for (const entry of deduped) {
      if (!isQueuedEntryStillRelevant(entry)) {
        ignoredAfterDebounce += 1;
        continue;
      }

      if (entry.kind === 'delete') {
        removeRow(entry.stateKey, entry.id);
      } else {
        mergeRow(entry.stateKey, entry.row);
      }
      applied += 1;
    }

    if (applied > 0 && latestActiveEvent) {
      setLastRealtimeEvent(latestActiveEvent);
    }
  });

  latestActiveEvent = null;

  if (deduped.length > 1 || ignoredAfterDebounce > 0) {
    logRealtimeDebug('batched realtime mutations flushed', {
      queued: pending.length,
      deduped: deduped.length,
      applied,
      ignoredAfterDebounce,
    });
  }
}

function scheduleFlush() {
  if (flushTimer) return;
  flushTimer = window.setTimeout(flushMutations, REALTIME_FLUSH_MS);
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
  const stateKey = TABLE_TO_STATE_KEY[table];
  if (!stateKey) return;

  if (!isEventForCurrentPeriod(table, payload)) {
    const periodInfo = getEventPeriodInfo(payload);
    logRealtimeDebug('ignored event for non-active period', {
      table,
      eventType: payload?.eventType,
      currentPeriodId: periodInfo.currentPeriodId,
      newPeriodId: periodInfo.newPeriodId,
      oldPeriodId: periodInfo.oldPeriodId,
    });
    return;
  }

  latestActiveEvent = event;

  const periodInfo = getEventPeriodInfo(payload);
  logRealtimeDebug('queued event for active period', {
    table,
    eventType: payload?.eventType,
    currentPeriodId: periodInfo.currentPeriodId,
    newPeriodId: periodInfo.newPeriodId,
    oldPeriodId: periodInfo.oldPeriodId,
  });

  const queueEntry = {
    table,
    stateKey,
    newPeriodId: periodInfo.newPeriodId,
    oldPeriodId: periodInfo.oldPeriodId,
    isLegacyPeriodRow: PERIOD_SCOPED_TABLES.has(table) && !periodInfo.hasPeriodId,
  };

  if (payload.eventType === 'DELETE') {
    enqueueMutation({ ...queueEntry, kind: 'delete', id: payload.old?.id });
    return;
  }

  enqueueMutation({ ...queueEntry, kind: 'merge', row: payload.new, id: payload.new?.id });
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
