import { getFeatureFlag } from '../config/featureFlags.js';
import { getSupabaseClient } from '../config/supabaseClient.js';
import {
  batch,
  getState,
  incrementRealtimeTableEvent,
  mergeRow,
  removeRow,
  setLastRealtimeEvent,
  setRealtimeDiagnostics,
  setRealtimeStatus,
} from '../state/store.js';

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

const GROUP_TABLES = {
  payroll_core: ['payroll_periods', 'payroll_period_snapshots', 'pp_employees', 'pp_projects', 'pp_schedules'],
  dtr_live: ['dtr_punches', 'pp_dtr_records'],
};

const OPTIONAL_MODULE_TABLES = {
  loans: ['employee_loans', 'loan_deductions'],
  contrib_flags: ['pp_contrib_flags'],
  profiles: ['profiles'],
  dtr_records_legacy: ['pp_dtr_records'],
};

const PERIOD_SCOPED_TABLES = new Set([
  'payroll_period_snapshots',
  'pp_dtr_records',
  'dtr_punches',
  'employee_loans',
  'loan_deductions',
  'pp_contrib_flags',
]);

const DTR_LIVE_TABLES = new Set(GROUP_TABLES.dtr_live);
const DEBUG_REALTIME_FLAG = 'DEBUG_REALTIME';
const REALTIME_FLUSH_MS = 120;

const mutationQueue = [];
let flushTimer = null;
let latestActiveEvent = null;

let manager = null;

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

function refreshDiagnostics() {
  if (!manager) return;
  setRealtimeDiagnostics({
    activeChannelCount: manager.channelsByTable.size,
    activeRealtimeGroups: [...manager.activeGroups],
    activeRealtimeTables: [...manager.channelsByTable.keys()],
    degradedModeEnabled: !!manager.degradedModeEnabled,
  });
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

  if (manager?.dtrLiveSuspended && DTR_LIVE_TABLES.has(table)) return;

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
  incrementRealtimeTableEvent(table);

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

function subscribeTables(tables, groupName) {
  if (!manager?.supabase) return;

  for (const table of tables) {
    if (manager.channelsByTable.has(table)) continue;

    const channel = manager.supabase
      .channel(`rt:${groupName}:${table}`)
      .on('postgres_changes', { event: '*', schema: 'public', table }, (payload) => handleChange(table, payload))
      .subscribe((status) => {
        setRealtimeStatus(status);
      });

    manager.channelsByTable.set(table, channel);
    manager.groupTables.set(groupName, new Set([...(manager.groupTables.get(groupName) || new Set()), table]));
  }

  manager.activeGroups.add(groupName);
  refreshDiagnostics();
}

function unsubscribeGroup(groupName) {
  if (!manager?.supabase) return;
  const tables = manager.groupTables.get(groupName);
  if (!tables?.size) {
    manager.activeGroups.delete(groupName);
    refreshDiagnostics();
    return;
  }

  for (const table of tables) {
    const channel = manager.channelsByTable.get(table);
    if (channel) manager.supabase.removeChannel(channel);
    manager.channelsByTable.delete(table);
  }

  manager.groupTables.delete(groupName);
  manager.activeGroups.delete(groupName);
  if (!manager.channelsByTable.size) setRealtimeStatus('idle');
  refreshDiagnostics();
}

export function initRealtimeManager() {
  if (manager) return manager;

  const supabase = getSupabaseClient();
  if (!supabase) {
    setRealtimeStatus('degraded');
    setRealtimeDiagnostics({ degradedModeEnabled: true });
    manager = {
      supabase: null,
      channelsByTable: new Map(),
      groupTables: new Map(),
      activeGroups: new Set(),
      dtrLiveSuspended: false,
      dtrWasActiveBeforeSuspend: false,
      degradedModeEnabled: true,
    };
    return manager;
  }

  setRealtimeStatus('ready');
  manager = {
    supabase,
    channelsByTable: new Map(),
    groupTables: new Map(),
    activeGroups: new Set(),
    dtrLiveSuspended: false,
    dtrWasActiveBeforeSuspend: false,
    degradedModeEnabled: false,
  };
  refreshDiagnostics();
  return manager;
}

export function subscribePayrollCore({ periodId } = {}) {
  void periodId;
  if (!manager) initRealtimeManager();
  subscribeTables(GROUP_TABLES.payroll_core, 'payroll_core');
}

export function unsubscribePayrollCore() {
  if (!manager) return;
  unsubscribeGroup('payroll_core');
}

export function subscribeDtrLive({ periodId } = {}) {
  void periodId;
  if (!manager) initRealtimeManager();
  manager.dtrLiveSuspended = false;
  subscribeTables(GROUP_TABLES.dtr_live, 'dtr_live');
}

export function unsubscribeDtrLive() {
  if (!manager) return;
  manager.dtrWasActiveBeforeSuspend = false;
  manager.dtrLiveSuspended = false;
  unsubscribeGroup('dtr_live');
}

export function suspendDtrLive() {
  if (!manager) return;
  manager.dtrWasActiveBeforeSuspend = manager.activeGroups.has('dtr_live');
  manager.dtrLiveSuspended = true;
  if (manager.dtrWasActiveBeforeSuspend) {
    unsubscribeGroup('dtr_live');
  }
}

export function resumeDtrLive({ periodId } = {}) {
  void periodId;
  if (!manager) initRealtimeManager();
  if (!manager) return;
  const shouldResume = manager.dtrLiveSuspended && manager.dtrWasActiveBeforeSuspend;
  manager.dtrLiveSuspended = false;
  if (shouldResume) {
    subscribeTables(GROUP_TABLES.dtr_live, 'dtr_live');
  }
}

export function subscribeOptionalModule(moduleName) {
  if (!manager) initRealtimeManager();
  const tables = OPTIONAL_MODULE_TABLES[moduleName];
  if (!tables?.length) return;
  subscribeTables(tables, `optional:${moduleName}`);
}

export function unsubscribeOptionalModule(moduleName) {
  if (!manager) return;
  unsubscribeGroup(`optional:${moduleName}`);
}

export function isDtrLiveActive() {
  return !!manager?.activeGroups?.has('dtr_live');
}

export function destroyRealtimeManager() {
  if (!manager) return;
  if (flushTimer) {
    window.clearTimeout(flushTimer);
    flushTimer = null;
  }
  mutationQueue.splice(0, mutationQueue.length);

  for (const groupName of [...manager.groupTables.keys()]) {
    unsubscribeGroup(groupName);
  }

  setRealtimeStatus('closed');
  setRealtimeDiagnostics({
    activeChannelCount: 0,
    activeRealtimeGroups: [],
    activeRealtimeTables: [],
  });

  manager = null;
}
