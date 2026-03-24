const initialState = {
  currentPeriodId: null,
  payrollPeriods: new Map(),
  payrollSnapshots: new Map(),
  dtrRecords: new Map(),
  dtrPunches: new Map(),
  employees: new Map(),
  projects: new Map(),
  schedules: new Map(),
  loans: new Map(),
  loanDeductions: new Map(),
  contribFlags: new Map(),
  profiles: new Map(),
  diagnostics: {
    supabaseConnected: null,
    realtimeStatus: 'idle',
    currentPeriodLocked: null,
    periodBootstrapPending: true,
    periodSwitchInFlight: false,
    periodSwitchError: null,
    lastRealtimeEvent: null,
    lastConflict: null,
    activeChannelCount: 0,
    activeRealtimeGroups: [],
    activeRealtimeTables: [],
    eventsByTable: {},
    degradedModeEnabled: false,
  },
};

const state = structuredClone(initialState);
const listeners = new Set();
let batchDepth = 0;
let pendingChanges = [];

export function getState() {
  return state;
}

export function subscribe(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function notify(change) {
  if (batchDepth > 0) {
    pendingChanges.push(change);
    return;
  }
  listeners.forEach((listener) => listener(state, change));
}

export function batch(fn) {
  batchDepth += 1;
  try {
    return fn();
  } finally {
    batchDepth -= 1;
    if (batchDepth === 0 && pendingChanges.length) {
      const changes = pendingChanges;
      pendingChanges = [];
      notify({ type: 'batch', changes });
    }
  }
}

export function setCurrentPeriod(periodId) {
  state.currentPeriodId = periodId;
  const period = periodId ? state.payrollPeriods.get(periodId) : null;
  state.diagnostics.currentPeriodLocked = period ? !!period.is_locked : null;
  notify({ type: 'set_current_period', periodId });
}

export function setPeriodBootstrapPending(pending) {
  state.diagnostics.periodBootstrapPending = !!pending;
  notify({ type: 'diagnostics_period_bootstrap_pending', pending: !!pending });
}

export function setPeriodSwitchInFlight(inFlight) {
  state.diagnostics.periodSwitchInFlight = !!inFlight;
  notify({ type: 'diagnostics_period_switch_in_flight', inFlight: !!inFlight });
}

export function clearPeriodSwitchError() {
  state.diagnostics.periodSwitchError = null;
  notify({ type: 'diagnostics_period_switch_error_clear' });
}

export function setPeriodSwitchError(message) {
  state.diagnostics.periodSwitchError = message ? String(message) : 'Unknown period switch error';
  notify({ type: 'diagnostics_period_switch_error', message: state.diagnostics.periodSwitchError });
}

export function mergeRow(tableKey, row, primaryKey = 'id') {
  if (!row || !row[primaryKey]) return;
  const collection = state[tableKey];
  if (!(collection instanceof Map)) {
    throw new Error(`Unknown table key: ${tableKey}`);
  }
  const prev = collection.get(row[primaryKey]) || {};
  collection.set(row[primaryKey], { ...prev, ...row });
  if (tableKey === 'payrollPeriods' && row.id === state.currentPeriodId) {
    state.diagnostics.currentPeriodLocked = !!row.is_locked;
  }
  notify({ type: 'merge_row', tableKey, row });
}

export function removeRow(tableKey, id) {
  const collection = state[tableKey];
  if (!(collection instanceof Map) || !id) return;
  collection.delete(id);
  notify({ type: 'remove_row', tableKey, id });
}

export function resetTable(tableKey) {
  const collection = state[tableKey];
  if (!(collection instanceof Map)) return;
  collection.clear();
  notify({ type: 'reset_table', tableKey });
}

export function setSupabaseConnected(connected) {
  state.diagnostics.supabaseConnected = !!connected;
  notify({ type: 'diagnostics_supabase_connected', connected: !!connected });
}

export function setRealtimeStatus(status) {
  state.diagnostics.realtimeStatus = status;
  notify({ type: 'diagnostics_realtime_status', status });
}

export function setLastRealtimeEvent(event) {
  state.diagnostics.lastRealtimeEvent = event;
  notify({ type: 'diagnostics_realtime_event', event });
}

export function reportConflict(conflict) {
  state.diagnostics.lastConflict = conflict;
  notify({ type: 'diagnostics_conflict', conflict });
}

export function clearConflict() {
  state.diagnostics.lastConflict = null;
  notify({ type: 'diagnostics_conflict_clear' });
}

export function setRealtimeDiagnostics(patch = {}) {
  state.diagnostics = { ...state.diagnostics, ...patch };
  notify({ type: 'diagnostics_realtime_details', patch });
}

export function incrementRealtimeTableEvent(table) {
  if (!table) return;
  const next = { ...(state.diagnostics.eventsByTable || {}) };
  next[table] = (next[table] || 0) + 1;
  state.diagnostics.eventsByTable = next;
  notify({ type: 'diagnostics_realtime_table_event', table, count: next[table] });
}
