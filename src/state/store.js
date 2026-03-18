const initialState = {
  currentPeriodId: null,
  payrollPeriods: new Map(),
  payrollSnapshots: new Map(),
  dtrRecords: new Map(),
  dtrPunches: new Map(),
  dtrApprovals: new Map(),
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
    periodSwitchInFlight: false,
    periodSwitchError: '',
    lastRealtimeEvent: null,
    lastConflict: null,
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


export function setPeriodSwitchInFlight(inFlight) {
  state.diagnostics.periodSwitchInFlight = !!inFlight;
  notify({ type: 'diagnostics_period_switch', inFlight: !!inFlight });
}


export function setPeriodSwitchError(message) {
  state.diagnostics.periodSwitchError = String(message || '').trim();
  notify({ type: 'diagnostics_period_switch_error', message: state.diagnostics.periodSwitchError });
}

export function clearPeriodSwitchError() {
  setPeriodSwitchError('');
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
