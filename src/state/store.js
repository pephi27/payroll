const initialState = {
  currentPeriodId: null,
  payrollPeriods: new Map(),
  payrollSnapshots: new Map(),
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
    lastRealtimeEvent: null,
  },
};

const state = structuredClone(initialState);
const listeners = new Set();

export function getState() {
  return state;
}

export function subscribe(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function notify(change) {
  listeners.forEach((listener) => listener(state, change));
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
