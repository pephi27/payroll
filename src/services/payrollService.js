import { getSupabaseClient } from '../config/supabaseClient.js';
import { getFeatureFlag } from '../config/featureFlags.js';
import { getState, mergeRow, removeRow, reportConflict, resetTable } from '../state/store.js';

export class StaleWriteError extends Error {
  constructor(message, context = {}) {
    super(message);
    this.name = 'StaleWriteError';
    this.context = context;
  }
}

function summarizePayload(payload) {
  if (!payload || typeof payload !== 'object') return payload;
  const summary = {};
  for (const key of ['id', 'payroll_period_id', 'employee_id', 'project_id', 'loan_id', 'updated_at']) {
    if (key in payload) summary[key] = payload[key];
  }
  return summary;
}

function logWrite(action, table, payload) {
  console.info('[payroll:write]', { action, table, payload: summarizePayload(payload), at: new Date().toISOString() });
}

const TABLES = {
  periods: 'payroll_periods',
  snapshots: 'payroll_period_snapshots',
  dtr: 'pp_dtr_records',
  punches: 'dtr_punches',
  dtrApprovals: 'pp_dtr_approvals',
  employees: 'pp_employees',
  projects: 'pp_projects',
  schedules: 'pp_schedules',
  loans: 'employee_loans',
  loanDeductions: 'loan_deductions',
  contribFlags: 'pp_contrib_flags',
  profiles: 'profiles',
};

const TABLE_LOADERS = {
  dtrRecords: { table: TABLES.dtr, stateKey: 'dtrRecords' },
  employees: { table: TABLES.employees, stateKey: 'employees' },
  projects: { table: TABLES.projects, stateKey: 'projects' },
  schedules: { table: TABLES.schedules, stateKey: 'schedules' },
  loans: { table: TABLES.loans, stateKey: 'loans' },
  loanDeductions: { table: TABLES.loanDeductions, stateKey: 'loanDeductions' },
  contribFlags: { table: TABLES.contribFlags, stateKey: 'contribFlags' },
  dtrApprovals: { table: TABLES.dtrApprovals, stateKey: 'dtrApprovals' },
  profiles: { table: TABLES.profiles, stateKey: 'profiles' },
};



const PERIOD_SCOPED_STATE_KEYS = [
  'dtrRecords',
  'dtrPunches',
  'employees',
  'projects',
  'schedules',
  'loans',
  'loanDeductions',
  'contribFlags',
  'dtrApprovals',
];

function resetPeriodScopedState() {
  PERIOD_SCOPED_STATE_KEYS.forEach((tableKey) => resetTable(tableKey));
}

function isMissingFunctionError(error, fnName) {
  const message = String(error?.message || '');
  return error?.code === 'PGRST202' || new RegExp(`function .*${fnName}`, 'i').test(message);
}

function isStalePeriodLockError(error) {
  return error?.code === 'P0001' && /stale payroll period lock write/i.test(String(error?.message || ''));
}
const OPTIMIZED_LOAD_FLAG = 'USE_OPTIMIZED_LOAD';
const PAGE_SIZE = 1000;
const PERIOD_COLUMN = 'payroll_period_id';

function hasOwn(row, key) {
  return Object.prototype.hasOwnProperty.call(row || {}, key);
}

function sortRowsByKnownTimestamp(table, rows) {
  if (table !== TABLES.punches) return rows;
  rows.sort((a, b) => {
    const aStamp = a?.punch_at || `${a?.date || ''} ${a?.time || ''}`;
    const bStamp = b?.punch_at || `${b?.date || ''} ${b?.time || ''}`;
    return String(aStamp).localeCompare(String(bStamp));
  });
  return rows;
}

async function fetchTablePage({ table, from, to, periodId, optimized }) {
  let query = requireSupabaseClient().from(table).select('*').range(from, to);
  if (optimized && periodId && table !== TABLES.punches) {
    query = query.eq(PERIOD_COLUMN, periodId);
  }
  return query;
}

async function fetchAllRowsPaginated({ table, periodId, optimized }) {
  const rows = [];
  let page = 0;

  for (;;) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await fetchTablePage({ table, from, to, periodId, optimized });
    if (error) {
      return { data: null, error };
    }

    const pageRows = Array.isArray(data) ? data : [];
    rows.push(...pageRows);
    if (pageRows.length < PAGE_SIZE) break;
    page += 1;
  }

  return { data: sortRowsByKnownTimestamp(table, rows), error: null };
}

function getPeriodCoverage(rows, periodId) {
  const list = Array.isArray(rows) ? rows : [];
  const withPeriodId = list.filter((row) => hasOwn(row, PERIOD_COLUMN) && row?.[PERIOD_COLUMN]);
  const missingPeriodId = list.filter((row) => !hasOwn(row, PERIOD_COLUMN) || !row?.[PERIOD_COLUMN]);
  const matched = withPeriodId.filter((row) => row[PERIOD_COLUMN] === periodId);

  return {
    total: list.length,
    withPeriodId: withPeriodId.length,
    missingPeriodId: missingPeriodId.length,
    matched: matched.length,
    hasLegacyRows: missingPeriodId.length > 0,
  };
}

async function loadRowsWithOptionalOptimizedFilter(table, periodId) {
  const useOptimized = getFeatureFlag(OPTIMIZED_LOAD_FLAG, false);
  if (!useOptimized || !periodId) {
    return fetchAllRowsPaginated({ table, periodId, optimized: false });
  }

  const optimizedResult = await fetchAllRowsPaginated({ table, periodId, optimized: true });
  if (!optimizedResult.error) {
    return optimizedResult;
  }

  console.warn('[payroll:read:fallback] optimized query failed, falling back to legacy query', {
    table,
    code: optimizedResult.error.code,
    message: optimizedResult.error.message,
  });

  return fetchAllRowsPaginated({ table, periodId, optimized: false });
}

async function debugVerifyPeriodLoad(table, periodId) {
  const useOptimized = getFeatureFlag(OPTIMIZED_LOAD_FLAG, false);
  if (!useOptimized || !periodId) return;

  try {
    const legacy = await fetchAllRowsPaginated({ table, periodId, optimized: false });
    const optimized = await fetchAllRowsPaginated({ table, periodId, optimized: true });
    if (legacy.error || optimized.error) return;

    const legacyCoverage = getPeriodCoverage(legacy.data, periodId);
    const optimizedCoverage = getPeriodCoverage(optimized.data, periodId);

    const shouldFallback = legacyCoverage.hasLegacyRows;
    if (shouldFallback) {
      console.warn('[payroll:verify] legacy rows without payroll_period_id detected; optimized filtering may omit rows', {
        table,
        periodId,
        legacyCoverage,
        optimizedCoverage,
      });
      return;
    }

    if (legacyCoverage.matched !== optimizedCoverage.total) {
      console.warn('[payroll:verify] row count mismatch between legacy and optimized loads', {
        table,
        periodId,
        legacyMatched: legacyCoverage.matched,
        optimizedCount: optimizedCoverage.total,
      });
    }

    const legacyTotal = (legacy.data || []).reduce((sum, row) => {
      const gross = Number(row?.gross_pay || 0);
      const net = Number(row?.net_pay || 0);
      return sum + gross + net;
    }, 0);

    const optimizedTotal = (optimized.data || []).reduce((sum, row) => {
      const gross = Number(row?.gross_pay || 0);
      const net = Number(row?.net_pay || 0);
      return sum + gross + net;
    }, 0);

    if (Math.abs(legacyTotal - optimizedTotal) > 0.0001) {
      console.warn('[payroll:verify] totals mismatch between legacy and optimized loads', {
        table,
        periodId,
        legacyTotal,
        optimizedTotal,
      });
    }
  } catch (error) {
    console.warn('[payroll:verify] debug verify failed', { table, periodId, message: error?.message || String(error) });
  }
}


async function fetchPunchRowsByKnownShapes(periodId) {
  if (!periodId) return { data: [], error: null };
  const period = await getPeriodById(periodId);
  if (!period) {
    return { data: null, error: new Error(`Payroll period ${periodId} not found.`) };
  }

  const periodStart = normalizeDateString(period.period_start);
  const periodEnd = normalizeDateString(period.period_end);
  if (!periodStart || !periodEnd) {
    return { data: null, error: new Error(`Payroll period ${periodId} has invalid date boundaries.`) };
  }

  try {
    await ensureTableAvailable(TABLES.punches, { context: 'load DTR punches' });
  } catch (error) {
    if (isMissingTableError(error, TABLES.punches) || /Supabase table "dtr_punches" is not available/i.test(String(error?.message || ''))) {
      console.warn('[payroll:dtr] punches table unavailable; loading zero punch rows until the migration is applied', {
        periodId,
        message: error?.message || String(error),
      });
      return { data: [], error: null };
    }
    return { data: null, error };
  }

  const rows = [];
  let page = 0;
  for (;;) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await requireSupabaseClient()
      .from(TABLES.punches)
      .select('*')
      .gte('date', periodStart)
      .lte('date', periodEnd)
      .range(from, to);
    if (error) return { data: null, error };

    const pageRows = Array.isArray(data) ? data : [];
    rows.push(...pageRows);
    if (pageRows.length < PAGE_SIZE) break;
    page += 1;
  }

  const filtered = rows.filter((row) => rowBelongsToPeriod(row, periodId, period));
  sortRowsByKnownTimestamp(TABLES.punches, filtered);
  return { data: filtered, error: null };
}

function requireSupabaseClient() {
  const client = getSupabaseClient();
  if (!client) {
    throw new Error('Supabase client is not ready. Ensure the legacy bootstrap initialized window.supabase.');
  }
  return client;
}

let hasRpcLockGuard = null;
let hasRpcDtrEditableGuard = null;
const tableAvailability = new Map();

function isMissingTableError(error, tableName = '') {
  const message = String(error?.message || error?.details || error?.error_description || '');
  const code = String(error?.code || '');
  const escapedTableName = String(tableName || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return code === 'PGRST205'
    || code === '42P01'
    || /schema cache|Could not find the table|does not exist/i.test(message)
    || (escapedTableName ? new RegExp(`relation .*${escapedTableName}`, 'i').test(message) : false);
}

function createMissingTableError(tableName, context = 'use this feature') {
  return new Error(
    `Supabase table "${tableName}" is not available, so the app cannot ${context}. ` +
    'Run the DTR punch migrations and refresh the Supabase schema cache before retrying.',
  );
}

async function ensureTableAvailable(tableName, { context = 'use this feature' } = {}) {
  if (tableAvailability.get(tableName) === true) return;
  if (tableAvailability.get(tableName) === false) {
    throw createMissingTableError(tableName, context);
  }

  const { error } = await requireSupabaseClient()
    .from(tableName)
    .select('id', { head: true, count: 'exact' })
    .limit(1);

  if (!error) {
    tableAvailability.set(tableName, true);
    return;
  }

  if (isMissingTableError(error, tableName)) {
    tableAvailability.set(tableName, false);
    throw createMissingTableError(tableName, context);
  }

  throw error;
}

function normalizeWorkDate(workDate) {
  const raw = String(workDate || '').trim();
  const match = raw.match(/^(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : '';
}

function deriveWorkDateFromPunchAt(punchAt) {
  const value = String(punchAt || '').trim();
  const match = value.match(/^(\d{4}-\d{2}-\d{2})[T\s]/) || value.match(/^(\d{4}-\d{2}-\d{2})$/);
  return match ? match[1] : '';
}

function normalizeDateString(value) {
  const raw = String(value || '').trim();
  const match = raw.match(/^(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : '';
}

function normalizeTimeString(value) {
  const raw = String(value || '').trim();
  const match = raw.match(/^(\d{2}:\d{2})/);
  return match ? match[1] : '';
}

function parseLegacyPunchStamp(punchAt) {
  const value = String(punchAt || '').trim();
  const match = value.match(/^(\d{4}-\d{2}-\d{2})[T\s](\d{2}:\d{2})/);
  if (!match) return { workDate: '', time: '' };
  return { workDate: match[1], time: match[2] };
}

function periodIncludesDate(period, workDate) {
  const start = normalizeDateString(period?.period_start);
  const end = normalizeDateString(period?.period_end);
  const date = normalizeDateString(workDate);
  if (!start || !end || !date) return false;
  return date >= start && date <= end;
}

async function getPeriodById(periodId) {
  const id = String(periodId || '').trim();
  if (!id) return null;
  const cached = getState().payrollPeriods.get(id);
  if (cached) return cached;

  const { data, error } = await requireSupabaseClient()
    .from(TABLES.periods)
    .select('*')
    .eq('id', id)
    .maybeSingle();
  if (error) throw error;
  if (data) mergeRow('payrollPeriods', data);
  return data || null;
}

async function resolvePeriodIdForWorkDate(workDate, preferredPeriodId = null) {
  const ymd = normalizeDateString(workDate);
  if (!ymd) throw new Error('Missing work date for DTR row context resolution.');

  if (preferredPeriodId) {
    const preferred = await getPeriodById(preferredPeriodId);
    if (preferred && periodIncludesDate(preferred, ymd)) {
      return String(preferred.id);
    }
  }

  const matches = [];
  getState().payrollPeriods.forEach((period) => {
    if (periodIncludesDate(period, ymd)) matches.push(period);
  });

  if (matches.length === 1) return String(matches[0].id);
  if (matches.length === 0) throw new Error(`No payroll period matches work date ${ymd}. Update denied.`);
  throw new Error(`Multiple payroll periods match work date ${ymd}. Update denied.`);
}

function getLegacyPunchWorkDate(rowLike = {}) {
  return normalizeDateString(rowLike.date) || deriveWorkDateFromPunchAt(rowLike.punch_at);
}

function getLegacyPunchEmployeeId(rowLike = {}) {
  return String(rowLike.emp_id ?? rowLike.employee_id ?? '').trim();
}

function getRowMetaPeriodId(rowLike = {}) {
  const meta = rowLike?.data?.payroll_period_id;
  return meta == null ? '' : String(meta).trim();
}

function rowBelongsToPeriod(row, periodId, period) {
  const metaPeriodId = getRowMetaPeriodId(row);
  // Never trust metadata period id blindly; it must match target period and period date window.
  if (metaPeriodId) {
    if (metaPeriodId !== String(periodId)) return false;
    if (!period) return true;
    return periodIncludesDate(period, getLegacyPunchWorkDate(row));
  }
  return periodIncludesDate(period, getLegacyPunchWorkDate(row));
}

async function getPunchRowContextFromRow(rowLike = {}, periodHint = null) {
  const employeeId = getLegacyPunchEmployeeId(rowLike);
  const workDate = getLegacyPunchWorkDate(rowLike);
  const metaPeriodId = getRowMetaPeriodId(rowLike);
  const periodId = await resolvePeriodIdForWorkDate(workDate, metaPeriodId || periodHint || null);
  return assertDtrContext({ periodId, employeeId, workDate });
}

function normalizePunchPatchForLegacyShape(existing = {}, patch = {}, periodIdHint = null) {
  const legacyPatch = sanitizeUpdatePatch(patch);
  if (Object.prototype.hasOwnProperty.call(legacyPatch, 'employee_id')) {
    legacyPatch.emp_id = legacyPatch.employee_id;
    delete legacyPatch.employee_id;
  }

  if (Object.prototype.hasOwnProperty.call(legacyPatch, 'punch_at')) {
    const parsed = parseLegacyPunchStamp(legacyPatch.punch_at);
    if (parsed.workDate) legacyPatch.date = parsed.workDate;
    if (parsed.time) legacyPatch.time = parsed.time;
    delete legacyPatch.punch_at;
  }

  if (!legacyPatch.data || typeof legacyPatch.data !== 'object') {
    legacyPatch.data = { ...(existing?.data || {}) };
  }
  legacyPatch.data = { ...legacyPatch.data };

  const chosenPeriodId = String(
    periodIdHint
    || legacyPatch.data.payroll_period_id
    || existing?.data?.payroll_period_id
    || '',
  ).trim();
  if (chosenPeriodId) legacyPatch.data.payroll_period_id = chosenPeriodId;

  return legacyPatch;
}

function assertDtrContext({ periodId, employeeId, workDate }) {
  const normalizedWorkDate = normalizeWorkDate(workDate);
  if (!periodId || !employeeId || !normalizedWorkDate) {
    throw new Error('Missing DTR row context. Expected periodId, employeeId, and workDate.');
  }
  return {
    periodId: String(periodId),
    employeeId: String(employeeId),
    workDate: normalizedWorkDate,
  };
}

function findApprovalRowInState({ periodId, employeeId, workDate }) {
  const state = getState();
  const targetPeriodId = String(periodId);
  const targetEmployeeId = String(employeeId);
  const targetWorkDate = normalizeWorkDate(workDate);
  for (const row of state.dtrApprovals.values()) {
    if (!row) continue;
    if (String(row.payroll_period_id) !== targetPeriodId) continue;
    if (String(row.employee_id) !== targetEmployeeId) continue;
    if (normalizeWorkDate(row.work_date) !== targetWorkDate) continue;
    return row;
  }
  return null;
}

async function ensurePeriodUnlockedWithRpc(periodId) {
  // Important: this is a precheck guard for compatibility; it is not a transactional DB-side enforcement
  // because writes are still executed in a separate statement.
  if (!periodId) return;
  const client = requireSupabaseClient();

  if (hasRpcLockGuard !== false) {
    const { error } = await client.rpc('assert_payroll_period_unlocked', { p_period_id: periodId });
    if (!error) {
      hasRpcLockGuard = true;
      return;
    }

    const missingRpc = isMissingFunctionError(error, 'assert_payroll_period_unlocked');
    if (!missingRpc) throw error;
    hasRpcLockGuard = false;
    console.warn('[payroll:lock-guard] RPC assert_payroll_period_unlocked is unavailable; using client-side lock fallback');
  }

  await ensurePeriodUnlocked(periodId);
}

async function ensurePeriodUnlocked(periodId) {
  if (!periodId) return;

  const { data, error } = await requireSupabaseClient()
    .from(TABLES.periods)
    .select('id,is_locked')
    .eq('id', periodId)
    .single();

  if (error) throw error;
  if (data.is_locked) {
    throw new Error(`Payroll period ${periodId} is locked. Update denied.`);
  }
}

async function ensureDtrRowEditableWithFallback({ periodId, employeeId, workDate }) {
  const context = assertDtrContext({ periodId, employeeId, workDate });
  await ensurePeriodUnlocked(context.periodId);

  const { data, error } = await requireSupabaseClient()
    .from(TABLES.dtrApprovals)
    .select('id,is_approved')
    .eq('payroll_period_id', context.periodId)
    .eq('employee_id', context.employeeId)
    .eq('work_date', context.workDate)
    .maybeSingle();

  if (error) throw error;
  if (data?.is_approved === true) {
    throw new Error(`DTR row ${context.employeeId} on ${context.workDate} is approved. Update denied.`);
  }
}

async function loadExistingDtrApprovalByContext({ periodId, employeeId, workDate }) {
  const context = assertDtrContext({ periodId, employeeId, workDate });
  const { data, error } = await requireSupabaseClient()
    .from(TABLES.dtrApprovals)
    .select('*')
    .eq('payroll_period_id', context.periodId)
    .eq('employee_id', context.employeeId)
    .eq('work_date', context.workDate)
    .maybeSingle();
  if (error) throw error;
  return data;
}


function sanitizeUpdatePatch(patch = {}) {
  const cleaned = {};
  for (const [key, value] of Object.entries(patch || {})) {
    if (value === undefined) continue;
    if (key === 'id') continue;
    cleaned[key] = value;
  }
  return cleaned;
}

function sanitizeInsertPayload(row = {}) {
  const cleaned = {};
  for (const [key, value] of Object.entries(row || {})) {
    if (value === undefined) continue;
    cleaned[key] = value;
  }
  return cleaned;
}

function composeCreateRow(id, patch = {}) {
  const cleanedPatch = sanitizeInsertPayload(patch);
  if (id == null || id === '') return cleanedPatch;
  return { id, ...cleanedPatch };
}

function applyExpectedUpdatedAt(query, expectedUpdatedAt) {
  if (expectedUpdatedAt == null) {
    return query.is('updated_at', null);
  }
  return query.eq('updated_at', expectedUpdatedAt);
}

async function updateWithOptimisticLock({ table, id, patch, expectedUpdatedAt, periodId, stateKey }) {
  await ensurePeriodUnlockedWithRpc(periodId);
  await ensureTableAvailable(table, { context: `update records in ${table}` });
  logWrite('update', table, { id, ...patch, expected_updated_at: expectedUpdatedAt });

  let query = requireSupabaseClient()
    .from(table)
    .update({ ...sanitizeUpdatePatch(patch), updated_at: new Date().toISOString() })
    .eq('id', id);

  query = applyExpectedUpdatedAt(query, expectedUpdatedAt);

  const { data, error } = await query.select('*').maybeSingle();
  if (error) throw error;

  if (!data) {
    const conflict = { table, id, expectedUpdatedAt, at: new Date().toISOString() };
    reportConflict(conflict);
    throw new StaleWriteError('This record was updated by another user. Reloading.', conflict);
  }

  mergeRow(stateKey, data);
  return data;
}

async function createRowWithLock({ table, row, periodId, stateKey }) {
  await ensurePeriodUnlockedWithRpc(periodId);
  await ensureTableAvailable(table, { context: `create records in ${table}` });
  logWrite('insert', table, row);
  const payload = { ...sanitizeInsertPayload(row), updated_at: new Date().toISOString() };

  const { data, error } = await requireSupabaseClient()
    .from(table)
    .insert(payload)
    .select('*')
    .single();

  if (error) throw error;
  mergeRow(stateKey, data);
  return data;
}


async function deleteWithOptimisticLock({ table, id, expectedUpdatedAt, periodId, stateKey }) {
  await ensurePeriodUnlockedWithRpc(periodId);
  await ensureTableAvailable(table, { context: `delete records from ${table}` });
  logWrite('delete', table, { id, expected_updated_at: expectedUpdatedAt });

  let query = requireSupabaseClient().from(table).delete().eq('id', id);
  query = applyExpectedUpdatedAt(query, expectedUpdatedAt);

  const { data, error } = await query.select('id').maybeSingle();
  if (error) throw error;

  if (!data) {
    const conflict = { table, id, expectedUpdatedAt, at: new Date().toISOString() };
    reportConflict(conflict);
    throw new StaleWriteError('This record was updated by another user. Reloading.', conflict);
  }

  return { id, table, stateKey };
}

export const payrollService = {
  tables: TABLES,

  async debugVerifyOptimizedLoad(periodId) {
    const tables = [
      TABLES.dtr,
      TABLES.loans,
      TABLES.loanDeductions,
      TABLES.contribFlags,
      TABLES.punches,
    ];

    for (const table of tables) {
      await debugVerifyPeriodLoad(table, periodId);
    }
  },

  async loadPeriods() {
    const { data, error } = await requireSupabaseClient()
      .from(TABLES.periods)
      .select('*')
      .order('period_start', { ascending: false });

    if (error) throw error;
    data.forEach((period) => mergeRow('payrollPeriods', period));
    return data;
  },

  async fetchPunchesByPeriod(periodId) {
    const { data, error } = await fetchPunchRowsByKnownShapes(periodId);
    if (error) throw error;
    return Array.isArray(data) ? data : [];
  },

  async loadPunchesByPeriod(periodId) {
    const rows = await this.fetchPunchesByPeriod(periodId);
    rows.forEach((row) => mergeRow('dtrPunches', row));
    return rows;
  },

  async fetchDtrApprovalsByPeriod(periodId) {
    if (!periodId) return [];
    const { data, error } = await requireSupabaseClient()
      .from(TABLES.dtrApprovals)
      .select('*')
      .eq('payroll_period_id', periodId);
    if (error) throw error;
    return Array.isArray(data) ? data : [];
  },

  async loadDtrApprovalsByPeriod(periodId) {
    const rows = await this.fetchDtrApprovalsByPeriod(periodId);
    rows.forEach((row) => mergeRow('dtrApprovals', row));
    return rows;
  },

  getDtrApprovalKey({ periodId, employeeId, workDate }) {
    const context = assertDtrContext({ periodId, employeeId, workDate });
    return `${context.periodId}__${context.employeeId}__${context.workDate}`;
  },

  async ensureDtrRowEditableWithRpcOrFallback({ periodId, employeeId, workDate }) {
    const context = assertDtrContext({ periodId, employeeId, workDate });
    const client = requireSupabaseClient();

    if (hasRpcDtrEditableGuard !== false) {
      const { error } = await client.rpc('assert_dtr_row_editable', {
        p_period_id: context.periodId,
        p_employee_id: context.employeeId,
        p_work_date: context.workDate,
      });
      if (!error) {
        hasRpcDtrEditableGuard = true;
        return;
      }

      const missingRpc = isMissingFunctionError(error, 'assert_dtr_row_editable');
      if (!missingRpc) throw error;
      hasRpcDtrEditableGuard = false;
      console.warn('[payroll:dtr-guard] RPC assert_dtr_row_editable is unavailable; using fallback checks');
    }

    await ensureDtrRowEditableWithFallback(context);
  },

  async approveDtrRow({ periodId, employeeId, workDate, note = null, approvedBy = null }) {
    const context = assertDtrContext({ periodId, employeeId, workDate });
    await ensurePeriodUnlockedWithRpc(context.periodId);

    const id = this.getDtrApprovalKey(context);
    const existing = getState().dtrApprovals.get(id)
      || findApprovalRowInState(context)
      || await loadExistingDtrApprovalByContext(context);
    const now = new Date().toISOString();
    const patch = {
      id,
      payroll_period_id: context.periodId,
      employee_id: context.employeeId,
      work_date: context.workDate,
      is_approved: true,
      approved_at: now,
      approved_by: approvedBy,
      note,
      updated_at: now,
    };

    if (existing) {
      return updateWithOptimisticLock({
        table: TABLES.dtrApprovals,
        id,
        patch,
        expectedUpdatedAt: existing.updated_at ?? null,
        periodId: context.periodId,
        stateKey: 'dtrApprovals',
      });
    }

    return createRowWithLock({
      table: TABLES.dtrApprovals,
      row: patch,
      periodId: context.periodId,
      stateKey: 'dtrApprovals',
    });
  },

  async unapproveDtrRow({ periodId, employeeId, workDate }) {
    const context = assertDtrContext({ periodId, employeeId, workDate });
    await ensurePeriodUnlockedWithRpc(context.periodId);
    const id = this.getDtrApprovalKey(context);
    const existing = getState().dtrApprovals.get(id)
      || findApprovalRowInState(context)
      || await loadExistingDtrApprovalByContext(context);
    if (!existing) return null;

    return updateWithOptimisticLock({
      table: TABLES.dtrApprovals,
      id,
      patch: {
        is_approved: false,
        approved_at: null,
        approved_by: null,
      },
      expectedUpdatedAt: existing.updated_at ?? null,
      periodId: context.periodId,
      stateKey: 'dtrApprovals',
    });
  },

  async loadCoreReadModels({ periodId, resetPeriodTables = true } = {}) {
    if (resetPeriodTables) {
      resetPeriodScopedState();
    }

    const loaders = Object.values(TABLE_LOADERS).map(async ({ table, stateKey }) => {
      const result = await loadRowsWithOptionalOptimizedFilter(table, periodId);
      if (result.error) throw result.error;

      const rows = Array.isArray(result.data) ? result.data : [];
      const coverage = getPeriodCoverage(rows, periodId);

      let rowsToMerge = rows;
      if (getFeatureFlag(OPTIMIZED_LOAD_FLAG, false) && periodId && coverage.hasLegacyRows) {
        console.warn('[payroll:read:fallback] optimized filtering may omit legacy rows; using legacy in-memory filter', {
          table,
          periodId,
          coverage,
        });
        const legacy = await fetchAllRowsPaginated({ table, periodId, optimized: false });
        if (legacy.error) throw legacy.error;
        rowsToMerge = Array.isArray(legacy.data) ? legacy.data : [];
      }

      rowsToMerge.forEach((row) => mergeRow(stateKey, row));
      debugVerifyPeriodLoad(table, periodId);
      return { table, count: rowsToMerge.length };
    });

    const punches = this.loadPunchesByPeriod(periodId);
    return Promise.all([...loaders, punches]);
  },

  async createPunch({ periodId, employeeId, projectId, punchAt, meta = {}, skipEditableCheck = false }) {
    const parsed = parseLegacyPunchStamp(punchAt);
    const context = assertDtrContext({ periodId, employeeId, workDate: parsed.workDate });
    if (!parsed.time) {
      throw new Error('Missing punch time. Update denied.');
    }
    if (!skipEditableCheck) {
      await this.ensureDtrRowEditableWithRpcOrFallback(context);
    }
    const row = {
      emp_id: context.employeeId,
      date: context.workDate,
      time: parsed.time,
      source: String(meta?.source || 'manual'),
      data: {
        ...(meta && typeof meta === 'object' ? meta : {}),
        payroll_period_id: context.periodId,
        project_id: projectId ?? null,
      },
    };

    return createRowWithLock({
      table: TABLES.punches,
      row,
      periodId: context.periodId,
      stateKey: 'dtrPunches',
    });
  },

  async updatePunch(punchId, patch, options = {}) {
    const state = getState();
    const existing = state.dtrPunches.get(punchId);
    if (!existing) throw new Error(`Punch ${punchId} not found in state.`);
    const sourceContext = await getPunchRowContextFromRow(existing, getState().currentPeriodId);
    const legacyPatch = normalizePunchPatchForLegacyShape(existing, patch, sourceContext.periodId);
    const destinationContext = await getPunchRowContextFromRow({ ...existing, ...legacyPatch }, sourceContext.periodId);

    if (!options.skipEditableCheck) {
      await this.ensureDtrRowEditableWithRpcOrFallback(sourceContext);

      const destinationDiffers = sourceContext.periodId !== destinationContext.periodId
        || sourceContext.employeeId !== destinationContext.employeeId
        || sourceContext.workDate !== destinationContext.workDate;
      if (destinationDiffers) {
        await this.ensureDtrRowEditableWithRpcOrFallback(destinationContext);
      }
    }

    return updateWithOptimisticLock({
      table: TABLES.punches,
      id: punchId,
      patch: legacyPatch,
      expectedUpdatedAt: existing.updated_at ?? null,
      periodId: sourceContext.periodId,
      stateKey: 'dtrPunches',
    });
  },

  async deletePunch(punchId, options = {}) {
    const state = getState();
    const existing = state.dtrPunches.get(punchId);
    if (!existing) throw new Error(`Punch ${punchId} not found in state.`);
    const context = await getPunchRowContextFromRow(existing, getState().currentPeriodId);
    if (!options.skipEditableCheck) {
      await this.ensureDtrRowEditableWithRpcOrFallback(context);
    }

    await deleteWithOptimisticLock({
      table: TABLES.punches,
      id: punchId,
      expectedUpdatedAt: existing.updated_at ?? null,
      periodId: context.periodId,
      stateKey: 'dtrPunches',
    });

    removeRow('dtrPunches', punchId);
    return { id: punchId };
  },

  async replacePunchesForEmployeeDate({ periodId, employeeId, workDate, requestedTimes, meta = {}, projectId = null }) {
    const context = assertDtrContext({ periodId, employeeId, workDate });
    await this.ensureDtrRowEditableWithRpcOrFallback(context);

    const wantedTimes = (Array.isArray(requestedTimes) ? requestedTimes : [])
      .map((time) => String(time || '').trim())
      .filter(Boolean);

    const existingRows = Array.from(getState().dtrPunches.values()).filter((row) => {
      if (!row) return false;
      const period = getState().payrollPeriods.get(context.periodId);
      if (!rowBelongsToPeriod(row, context.periodId, period)) return false;
      if (String(row.emp_id || '') !== context.employeeId) return false;
      return normalizeDateString(row.date) === context.workDate;
    });

    const existingQueue = existingRows.map((row) => ({
      row,
      time: normalizeTimeString(row.time),
    }));

    const toCreate = [];
    wantedTimes.forEach((time) => {
      const idx = existingQueue.findIndex((entry) => entry.time === time);
      if (idx >= 0) {
        existingQueue.splice(idx, 1);
        return;
      }
      toCreate.push(time);
    });

    for (const entry of existingQueue) {
      await this.deletePunch(entry.row.id, { skipEditableCheck: true });
    }

    for (const time of toCreate) {
      await this.createPunch({
        periodId: context.periodId,
        employeeId: context.employeeId,
        projectId,
        punchAt: `${context.workDate}T${time}:00`,
        meta,
        skipEditableCheck: true,
      });
    }
  },

  async upsertEmployee(employeeId, patch = {}) {
    const state = getState();
    const existing = state.employees.get(employeeId);
    if (existing) {
      return updateWithOptimisticLock({
        table: TABLES.employees,
        id: employeeId,
        patch: sanitizeUpdatePatch(patch),
        expectedUpdatedAt: existing.updated_at ?? null,
        stateKey: 'employees',
      });
    }

    return createRowWithLock({
      table: TABLES.employees,
      row: composeCreateRow(employeeId, patch),
      stateKey: 'employees',
    });
  },

  async upsertLoan(loanId, patch = {}) {
    const state = getState();
    const existing = state.loans.get(loanId);
    const periodId = patch.payroll_period_id ?? existing?.payroll_period_id ?? getState().currentPeriodId;

    if (existing) {
      return updateWithOptimisticLock({
        table: TABLES.loans,
        id: loanId,
        patch: sanitizeUpdatePatch(patch),
        expectedUpdatedAt: existing.updated_at ?? null,
        periodId,
        stateKey: 'loans',
      });
    }

    return createRowWithLock({
      table: TABLES.loans,
      row: composeCreateRow(loanId, patch),
      periodId,
      stateKey: 'loans',
    });
  },

  async upsertLoanDeduction(deductionId, patch = {}) {
    const state = getState();
    const existing = state.loanDeductions.get(deductionId);
    const periodId = patch.payroll_period_id ?? existing?.payroll_period_id ?? getState().currentPeriodId;

    if (existing) {
      return updateWithOptimisticLock({
        table: TABLES.loanDeductions,
        id: deductionId,
        patch: sanitizeUpdatePatch(patch),
        expectedUpdatedAt: existing.updated_at ?? null,
        periodId,
        stateKey: 'loanDeductions',
      });
    }

    return createRowWithLock({
      table: TABLES.loanDeductions,
      row: composeCreateRow(deductionId, patch),
      periodId,
      stateKey: 'loanDeductions',
    });
  },

  async upsertDtrRecord(recordId, patch = {}) {
    const state = getState();
    const existing = state.dtrRecords.get(recordId);
    const periodId = patch.payroll_period_id ?? existing?.payroll_period_id ?? getState().currentPeriodId;

    if (existing) {
      return updateWithOptimisticLock({
        table: TABLES.dtr,
        id: recordId,
        patch: sanitizeUpdatePatch(patch),
        expectedUpdatedAt: existing.updated_at ?? null,
        periodId,
        stateKey: 'dtrRecords',
      });
    }

    return createRowWithLock({
      table: TABLES.dtr,
      row: composeCreateRow(recordId, patch),
      periodId,
      stateKey: 'dtrRecords',
    });
  },

  async upsertProject(projectId, patch = {}) {
    const state = getState();
    const existing = state.projects.get(projectId);
    if (existing) {
      return updateWithOptimisticLock({
        table: TABLES.projects,
        id: projectId,
        patch: sanitizeUpdatePatch(patch),
        expectedUpdatedAt: existing.updated_at ?? null,
        stateKey: 'projects',
      });
    }

    return createRowWithLock({
      table: TABLES.projects,
      row: composeCreateRow(projectId, patch),
      stateKey: 'projects',
    });
  },

  async upsertSchedule(scheduleId, patch = {}) {
    const state = getState();
    const existing = state.schedules.get(scheduleId);
    if (existing) {
      return updateWithOptimisticLock({
        table: TABLES.schedules,
        id: scheduleId,
        patch: sanitizeUpdatePatch(patch),
        expectedUpdatedAt: existing.updated_at ?? null,
        stateKey: 'schedules',
      });
    }

    return createRowWithLock({
      table: TABLES.schedules,
      row: composeCreateRow(scheduleId, patch),
      stateKey: 'schedules',
    });
  },

  async upsertContribFlag(flagId, patch = {}) {
    const state = getState();
    const existing = state.contribFlags.get(flagId);
    const periodId = patch.payroll_period_id ?? existing?.payroll_period_id ?? getState().currentPeriodId;

    if (existing) {
      return updateWithOptimisticLock({
        table: TABLES.contribFlags,
        id: flagId,
        patch: sanitizeUpdatePatch(patch),
        expectedUpdatedAt: existing.updated_at ?? null,
        periodId,
        stateKey: 'contribFlags',
      });
    }

    return createRowWithLock({
      table: TABLES.contribFlags,
      row: composeCreateRow(flagId, patch),
      periodId,
      stateKey: 'contribFlags',
    });
  },


  async deleteDtrRecord(recordId) {
    const state = getState();
    const existing = state.dtrRecords.get(recordId);
    if (!existing) throw new Error(`DTR record ${recordId} not found in state.`);

    await deleteWithOptimisticLock({
      table: TABLES.dtr,
      id: recordId,
      expectedUpdatedAt: existing.updated_at ?? null,
      periodId: existing.payroll_period_id,
      stateKey: 'dtrRecords',
    });

    removeRow('dtrRecords', recordId);
    return { id: recordId };
  },

  async deleteLoanDeduction(deductionId) {
    const state = getState();
    const existing = state.loanDeductions.get(deductionId);
    if (!existing) throw new Error(`Loan deduction ${deductionId} not found in state.`);

    await deleteWithOptimisticLock({
      table: TABLES.loanDeductions,
      id: deductionId,
      expectedUpdatedAt: existing.updated_at ?? null,
      periodId: existing.payroll_period_id,
      stateKey: 'loanDeductions',
    });

    removeRow('loanDeductions', deductionId);
    return { id: deductionId };
  },

  async deleteLoan(loanId) {
    const state = getState();
    const existing = state.loans.get(loanId);
    if (!existing) throw new Error(`Loan ${loanId} not found in state.`);

    await deleteWithOptimisticLock({
      table: TABLES.loans,
      id: loanId,
      expectedUpdatedAt: existing.updated_at ?? null,
      periodId: existing.payroll_period_id,
      stateKey: 'loans',
    });

    removeRow('loans', loanId);
    return { id: loanId };
  },

  async deleteContribFlag(flagId) {
    const state = getState();
    const existing = state.contribFlags.get(flagId);
    if (!existing) throw new Error(`Contribution flag ${flagId} not found in state.`);

    await deleteWithOptimisticLock({
      table: TABLES.contribFlags,
      id: flagId,
      expectedUpdatedAt: existing.updated_at ?? null,
      periodId: existing.payroll_period_id,
      stateKey: 'contribFlags',
    });

    removeRow('contribFlags', flagId);
    return { id: flagId };
  },

  async deleteEmployee(employeeId) {
    const state = getState();
    const existing = state.employees.get(employeeId);
    if (!existing) throw new Error(`Employee ${employeeId} not found in state.`);

    await deleteWithOptimisticLock({
      table: TABLES.employees,
      id: employeeId,
      expectedUpdatedAt: existing.updated_at ?? null,
      stateKey: 'employees',
    });

    removeRow('employees', employeeId);
    return { id: employeeId };
  },

  async deleteProject(projectId) {
    const state = getState();
    const existing = state.projects.get(projectId);
    if (!existing) throw new Error(`Project ${projectId} not found in state.`);

    await deleteWithOptimisticLock({
      table: TABLES.projects,
      id: projectId,
      expectedUpdatedAt: existing.updated_at ?? null,
      stateKey: 'projects',
    });

    removeRow('projects', projectId);
    return { id: projectId };
  },

  async deleteSchedule(scheduleId) {
    const state = getState();
    const existing = state.schedules.get(scheduleId);
    if (!existing) throw new Error(`Schedule ${scheduleId} not found in state.`);

    await deleteWithOptimisticLock({
      table: TABLES.schedules,
      id: scheduleId,
      expectedUpdatedAt: existing.updated_at ?? null,
      stateKey: 'schedules',
    });

    removeRow('schedules', scheduleId);
    return { id: scheduleId };
  },

  async setPeriodLock({ periodId, isLocked, expectedUpdatedAt = null, note = '', reason = '', actorId = null } = {}) {
    const payload = {
      p_period_id: periodId,
      p_is_locked: !!isLocked,
      p_expected_updated_at: expectedUpdatedAt,
      p_note: note || '',
      p_reason: reason || '',
      p_actor_id: actorId,
    };

    logWrite('rpc', 'set_payroll_period_lock', { id: periodId, is_locked: !!isLocked, expectedUpdatedAt });

    const { data, error } = await requireSupabaseClient()
      .rpc('set_payroll_period_lock', payload)
      .single();

    if (error) {
      if (isMissingFunctionError(error, 'set_payroll_period_lock')) {
        throw new Error('Payroll period lock RPC is unavailable. Run the latest DB migrations before locking/unlocking.');
      }
      if (isStalePeriodLockError(error)) {
        const conflict = { table: TABLES.periods, id: periodId, expectedUpdatedAt, at: new Date().toISOString() };
        reportConflict(conflict);
        try {
          await this.loadPeriods();
        } catch (reloadError) {
          console.warn('[payroll:lock] failed to refresh periods after stale lock conflict', reloadError);
        }
        throw new StaleWriteError('This payroll period lock was changed by another user. Latest period state was refreshed.', conflict);
      }
      throw error;
    }

    mergeRow('payrollPeriods', data);
    return data;
  },

  resetPeriodScopedState,

  async saveSnapshot(snapshot) {
    const { payroll_period_id: periodId } = snapshot;
    await ensurePeriodUnlockedWithRpc(periodId);

    logWrite('insert', TABLES.snapshots, snapshot);

    const { data, error } = await requireSupabaseClient()
      .from(TABLES.snapshots)
      .insert({ ...sanitizeInsertPayload(snapshot), created_at: new Date().toISOString() })
      .select('*')
      .single();

    if (error) throw error;
    mergeRow('payrollSnapshots', data);
    return data;
  },

  async updateSnapshot() {
    throw new Error('Snapshots are immutable. Create a new snapshot row instead.');
  },
};
