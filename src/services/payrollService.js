import { getSupabaseClient } from '../config/supabaseClient.js';
import { getFeatureFlag } from '../config/featureFlags.js';
import { getState, mergeRow, removeRow, reportConflict } from '../state/store.js';

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


const OPTIMIZED_LOAD_FLAG = 'USE_OPTIMIZED_LOAD';
const PAGE_SIZE = 1000;
const PERIOD_COLUMN = 'payroll_period_id';

function hasOwn(row, key) {
  return Object.prototype.hasOwnProperty.call(row || {}, key);
}

function isMissingTableError(error) {
  const code = String(error?.code || '').toUpperCase();
  const message = String(error?.message || '').toLowerCase();
  if (code === 'PGRST205' || code === '42P01') return true;
  return message.includes('could not find the table') || message.includes('relation') && message.includes('does not exist');
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
  if (optimized && periodId) {
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
  const useScopedReads = getFeatureFlag('payroll_ff_scoped_reads_v1', false) || getFeatureFlag(OPTIMIZED_LOAD_FLAG, false);

  if (useScopedReads && periodId) {
    const scoped = await loadRowsWithOptionalOptimizedFilter(TABLES.punches, periodId);
    if (!scoped.error) {
      const coverage = getPeriodCoverage(scoped.data, periodId);
      if (!coverage.hasLegacyRows) {
        debugVerifyPeriodLoad(TABLES.punches, periodId);
        return scoped;
      }

      console.warn('[payroll:read:fallback] scoped punches omitted legacy rows, falling back to legacy full fetch', {
        periodId,
        coverage,
      });
    } else {
      console.warn('[payroll:read:fallback] scoped punches query failed, falling back to legacy full fetch', {
        code: scoped.error.code,
        message: scoped.error.message,
      });
    }
  }

  const legacy = await fetchAllRowsPaginated({ table: TABLES.punches, periodId, optimized: false });
  if (legacy.error) return legacy;

  const rows = Array.isArray(legacy.data) ? legacy.data : [];
  const hasPeriodColumn = rows.some((row) => hasOwn(row, PERIOD_COLUMN));

  let filtered = rows;
  if (periodId && hasPeriodColumn) {
    filtered = rows.filter((row) => row?.payroll_period_id === periodId);
  }

  sortRowsByKnownTimestamp(TABLES.punches, filtered);
  debugVerifyPeriodLoad(TABLES.punches, periodId);
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

function deriveTimeFromPunchAt(punchAt, fallbackTime = '') {
  const value = String(punchAt || '').trim();
  const match = value.match(/^\d{4}-\d{2}-\d{2}[T\s](\d{2}:\d{2})/);
  if (match) return match[1];
  const fallback = String(fallbackTime || '').trim();
  const fallbackMatch = fallback.match(/^(\d{2}:\d{2})/);
  return fallbackMatch ? fallbackMatch[1] : '';
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

    const message = String(error?.message || '');
    const missingRpc = error.code === 'PGRST202' || /function .*assert_payroll_period_unlocked/i.test(message);
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

function getPunchRowContext({ periodId, employeeId, punchAt }) {
  return assertDtrContext({
    periodId,
    employeeId,
    workDate: deriveWorkDateFromPunchAt(punchAt),
  });
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

  async loadPunchesByPeriod(periodId) {
    const { data, error } = await fetchPunchRowsByKnownShapes(periodId);
    if (error) throw error;

    data.forEach((row) => mergeRow('dtrPunches', row));
    return data;
  },

  async loadDtrApprovalsByPeriod(periodId) {
    if (!periodId) return [];
    const { data, error } = await requireSupabaseClient()
      .from(TABLES.dtrApprovals)
      .select('*')
      .eq('payroll_period_id', periodId);
    if (error) throw error;
    data.forEach((row) => mergeRow('dtrApprovals', row));
    return data;
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

      const message = String(error?.message || '');
      const missingRpc = error.code === 'PGRST202' || /function .*assert_dtr_row_editable/i.test(message);
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

  async loadCoreReadModels({ periodId } = {}) {
    const loaders = Object.entries(TABLE_LOADERS).map(async ([loaderKey, { table, stateKey }]) => {
      const result = await loadRowsWithOptionalOptimizedFilter(table, periodId);
      if (result.error) {
        const isOptionalApprovals = loaderKey === 'dtrApprovals';
        if (isOptionalApprovals && isMissingTableError(result.error)) {
          console.warn('[payroll:read] dtr approvals table not found; continuing without approval rows', {
            table,
            code: result.error.code,
            message: result.error.message,
          });
          return { table, count: 0, skipped: true };
        }
        throw result.error;
      }

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
    if (!skipEditableCheck) {
      await this.ensureDtrRowEditableWithRpcOrFallback(getPunchRowContext({ periodId, employeeId, punchAt }));
    }
    const row = {
      payroll_period_id: periodId,
      employee_id: employeeId,
      project_id: projectId,
      punch_at: punchAt,
      meta,
    };

    return createRowWithLock({
      table: TABLES.punches,
      row,
      periodId,
      stateKey: 'dtrPunches',
    });
  },

  async updatePunch(punchId, patch, options = {}) {
    const state = getState();
    const existing = state.dtrPunches.get(punchId);
    if (!existing) throw new Error(`Punch ${punchId} not found in state.`);
    if (!options.skipEditableCheck) {
      const sourceContext = getPunchRowContext({
        periodId: existing.payroll_period_id,
        employeeId: existing.employee_id,
        punchAt: existing.punch_at || `${existing.date || ''} ${existing.time || ''}`,
      });
      const destinationContext = getPunchRowContext({
        periodId: patch?.payroll_period_id ?? existing.payroll_period_id,
        employeeId: patch?.employee_id ?? existing.employee_id,
        punchAt: patch?.punch_at ?? existing.punch_at ?? `${existing.date || ''} ${existing.time || ''}`,
      });

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
      patch,
      expectedUpdatedAt: existing.updated_at ?? null,
      periodId: existing.payroll_period_id,
      stateKey: 'dtrPunches',
    });
  },

  async deletePunch(punchId, options = {}) {
    const state = getState();
    const existing = state.dtrPunches.get(punchId);
    if (!existing) throw new Error(`Punch ${punchId} not found in state.`);
    if (!options.skipEditableCheck) {
      await this.ensureDtrRowEditableWithRpcOrFallback(getPunchRowContext({
        periodId: existing.payroll_period_id,
        employeeId: existing.employee_id,
        punchAt: existing.punch_at || `${existing.date || ''} ${existing.time || ''}`,
      }));
    }

    await deleteWithOptimisticLock({
      table: TABLES.punches,
      id: punchId,
      expectedUpdatedAt: existing.updated_at ?? null,
      periodId: existing.payroll_period_id,
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
      if (String(row.payroll_period_id) !== context.periodId) return false;
      if (String(row.employee_id) !== context.employeeId) return false;
      return deriveWorkDateFromPunchAt(row.punch_at || `${row.date || ''} ${row.time || ''}`) === context.workDate;
    });

    const existingQueue = existingRows.map((row) => ({
      row,
      time: deriveTimeFromPunchAt(row.punch_at, row.time),
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

  async setPeriodLock(periodId, isLocked) {
    const patch = { is_locked: !!isLocked, updated_at: new Date().toISOString() };
    logWrite('update', TABLES.periods, { id: periodId, ...patch });

    const { data, error } = await requireSupabaseClient()
      .from(TABLES.periods)
      .update(patch)
      .eq('id', periodId)
      .select('*')
      .single();

    if (error) throw error;
    mergeRow('payrollPeriods', data);
    return data;
  },

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
