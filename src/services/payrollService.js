import { getSupabaseClient } from '../config/supabaseClient.js';
import { getFeatureFlag } from '../config/featureFlags.js';
import { getState, mergeRow, reportConflict } from '../state/store.js';

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
  profiles: { table: TABLES.profiles, stateKey: 'profiles' },
};


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

function applyExpectedUpdatedAt(query, expectedUpdatedAt) {
  if (expectedUpdatedAt == null) {
    return query.is('updated_at', null);
  }
  return query.eq('updated_at', expectedUpdatedAt);
}

async function updateWithOptimisticLock({ table, id, patch, expectedUpdatedAt, periodId, stateKey }) {
  await ensurePeriodUnlocked(periodId);
  logWrite('update', table, { id, ...patch, expected_updated_at: expectedUpdatedAt });

  let query = requireSupabaseClient()
    .from(table)
    .update({ ...patch, updated_at: new Date().toISOString() })
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
  await ensurePeriodUnlocked(periodId);
  logWrite('insert', table, row);
  const payload = { ...row, updated_at: new Date().toISOString() };

  const { data, error } = await requireSupabaseClient()
    .from(table)
    .insert(payload)
    .select('*')
    .single();

  if (error) throw error;
  mergeRow(stateKey, data);
  return data;
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

  async loadCoreReadModels({ periodId } = {}) {
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

  async createPunch({ periodId, employeeId, projectId, punchAt, meta = {} }) {
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

  async updatePunch(punchId, patch) {
    const state = getState();
    const existing = state.dtrPunches.get(punchId);
    if (!existing) throw new Error(`Punch ${punchId} not found in state.`);

    return updateWithOptimisticLock({
      table: TABLES.punches,
      id: punchId,
      patch,
      expectedUpdatedAt: existing.updated_at ?? null,
      periodId: existing.payroll_period_id,
      stateKey: 'dtrPunches',
    });
  },

  async deletePunch(punchId) {
    const state = getState();
    const existing = state.dtrPunches.get(punchId);
    if (!existing) throw new Error(`Punch ${punchId} not found in state.`);

    await ensurePeriodUnlocked(existing.payroll_period_id);
    logWrite('delete', TABLES.punches, { id: punchId });

    let query = requireSupabaseClient().from(TABLES.punches).delete().eq('id', punchId);
    query = applyExpectedUpdatedAt(query, existing.updated_at ?? null);

    const { data, error } = await query.select('id').maybeSingle();
    if (error) throw error;

    if (!data) {
      const conflict = { table: TABLES.punches, id: punchId, expectedUpdatedAt: existing.updated_at ?? null, at: new Date().toISOString() };
      reportConflict(conflict);
      throw new StaleWriteError('This record was updated by another user. Reloading.', conflict);
    }

    return { id: punchId };
  },

  async upsertEmployee(employeeId, patch) {
    const state = getState();
    const existing = state.employees.get(employeeId);
    if (existing) {
      return updateWithOptimisticLock({
        table: TABLES.employees,
        id: employeeId,
        patch,
        expectedUpdatedAt: existing.updated_at ?? null,
        stateKey: 'employees',
      });
    }

    return createRowWithLock({
      table: TABLES.employees,
      row: { id: employeeId, ...patch },
      stateKey: 'employees',
    });
  },

  async upsertLoan(loanId, patch) {
    const state = getState();
    const existing = state.loans.get(loanId);
    const periodId = patch.payroll_period_id ?? existing?.payroll_period_id ?? getState().currentPeriodId;

    if (existing) {
      return updateWithOptimisticLock({
        table: TABLES.loans,
        id: loanId,
        patch,
        expectedUpdatedAt: existing.updated_at ?? null,
        periodId,
        stateKey: 'loans',
      });
    }

    return createRowWithLock({
      table: TABLES.loans,
      row: { id: loanId, ...patch },
      periodId,
      stateKey: 'loans',
    });
  },

  async upsertLoanDeduction(deductionId, patch) {
    const state = getState();
    const existing = state.loanDeductions.get(deductionId);
    const periodId = patch.payroll_period_id ?? existing?.payroll_period_id ?? getState().currentPeriodId;

    if (existing) {
      return updateWithOptimisticLock({
        table: TABLES.loanDeductions,
        id: deductionId,
        patch,
        expectedUpdatedAt: existing.updated_at ?? null,
        periodId,
        stateKey: 'loanDeductions',
      });
    }

    return createRowWithLock({
      table: TABLES.loanDeductions,
      row: { id: deductionId, ...patch },
      periodId,
      stateKey: 'loanDeductions',
    });
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
    await ensurePeriodUnlocked(periodId);

    logWrite('insert', TABLES.snapshots, snapshot);

    const { data, error } = await requireSupabaseClient()
      .from(TABLES.snapshots)
      .insert({ ...snapshot, created_at: new Date().toISOString() })
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
