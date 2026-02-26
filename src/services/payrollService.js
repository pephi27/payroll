import { getSupabaseClient } from '../config/supabaseClient.js';
import { getState, mergeRow } from '../state/store.js';

function summarizePayload(payload) {
  if (!payload || typeof payload !== 'object') return payload;
  const summary = {};
  for (const key of ['id', 'payroll_period_id', 'employee_id', 'project_id', 'created_at', 'updated_at']) {
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
  employees: { table: TABLES.employees, stateKey: 'employees' },
  projects: { table: TABLES.projects, stateKey: 'projects' },
  schedules: { table: TABLES.schedules, stateKey: 'schedules' },
  loans: { table: TABLES.loans, stateKey: 'loans' },
  loanDeductions: { table: TABLES.loanDeductions, stateKey: 'loanDeductions' },
  contribFlags: { table: TABLES.contribFlags, stateKey: 'contribFlags' },
  profiles: { table: TABLES.profiles, stateKey: 'profiles' },
};

function requireSupabaseClient() {
  const client = getSupabaseClient();
  if (!client) {
    throw new Error('Supabase client is not ready. Ensure the legacy bootstrap initialized window.supabase.');
  }
  return client;
}

async function ensurePeriodUnlocked(periodId) {
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

export const payrollService = {
  tables: TABLES,

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
    let query = requireSupabaseClient()
      .from(TABLES.punches)
      .select('*')
      .order('punch_at', { ascending: true });

    if (periodId) {
      query = query.eq('payroll_period_id', periodId);
    }

    const { data, error } = await query;
    if (error) throw error;
    data.forEach((row) => mergeRow('dtrPunches', row));
    return data;
  },

  async loadCoreReadModels({ periodId } = {}) {
    const loaders = Object.values(TABLE_LOADERS).map(async ({ table, stateKey }) => {
      const { data, error } = await requireSupabaseClient().from(table).select('*');
      if (error) throw error;
      data.forEach((row) => mergeRow(stateKey, row));
      return { table, count: data.length };
    });

    const punches = this.loadPunchesByPeriod(periodId);
    return Promise.all([...loaders, punches]);
  },

  async createPunch({ periodId, employeeId, projectId, punchAt, meta = {} }) {
    await ensurePeriodUnlocked(periodId);
    const row = {
      payroll_period_id: periodId,
      employee_id: employeeId,
      project_id: projectId,
      punch_at: punchAt,
      meta,
    };

    logWrite('insert', TABLES.punches, row);

    const { data, error } = await requireSupabaseClient()
      .from(TABLES.punches)
      .insert(row)
      .select('*')
      .single();

    if (error) throw error;
    mergeRow('dtrPunches', data);
    return data;
  },

  async updatePunch(punchId, patch) {
    const state = getState();
    const existing = state.dtrPunches.get(punchId);
    if (!existing) throw new Error(`Punch ${punchId} not found in state.`);

    await ensurePeriodUnlocked(existing.payroll_period_id);

    logWrite('update', TABLES.punches, { id: punchId, ...patch });

    const { data, error } = await requireSupabaseClient()
      .from(TABLES.punches)
      .update(patch)
      .eq('id', punchId)
      .select('*')
      .single();

    if (error) throw error;
    mergeRow('dtrPunches', data);
    return data;
  },

  async deletePunch(punchId) {
    const state = getState();
    const existing = state.dtrPunches.get(punchId);
    if (!existing) throw new Error(`Punch ${punchId} not found in state.`);

    await ensurePeriodUnlocked(existing.payroll_period_id);

    logWrite('delete', TABLES.punches, { id: punchId });

    const { error } = await requireSupabaseClient()
      .from(TABLES.punches)
      .delete()
      .eq('id', punchId);

    if (error) throw error;
    return { id: punchId };
  },

  async saveSnapshot(snapshot) {
    const { payroll_period_id: periodId } = snapshot;
    await ensurePeriodUnlocked(periodId);

    logWrite('insert', TABLES.snapshots, snapshot);

    const { data, error } = await requireSupabaseClient()
      .from(TABLES.snapshots)
      .insert(snapshot)
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
