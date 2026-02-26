import { getSupabaseClient } from '../config/supabaseClient.js';
import { getState, mergeRow } from '../state/store.js';

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

  async createPunch({ periodId, employeeId, projectId, punchAt, meta = {} }) {
    await ensurePeriodUnlocked(periodId);
    const row = {
      payroll_period_id: periodId,
      employee_id: employeeId,
      project_id: projectId,
      punch_at: punchAt,
      meta,
    };

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
