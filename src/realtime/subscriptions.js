import { supabase } from '../config/supabaseClient.js';
import { mergeRow, removeRow } from '../state/store.js';

const TABLE_TO_STATE_KEY = {
  payroll_periods: 'payrollPeriods',
  payroll_period_snapshots: 'payrollSnapshots',
  dtr_punches: 'dtrPunches',
  pp_employees: 'employees',
  pp_projects: 'projects',
  pp_schedules: 'schedules',
  employee_loans: 'loans',
  loan_deductions: 'loanDeductions',
  pp_contrib_flags: 'contribFlags',
  profiles: 'profiles',
};

function handleChange(table, payload) {
  const stateKey = TABLE_TO_STATE_KEY[table];
  if (!stateKey) return;

  if (payload.eventType === 'DELETE') {
    removeRow(stateKey, payload.old?.id);
    return;
  }

  mergeRow(stateKey, payload.new);
}

export function startRealtimeSubscriptions() {
  const channels = Object.keys(TABLE_TO_STATE_KEY).map((table) => {
    return supabase
      .channel(`rt:${table}`)
      .on(
        'postgres_changes',
        { event: '*', schema: 'public', table },
        (payload) => handleChange(table, payload),
      )
      .subscribe();
  });

  return () => {
    channels.forEach((channel) => supabase.removeChannel(channel));
  };
}
