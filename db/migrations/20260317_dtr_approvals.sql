-- DTR approval model + defense-in-depth editability assertion.

create table if not exists public.pp_dtr_approvals (
  id text primary key,
  payroll_period_id text not null,
  employee_id text not null,
  work_date date not null,
  is_approved boolean not null default true,
  approved_at timestamptz null,
  approved_by text null,
  note text null,
  meta jsonb not null default '{}'::jsonb,
  updated_at timestamptz null,
  created_at timestamptz not null default now()
);

create unique index if not exists pp_dtr_approvals_period_employee_workdate_uidx
  on public.pp_dtr_approvals (payroll_period_id, employee_id, work_date);

create or replace function public.assert_dtr_row_editable(
  p_period_id text,
  p_employee_id text,
  p_work_date date
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_locked boolean;
  v_is_approved boolean;
begin
  if p_period_id is null or p_employee_id is null or p_work_date is null then
    raise exception 'Missing DTR edit context (period, employee, work_date).';
  end if;

  select is_locked
    into v_locked
  from public.payroll_periods
  where id = p_period_id
  limit 1;

  if not found then
    raise exception 'Payroll period % not found. Update denied.', p_period_id;
  end if;

  if coalesce(v_locked, false) then
    raise exception 'Payroll period % is locked. Update denied.', p_period_id;
  end if;

  select is_approved
    into v_is_approved
  from public.pp_dtr_approvals
  where payroll_period_id = p_period_id
    and employee_id = p_employee_id
    and work_date = p_work_date
  limit 1;

  if coalesce(v_is_approved, false) then
    raise exception 'DTR row for employee % on % is approved. Update denied.', p_employee_id, p_work_date;
  end if;

  return;
end;
$$;
