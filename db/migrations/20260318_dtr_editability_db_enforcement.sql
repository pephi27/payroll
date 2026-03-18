-- Phase 2 hardening: enforce DTR row editability at the database layer
-- using the current legacy dtr_punches schema (emp_id/date/time/data).

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

create or replace function public.assert_payroll_period_unlocked(
  p_period_id text
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_locked boolean;
begin
  if p_period_id is null or btrim(p_period_id) = '' then
    raise exception 'Missing payroll period id. Update denied.';
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
end;
$$;

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
  v_is_approved boolean;
begin
  if p_period_id is null or btrim(p_period_id) = '' then
    raise exception 'Missing payroll period id for DTR edit. Update denied.';
  end if;

  if p_employee_id is null or btrim(p_employee_id) = '' then
    raise exception 'Missing employee id for DTR edit. Update denied.';
  end if;

  if p_work_date is null then
    raise exception 'Missing work date for DTR edit. Update denied.';
  end if;

  perform public.assert_payroll_period_unlocked(p_period_id);

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
end;
$$;

create or replace function public.resolve_dtr_period_id_for_work_date(
  p_work_date date,
  p_data jsonb default '{}'::jsonb
)
returns text
language plpgsql
security definer
set search_path = public
as $$
declare
  v_meta_period_id text;
  v_match_count integer;
  v_period_id text;
begin
  if p_work_date is null then
    raise exception 'Missing work date for DTR period resolution. Update denied.';
  end if;

  v_meta_period_id := nullif(btrim(coalesce(p_data->>'payroll_period_id', '')), '');

  if v_meta_period_id is not null then
    perform 1
    from public.payroll_periods
    where id = v_meta_period_id
      and p_work_date between period_start and period_end
    limit 1;

    if found then
      return v_meta_period_id;
    end if;

    raise exception 'DTR punch metadata period % does not match work date %. Update denied.', v_meta_period_id, p_work_date;
  end if;

  select count(*)
    into v_match_count
  from public.payroll_periods
  where p_work_date between period_start and period_end;

  if v_match_count = 1 then
    select id
      into v_period_id
    from public.payroll_periods
    where p_work_date between period_start and period_end
    order by period_start asc, id::text asc
    limit 1;

    return v_period_id;
  end if;

  if v_match_count = 0 then
    raise exception 'No payroll period covers work date %. Update denied.', p_work_date;
  end if;

  raise exception 'Multiple payroll periods cover work date %. Update denied.', p_work_date;
end;
$$;

create or replace function public.trg_assert_dtr_punch_editable()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_old_period_id text;
  v_new_period_id text;
begin
  if tg_op = 'INSERT' then
    v_new_period_id := public.resolve_dtr_period_id_for_work_date(new.date, new.data);
    perform public.assert_dtr_row_editable(v_new_period_id, new.emp_id, new.date);
    return new;
  end if;

  if tg_op = 'DELETE' then
    v_old_period_id := public.resolve_dtr_period_id_for_work_date(old.date, old.data);
    perform public.assert_dtr_row_editable(v_old_period_id, old.emp_id, old.date);
    return old;
  end if;

  if tg_op = 'UPDATE' then
    v_old_period_id := public.resolve_dtr_period_id_for_work_date(old.date, old.data);
    perform public.assert_dtr_row_editable(v_old_period_id, old.emp_id, old.date);

    v_new_period_id := public.resolve_dtr_period_id_for_work_date(new.date, new.data);
    if v_new_period_id is distinct from v_old_period_id
      or new.emp_id is distinct from old.emp_id
      or new.date is distinct from old.date then
      perform public.assert_dtr_row_editable(v_new_period_id, new.emp_id, new.date);
    end if;

    return new;
  end if;

  raise exception 'Unsupported DTR punch trigger operation: %', tg_op;
end;
$$;

create or replace function public.trg_assert_dtr_approval_period_unlocked()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if tg_op = 'INSERT' then
    perform public.assert_payroll_period_unlocked(new.payroll_period_id);
    return new;
  end if;

  if tg_op = 'DELETE' then
    perform public.assert_payroll_period_unlocked(old.payroll_period_id);
    return old;
  end if;

  if tg_op = 'UPDATE' then
    perform public.assert_payroll_period_unlocked(old.payroll_period_id);
    if new.payroll_period_id is distinct from old.payroll_period_id then
      perform public.assert_payroll_period_unlocked(new.payroll_period_id);
    end if;
    return new;
  end if;

  raise exception 'Unsupported DTR approval trigger operation: %', tg_op;
end;
$$;

do $$
begin
  if to_regclass('public.dtr_punches') is not null then
    execute 'drop trigger if exists dtr_punches_assert_editable_tg on public.dtr_punches';
    execute 'create trigger dtr_punches_assert_editable_tg
      before insert or update or delete on public.dtr_punches
      for each row execute function public.trg_assert_dtr_punch_editable()';
  end if;
end;
$$;

do $$
begin
  if to_regclass('public.pp_dtr_approvals') is not null then
    execute 'drop trigger if exists pp_dtr_approvals_assert_period_unlocked_tg on public.pp_dtr_approvals';
    execute 'create trigger pp_dtr_approvals_assert_period_unlocked_tg
      before insert or update or delete on public.pp_dtr_approvals
      for each row execute function public.trg_assert_dtr_approval_period_unlocked()';
  end if;
end;
$$;
