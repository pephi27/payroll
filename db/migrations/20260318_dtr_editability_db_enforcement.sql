-- Phase 2 hardening: enforce DTR row editability at the database layer.

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

  return;
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

  return;
end;
$$;

create or replace function public.extract_dtr_work_date_from_punch_at(
  p_punch_at text
)
returns date
language plpgsql
immutable
as $$
declare
  v_iso_date text;
begin
  if p_punch_at is null or btrim(p_punch_at) = '' then
    raise exception 'Missing punch timestamp. Update denied.';
  end if;

  v_iso_date := substring(btrim(p_punch_at) from '^(\d{4}-\d{2}-\d{2})');

  if v_iso_date is null then
    raise exception 'Invalid punch timestamp format (expected YYYY-MM-DD...). Update denied: %', p_punch_at;
  end if;

  return v_iso_date::date;
end;
$$;

create or replace function public.trg_assert_dtr_punch_editable()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_old_work_date date;
  v_new_work_date date;
begin
  if tg_op = 'INSERT' then
    v_new_work_date := public.extract_dtr_work_date_from_punch_at(new.punch_at::text);
    perform public.assert_dtr_row_editable(new.payroll_period_id, new.employee_id, v_new_work_date);
    return new;
  end if;

  if tg_op = 'DELETE' then
    v_old_work_date := public.extract_dtr_work_date_from_punch_at(old.punch_at::text);
    perform public.assert_dtr_row_editable(old.payroll_period_id, old.employee_id, v_old_work_date);
    return old;
  end if;

  if tg_op = 'UPDATE' then
    v_old_work_date := public.extract_dtr_work_date_from_punch_at(old.punch_at::text);
    perform public.assert_dtr_row_editable(old.payroll_period_id, old.employee_id, v_old_work_date);

    v_new_work_date := public.extract_dtr_work_date_from_punch_at(new.punch_at::text);
    if new.payroll_period_id is distinct from old.payroll_period_id
      or new.employee_id is distinct from old.employee_id
      or v_new_work_date is distinct from v_old_work_date then
      perform public.assert_dtr_row_editable(new.payroll_period_id, new.employee_id, v_new_work_date);
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
