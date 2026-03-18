-- Phase 3 hardening: atomic lock/unlock with optimistic conflict checks and audit history.

create table if not exists public.payroll_period_lock_events (
  id bigserial primary key,
  payroll_period_id text not null references public.payroll_periods(id) on delete cascade,
  action text not null,
  note text not null default '',
  reason text not null default '',
  actor_id text null,
  created_at timestamptz not null default now()
);

create index if not exists payroll_period_lock_events_period_idx
  on public.payroll_period_lock_events (payroll_period_id, created_at desc);

create or replace function public.set_payroll_period_lock(
  p_period_id text,
  p_is_locked boolean,
  p_expected_updated_at timestamptz default null,
  p_note text default '',
  p_reason text default '',
  p_actor_id text default null
)
returns public.payroll_periods
language plpgsql
security definer
set search_path = public
as $$
declare
  v_row public.payroll_periods%rowtype;
  v_now timestamptz := now();
  v_action text;
begin
  if p_period_id is null or btrim(p_period_id) = '' then
    raise exception using
      errcode = 'P0001',
      message = 'Missing payroll period id for lock update.';
  end if;

  select *
    into v_row
  from public.payroll_periods
  where id::text = p_period_id
  for update;

  if not found then
    raise exception using
      errcode = 'P0001',
      message = format('Payroll period %s not found.', p_period_id);
  end if;

  if p_expected_updated_at is not null and v_row.updated_at is distinct from p_expected_updated_at then
    raise exception using
      errcode = 'P0001',
      message = format(
        'Stale payroll period lock write for %s. Expected updated_at %s but found %s.',
        p_period_id,
        p_expected_updated_at,
        coalesce(v_row.updated_at::text, 'null')
      );
  end if;

  update public.payroll_periods
  set
    is_locked = coalesce(p_is_locked, false),
    updated_at = v_now
  where id::text = p_period_id
  returning * into v_row;

  v_action := case when coalesce(p_is_locked, false) then 'lock' else 'unlock' end;

  insert into public.payroll_period_lock_events (
    payroll_period_id,
    action,
    note,
    reason,
    actor_id,
    created_at
  )
  values (
    p_period_id,
    v_action,
    coalesce(p_note, ''),
    coalesce(p_reason, ''),
    p_actor_id,
    v_now
  );

  return v_row;
end;
$$;
