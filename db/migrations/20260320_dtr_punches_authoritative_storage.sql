-- Ensure authoritative DTR punch storage exists for Supabase-first refresh/bootstrap,
-- backfill from legacy blob storage, and correct the period-resolution function on live DBs.

create table if not exists public.dtr_punches (
  id text primary key,
  emp_id text not null,
  date date not null,
  time text not null,
  source text null,
  data jsonb not null default '{}'::jsonb,
  updated_by text null,
  updated_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create index if not exists dtr_punches_date_idx
  on public.dtr_punches (date);

create index if not exists dtr_punches_emp_date_idx
  on public.dtr_punches (emp_id, date);

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

do $$
begin
  if to_regclass('public.dtr_punches') is not null
    and to_regprocedure('public.trg_assert_dtr_punch_editable()') is not null then
    execute 'drop trigger if exists dtr_punches_assert_editable_tg on public.dtr_punches';
    execute 'create trigger dtr_punches_assert_editable_tg
      before insert or update or delete on public.dtr_punches
      for each row execute function public.trg_assert_dtr_punch_editable()';
  end if;
end;
$$;

do $$
declare
  v_payload jsonb := '[]'::jsonb;
begin
  if to_regclass('public.dtr_records') is null then
    return;
  end if;

  select case
    when jsonb_typeof(data) = 'array' then data
    when jsonb_typeof(data) = 'object' and jsonb_typeof(data->'records') = 'array' then data->'records'
    when jsonb_typeof(data) = 'object' and jsonb_typeof(data->'data') = 'array' then data->'data'
    else '[]'::jsonb
  end
    into v_payload
  from public.dtr_records
  where id = 'records'
  limit 1;

  if coalesce(jsonb_typeof(v_payload), '') <> 'array' then
    return;
  end if;

  insert into public.dtr_punches (
    id,
    emp_id,
    date,
    time,
    source,
    data,
    updated_by,
    updated_at,
    created_at
  )
  select
    concat(
      trim(coalesce(rec->>'empId', '')),
      '|',
      rec->>'date',
      '|',
      substring(coalesce(rec->>'time', '') from '^\d{2}:\d{2}'),
      '|',
      coalesce(nullif(rec->>'source', ''), case when coalesce((rec->>'manual')::boolean, false) then 'manual' else '' end)
    ) as id,
    trim(coalesce(rec->>'empId', '')) as emp_id,
    (rec->>'date')::date as date,
    substring(coalesce(rec->>'time', '') from '^\d{2}:\d{2}') as time,
    nullif(coalesce(rec->>'source', case when coalesce((rec->>'manual')::boolean, false) then 'manual' else '' end), '') as source,
    rec as data,
    'db_migration' as updated_by,
    now() as updated_at,
    now() as created_at
  from jsonb_array_elements(v_payload) as legacy(rec)
  where trim(coalesce(rec->>'empId', '')) <> ''
    and coalesce(rec->>'date', '') ~ '^\d{4}-\d{2}-\d{2}$'
    and substring(coalesce(rec->>'time', '') from '^\d{2}:\d{2}') is not null
  on conflict (id) do update
    set emp_id = excluded.emp_id,
        date = excluded.date,
        time = excluded.time,
        source = excluded.source,
        data = excluded.data,
        updated_by = excluded.updated_by,
        updated_at = excluded.updated_at
  where public.dtr_punches.data is distinct from excluded.data
     or public.dtr_punches.emp_id is distinct from excluded.emp_id
     or public.dtr_punches.date is distinct from excluded.date
     or public.dtr_punches.time is distinct from excluded.time
     or public.dtr_punches.source is distinct from excluded.source;
end;
$$;
