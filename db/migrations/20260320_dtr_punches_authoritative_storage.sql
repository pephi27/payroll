-- Canonical authoritative DTR punch storage.
-- Fresh installs should create public.dtr_punches in the service-layer row-per-punch shape,
-- then backfill from legacy blob storage without reintroducing legacy writes.

do $$
declare
  v_period_id_type text;
begin
  select format_type(a.atttypid, a.atttypmod)
    into v_period_id_type
  from pg_attribute a
  where a.attrelid = 'public.payroll_periods'::regclass
    and a.attname = 'id'
    and a.attnum > 0
    and not a.attisdropped;

  if v_period_id_type is null then
    raise exception 'public.payroll_periods.id type could not be resolved.';
  end if;

  if to_regclass('public.dtr_punches') is null then
    execute format($fmt$
      create table public.dtr_punches (
        id text primary key,
        payroll_period_id %s not null references public.payroll_periods(id) on delete cascade,
        employee_id text not null,
        project_id text null,
        punch_at timestamptz not null,
        meta jsonb not null default '{}'::jsonb,
        updated_by text null,
        updated_at timestamptz not null default now(),
        created_at timestamptz not null default now()
      )
    $fmt$, v_period_id_type);
  end if;
end;
$$;

create index if not exists dtr_punches_period_idx
  on public.dtr_punches (payroll_period_id);

create index if not exists dtr_punches_period_employee_idx
  on public.dtr_punches (payroll_period_id, employee_id);

create index if not exists dtr_punches_period_punch_at_idx
  on public.dtr_punches (payroll_period_id, punch_at);

create or replace function public.resolve_dtr_period_id_for_work_date(
  p_work_date date,
  p_meta jsonb default '{}'::jsonb
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

  v_meta_period_id := nullif(btrim(coalesce(p_meta->>'payroll_period_id', '')), '');

  if v_meta_period_id is not null then
    perform 1
    from public.payroll_periods
    where id::text = v_meta_period_id
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
    select id::text
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
  v_old_work_date date;
  v_new_work_date date;
begin
  if tg_op = 'INSERT' then
    v_new_work_date := timezone('UTC', new.punch_at)::date;
    v_new_period_id := coalesce(
      nullif(btrim(coalesce(new.payroll_period_id::text, '')), ''),
      public.resolve_dtr_period_id_for_work_date(v_new_work_date, new.meta)
    );
    perform public.assert_dtr_row_editable(v_new_period_id, new.employee_id, v_new_work_date);
    new.payroll_period_id := v_new_period_id;
    return new;
  end if;

  if tg_op = 'DELETE' then
    v_old_work_date := timezone('UTC', old.punch_at)::date;
    v_old_period_id := coalesce(
      nullif(btrim(coalesce(old.payroll_period_id::text, '')), ''),
      public.resolve_dtr_period_id_for_work_date(v_old_work_date, old.meta)
    );
    perform public.assert_dtr_row_editable(v_old_period_id, old.employee_id, v_old_work_date);
    return old;
  end if;

  if tg_op = 'UPDATE' then
    v_old_work_date := timezone('UTC', old.punch_at)::date;
    v_old_period_id := coalesce(
      nullif(btrim(coalesce(old.payroll_period_id::text, '')), ''),
      public.resolve_dtr_period_id_for_work_date(v_old_work_date, old.meta)
    );
    perform public.assert_dtr_row_editable(v_old_period_id, old.employee_id, v_old_work_date);

    v_new_work_date := timezone('UTC', new.punch_at)::date;
    v_new_period_id := coalesce(
      nullif(btrim(coalesce(new.payroll_period_id::text, '')), ''),
      public.resolve_dtr_period_id_for_work_date(v_new_work_date, new.meta)
    );

    if v_new_period_id is distinct from v_old_period_id
      or new.employee_id is distinct from old.employee_id
      or v_new_work_date is distinct from v_old_work_date then
      perform public.assert_dtr_row_editable(v_new_period_id, new.employee_id, v_new_work_date);
    end if;

    new.payroll_period_id := v_new_period_id;
    return new;
  end if;

  raise exception 'Unsupported DTR punch trigger operation: %', tg_op;
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
    payroll_period_id,
    employee_id,
    project_id,
    punch_at,
    meta,
    updated_by,
    updated_at,
    created_at
  )
  select
    concat(
      public.resolve_dtr_period_id_for_work_date((rec->>'date')::date, rec),
      '|',
      trim(coalesce(rec->>'empId', '')),
      '|',
      rec->>'date',
      'T',
      substring(coalesce(rec->>'time', '') from '^\d{2}:\d{2}'),
      '|',
      coalesce(nullif(rec->>'source', ''), case when coalesce((rec->>'manual')::boolean, false) then 'manual' else '' end)
    ) as id,
    public.resolve_dtr_period_id_for_work_date((rec->>'date')::date, rec) as payroll_period_id,
    trim(coalesce(rec->>'empId', '')) as employee_id,
    nullif(coalesce(rec->>'project_id', rec->>'projectId', ''), '') as project_id,
    timezone('UTC', ((rec->>'date')::date + substring(coalesce(rec->>'time', '') from '^\d{2}:\d{2}')::time)) as punch_at,
    jsonb_strip_nulls(
      rec
      || jsonb_build_object(
        'empId', trim(coalesce(rec->>'empId', '')),
        'payroll_period_id', public.resolve_dtr_period_id_for_work_date((rec->>'date')::date, rec),
        'project_id', nullif(coalesce(rec->>'project_id', rec->>'projectId', ''), ''),
        'date', rec->>'date',
        'time', substring(coalesce(rec->>'time', '') from '^\d{2}:\d{2}'),
        'source', nullif(coalesce(rec->>'source', case when coalesce((rec->>'manual')::boolean, false) then 'manual' else '' end), '')
      )
    ) as meta,
    'db_migration' as updated_by,
    now() as updated_at,
    now() as created_at
  from jsonb_array_elements(v_payload) as legacy(rec)
  where trim(coalesce(rec->>'empId', '')) <> ''
    and coalesce(rec->>'date', '') ~ '^\d{4}-\d{2}-\d{2}$'
    and substring(coalesce(rec->>'time', '') from '^\d{2}:\d{2}') is not null
  on conflict (id) do update
    set payroll_period_id = excluded.payroll_period_id,
        employee_id = excluded.employee_id,
        project_id = excluded.project_id,
        punch_at = excluded.punch_at,
        meta = excluded.meta,
        updated_by = excluded.updated_by,
        updated_at = excluded.updated_at
  where public.dtr_punches.meta is distinct from excluded.meta
     or public.dtr_punches.payroll_period_id is distinct from excluded.payroll_period_id
     or public.dtr_punches.employee_id is distinct from excluded.employee_id
     or public.dtr_punches.project_id is distinct from excluded.project_id
     or public.dtr_punches.punch_at is distinct from excluded.punch_at;
end;
$$;
