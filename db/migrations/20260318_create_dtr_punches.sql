-- Backfill-safe bootstrap for authoritative DTR punch storage.
-- This must exist before the 20260318 editability migration so older environments
-- stop falling back to legacy blob storage when `dtr_punches` is missing.

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
