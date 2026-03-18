-- Repair mixed/legacy public.dtr_punches installations so live Supabase matches the
-- canonical service-layer schema while preserving read compatibility for old rows.

DO $$
BEGIN
  IF to_regclass('public.dtr_punches') IS NULL THEN
    RETURN;
  END IF;

  ALTER TABLE public.dtr_punches
    ADD COLUMN IF NOT EXISTS id text,
    ADD COLUMN IF NOT EXISTS payroll_period_id text,
    ADD COLUMN IF NOT EXISTS employee_id text,
    ADD COLUMN IF NOT EXISTS project_id text,
    ADD COLUMN IF NOT EXISTS punch_at timestamptz,
    ADD COLUMN IF NOT EXISTS meta jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS emp_id text,
    ADD COLUMN IF NOT EXISTS date date,
    ADD COLUMN IF NOT EXISTS time text,
    ADD COLUMN IF NOT EXISTS source text,
    ADD COLUMN IF NOT EXISTS data jsonb,
    ADD COLUMN IF NOT EXISTS updated_by text,
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
END;
$$;

UPDATE public.dtr_punches
SET meta = CASE
      WHEN meta IS NULL OR jsonb_typeof(meta) <> 'object' THEN '{}'::jsonb
      ELSE meta
    END
    || CASE
         WHEN data IS NOT NULL AND jsonb_typeof(data) = 'object' THEN data
         ELSE '{}'::jsonb
       END;

UPDATE public.dtr_punches
SET employee_id = COALESCE(
      NULLIF(btrim(COALESCE(employee_id, '')), ''),
      NULLIF(btrim(COALESCE(emp_id, '')), ''),
      NULLIF(btrim(COALESCE(meta->>'employee_id', '')), ''),
      NULLIF(btrim(COALESCE(meta->>'empId', '')), '')
    )
WHERE COALESCE(btrim(COALESCE(employee_id, '')), '') = '';

UPDATE public.dtr_punches
SET punch_at = COALESCE(
      punch_at,
      CASE
        WHEN date IS NOT NULL AND substring(COALESCE(time, '') FROM '^\d{2}:\d{2}') IS NOT NULL
          THEN timezone('UTC', (date + substring(COALESCE(time, '') FROM '^\d{2}:\d{2}')::time))
        ELSE NULL
      END,
      CASE
        WHEN meta->>'date' ~ '^\d{4}-\d{2}-\d{2}$' AND substring(COALESCE(meta->>'time', '') FROM '^\d{2}:\d{2}') IS NOT NULL
          THEN timezone('UTC', ((meta->>'date')::date + substring(COALESCE(meta->>'time', '') FROM '^\d{2}:\d{2}')::time))
        ELSE NULL
      END
    )
WHERE punch_at IS NULL;

UPDATE public.dtr_punches
SET payroll_period_id = COALESCE(
      NULLIF(btrim(COALESCE(payroll_period_id, '')), ''),
      NULLIF(btrim(COALESCE(meta->>'payroll_period_id', '')), ''),
      public.resolve_dtr_period_id_for_work_date(timezone('UTC', punch_at)::date, meta)
    )
WHERE punch_at IS NOT NULL
  AND employee_id IS NOT NULL
  AND COALESCE(btrim(COALESCE(payroll_period_id, '')), '') = '';

UPDATE public.dtr_punches
SET project_id = COALESCE(
      NULLIF(btrim(COALESCE(project_id, '')), ''),
      NULLIF(btrim(COALESCE(meta->>'project_id', '')), ''),
      NULLIF(btrim(COALESCE(meta->>'projectId', '')), '')
    )
WHERE project_id IS NULL OR btrim(project_id) = '';

UPDATE public.dtr_punches
SET meta = jsonb_strip_nulls(
      COALESCE(meta, '{}'::jsonb)
      || jsonb_build_object(
        'payroll_period_id', payroll_period_id,
        'empId', employee_id,
        'project_id', project_id,
        'date', CASE WHEN punch_at IS NOT NULL THEN to_char(timezone('UTC', punch_at), 'YYYY-MM-DD') ELSE NULL END,
        'time', CASE WHEN punch_at IS NOT NULL THEN to_char(timezone('UTC', punch_at), 'HH24:MI') ELSE NULL END,
        'source', COALESCE(NULLIF(meta->>'source', ''), NULLIF(btrim(COALESCE(source, '')), ''))
      )
    );

UPDATE public.dtr_punches
SET id = COALESCE(
      NULLIF(btrim(COALESCE(id, '')), ''),
      concat(
        payroll_period_id,
        '|',
        employee_id,
        '|',
        to_char(timezone('UTC', punch_at), 'YYYY-MM-DD"T"HH24:MI'),
        '|',
        COALESCE(NULLIF(meta->>'source', ''), '')
      )
    )
WHERE punch_at IS NOT NULL
  AND COALESCE(btrim(COALESCE(id, '')), '') = '';

WITH ranked AS (
  SELECT ctid,
         row_number() OVER (
           PARTITION BY id
           ORDER BY COALESCE(updated_at, created_at, now()) DESC, ctid DESC
         ) AS rn
  FROM public.dtr_punches
  WHERE NULLIF(btrim(COALESCE(id, '')), '') IS NOT NULL
)
DELETE FROM public.dtr_punches target
USING ranked
WHERE target.ctid = ranked.ctid
  AND ranked.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS dtr_punches_id_uidx
  ON public.dtr_punches (id);

CREATE INDEX IF NOT EXISTS dtr_punches_period_idx
  ON public.dtr_punches (payroll_period_id);

CREATE INDEX IF NOT EXISTS dtr_punches_period_employee_idx
  ON public.dtr_punches (payroll_period_id, employee_id);

CREATE INDEX IF NOT EXISTS dtr_punches_period_punch_at_idx
  ON public.dtr_punches (payroll_period_id, punch_at);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.dtr_punches
    WHERE id IS NULL OR btrim(id) = ''
       OR payroll_period_id IS NULL OR btrim(payroll_period_id) = ''
       OR employee_id IS NULL OR btrim(employee_id) = ''
       OR punch_at IS NULL
  ) THEN
    RAISE NOTICE 'public.dtr_punches still has mixed-schema rows that could not be fully canonicalized; leaving NOT NULL enforcement unchanged until those rows are repaired.';
  ELSE
    ALTER TABLE public.dtr_punches
      ALTER COLUMN id SET NOT NULL,
      ALTER COLUMN payroll_period_id SET NOT NULL,
      ALTER COLUMN employee_id SET NOT NULL,
      ALTER COLUMN punch_at SET NOT NULL,
      ALTER COLUMN meta SET NOT NULL;
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.dtr_punches'::regclass
      AND contype = 'p'
  ) AND NOT EXISTS (
    SELECT 1
    FROM public.dtr_punches
    WHERE id IS NULL OR btrim(id) = ''
  ) THEN
    ALTER TABLE public.dtr_punches
      ADD CONSTRAINT dtr_punches_pkey PRIMARY KEY USING INDEX dtr_punches_id_uidx;
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.trg_assert_dtr_punch_editable()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_old_period_id text;
  v_new_period_id text;
  v_old_work_date date;
  v_new_work_date date;
BEGIN
  IF tg_op = 'INSERT' THEN
    v_new_work_date := timezone('UTC', new.punch_at)::date;
    v_new_period_id := COALESCE(
      NULLIF(btrim(COALESCE(new.payroll_period_id, '')), ''),
      public.resolve_dtr_period_id_for_work_date(v_new_work_date, new.meta)
    );
    PERFORM public.assert_dtr_row_editable(v_new_period_id, new.employee_id, v_new_work_date);
    new.payroll_period_id := v_new_period_id;
    RETURN new;
  END IF;

  IF tg_op = 'DELETE' THEN
    v_old_work_date := timezone('UTC', old.punch_at)::date;
    v_old_period_id := COALESCE(
      NULLIF(btrim(COALESCE(old.payroll_period_id, '')), ''),
      public.resolve_dtr_period_id_for_work_date(v_old_work_date, old.meta)
    );
    PERFORM public.assert_dtr_row_editable(v_old_period_id, old.employee_id, v_old_work_date);
    RETURN old;
  END IF;

  IF tg_op = 'UPDATE' THEN
    v_old_work_date := timezone('UTC', old.punch_at)::date;
    v_old_period_id := COALESCE(
      NULLIF(btrim(COALESCE(old.payroll_period_id, '')), ''),
      public.resolve_dtr_period_id_for_work_date(v_old_work_date, old.meta)
    );
    PERFORM public.assert_dtr_row_editable(v_old_period_id, old.employee_id, v_old_work_date);

    v_new_work_date := timezone('UTC', new.punch_at)::date;
    v_new_period_id := COALESCE(
      NULLIF(btrim(COALESCE(new.payroll_period_id, '')), ''),
      public.resolve_dtr_period_id_for_work_date(v_new_work_date, new.meta)
    );

    IF v_new_period_id IS DISTINCT FROM v_old_period_id
      OR new.employee_id IS DISTINCT FROM old.employee_id
      OR v_new_work_date IS DISTINCT FROM v_old_work_date THEN
      PERFORM public.assert_dtr_row_editable(v_new_period_id, new.employee_id, v_new_work_date);
    END IF;

    new.payroll_period_id := v_new_period_id;
    RETURN new;
  END IF;

  RAISE EXCEPTION 'Unsupported DTR punch trigger operation: %', tg_op;
END;
$$;

DO $$
BEGIN
  EXECUTE 'drop trigger if exists dtr_punches_assert_editable_tg on public.dtr_punches';
  EXECUTE 'create trigger dtr_punches_assert_editable_tg
    before insert or update or delete on public.dtr_punches
    for each row execute function public.trg_assert_dtr_punch_editable()';
END;
$$;
