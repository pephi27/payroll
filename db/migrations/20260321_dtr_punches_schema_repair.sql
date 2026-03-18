-- Repair legacy public.dtr_punches installations so the browser client can
-- use authoritative row-per-punch storage with `id`-based upserts.

DO $$
BEGIN
  IF to_regclass('public.dtr_punches') IS NULL THEN
    RETURN;
  END IF;

  ALTER TABLE public.dtr_punches
    ADD COLUMN IF NOT EXISTS id text,
    ADD COLUMN IF NOT EXISTS source text,
    ADD COLUMN IF NOT EXISTS updated_by text,
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
END;
$$;

UPDATE public.dtr_punches
SET time = COALESCE(substring(COALESCE(time, '') FROM '^\d{2}:\d{2}'), NULLIF(btrim(COALESCE(time, '')), ''))
WHERE COALESCE(time, '') <> ''
  AND COALESCE(substring(COALESCE(time, '') FROM '^\d{2}:\d{2}'), NULLIF(btrim(COALESCE(time, '')), '')) IS DISTINCT FROM time;

UPDATE public.dtr_punches
SET source = NULLIF(
      COALESCE(
        NULLIF(btrim(COALESCE(source, '')), ''),
        NULLIF(btrim(COALESCE(data->>'source', '')), ''),
        CASE WHEN COALESCE((data->>'manual')::boolean, false) THEN 'manual' ELSE '' END
      ),
      ''
    )
WHERE source IS NULL
   OR btrim(source) = '';

UPDATE public.dtr_punches
SET data =
  CASE
    WHEN jsonb_typeof(data) = 'object' THEN data
    ELSE '{}'::jsonb
  END
  || jsonb_strip_nulls(
      jsonb_build_object(
        'empId', NULLIF(btrim(COALESCE(emp_id, '')), ''),
        'date', CASE WHEN date IS NOT NULL THEN to_char(date, 'YYYY-MM-DD') ELSE NULL END,
        'time', NULLIF(btrim(COALESCE(time, '')), ''),
        'source', NULLIF(btrim(COALESCE(source, '')), '')
      )
    )
WHERE data IS NULL
   OR jsonb_typeof(data) <> 'object'
   OR data->>'empId' IS DISTINCT FROM NULLIF(btrim(COALESCE(emp_id, '')), '')
   OR data->>'date' IS DISTINCT FROM CASE WHEN date IS NOT NULL THEN to_char(date, 'YYYY-MM-DD') ELSE NULL END
   OR data->>'time' IS DISTINCT FROM NULLIF(btrim(COALESCE(time, '')), '')
   OR COALESCE(data->>'source', '') IS DISTINCT FROM COALESCE(NULLIF(btrim(COALESCE(source, '')), ''), '');

UPDATE public.dtr_punches
SET id = concat(
    NULLIF(btrim(COALESCE(emp_id, '')), ''),
    '|',
    CASE WHEN date IS NOT NULL THEN to_char(date, 'YYYY-MM-DD') ELSE '' END,
    '|',
    NULLIF(btrim(COALESCE(time, '')), ''),
    '|',
    COALESCE(NULLIF(btrim(COALESCE(source, '')), ''), CASE WHEN COALESCE((data->>'manual')::boolean, false) THEN 'manual' ELSE '' END)
  )
WHERE COALESCE(btrim(id), '') = ''
  AND NULLIF(btrim(COALESCE(emp_id, '')), '') IS NOT NULL
  AND date IS NOT NULL
  AND NULLIF(btrim(COALESCE(time, '')), '') IS NOT NULL;

UPDATE public.dtr_punches
SET id = NULL
WHERE COALESCE(btrim(id), '') = '';

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

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.dtr_punches'::regclass
      AND contype = 'p'
  ) THEN
    RETURN;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.dtr_punches
    WHERE id IS NULL OR btrim(id) = ''
  ) THEN
    RAISE NOTICE 'public.dtr_punches still has rows without computed id values; leaving primary key unset until those rows are repaired.';
    RETURN;
  END IF;

  ALTER TABLE public.dtr_punches
    ALTER COLUMN id SET NOT NULL;

  ALTER TABLE public.dtr_punches
    ADD CONSTRAINT dtr_punches_pkey PRIMARY KEY USING INDEX dtr_punches_id_uidx;
END;
$$;
