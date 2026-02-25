# Payroll Refactor Architecture (Supabase-First, Multi-User Safe)

## Target module structure

```text
src/
  config/
    supabaseClient.js        # single Supabase client
  state/
    store.js                 # centralized in-memory state + merge helpers
  services/
    payrollService.js        # row-level data access with lock checks
  realtime/
    subscriptions.js         # postgres_changes subscriptions per table
  domain/
    payrollCalculations.js   # pure business calculations
  ui/
    payrollController.js     # render orchestration from central state
  main.js                    # app bootstrap
```

## Anti-patterns identified in current implementation

1. **Dual source of truth (`localStorage` + Supabase)**
   - The app writes business data to `localStorage` and mirrors to cloud.
   - This causes split-brain state and data reversion during concurrent edits.
2. **Monolithic file architecture**
   - `index.html` mixes UI rendering, persistence logic, migration logic, and business calculations.
   - Difficult to test and reason about lock safety.
3. **Bulk upserts and full-dataset writes**
   - Several paths rebuild arrays and upsert batches.
   - In multi-user scenarios, this can overwrite newer records.
4. **Hydration race conditions**
   - Local and remote merge logic races with user edits.
   - Refresh timing can drop changes.
5. **Lock enforcement is not centralized**
   - `is_locked` behavior exists, but mutations are not guaranteed to pass through lock checks.
6. **Realtime handlers are inconsistent**
   - Some flows refetch or rewrite entire collections instead of row-level state merge.

## Production-oriented decisions implemented

1. **Supabase is the only source of truth**
   - New architecture does not read/write business state to `localStorage`.
2. **Row-level writes only**
   - Service methods perform `insert/update/delete` by row id.
   - No full table replacement in core payroll mutation paths.
3. **Realtime through `postgres_changes`**
   - One channel per table.
   - Incoming payloads are merged/deleted in a central state map.
4. **Centralized lock guard**
   - Mutating payroll methods call `ensurePeriodUnlocked(periodId)` before writing.
5. **Immutable snapshots**
   - Snapshots are insert-only; updates throw explicit errors.
6. **Separation of concerns**
   - Data access (`services`) is isolated from domain calculations (`domain`) and UI rendering (`ui`).

## Critical section rewrites

### Realtime subscription (row-level merge)

- Implemented in `src/realtime/subscriptions.js`
- Uses:
  - `event='*'`
  - table-scoped channels
  - merge for insert/update
  - remove for delete

### Row-level update with lock enforcement

- Implemented in `src/services/payrollService.js::updatePunch`
- Flow:
  1. Read current row from state
  2. Check parent period lock from `payroll_periods`
  3. Execute `.update(...).eq('id', punchId)`
  4. Merge result into state

### State merge function

- Implemented in `src/state/store.js::mergeRow`
- Uses `Map` by primary id and shallow merge to preserve existing row fields.

### Service layer pattern

- `payrollService` owns all persistence concerns.
- UI code only calls service methods and reads from store.
- Domain math stays pure in `src/domain/payrollCalculations.js`.

## Migration notes

1. Keep legacy scripts temporarily for compatibility.
2. Move UI actions to call `payrollService` methods.
3. Remove localStorage write paths table-by-table.
4. Add RLS policies that enforce lock rules server-side for defense in depth.
