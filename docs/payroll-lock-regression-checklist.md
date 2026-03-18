# Payroll Lock/Unlock Regression Checklist

1. **Concurrent lock/unlock in two tabs**
   - Open the same payroll period in Tab A and Tab B.
   - Lock in Tab A.
   - Attempt lock/unlock in Tab B without reloading.
   - Expected: conflict error is shown; Tab B does not silently overwrite.

2. **Locked period write denial across clients**
   - Keep Tab A locked on a period.
   - In Tab B, try DTR create/update/delete and punch create/update/delete.
   - Expected: operation is denied with lock message; no rows are written.

3. **Unlock audit note is persisted**
   - Unlock a locked period and provide a note in the modal.
   - Query `payroll_period_lock_events` for that period.
   - Expected: latest `action = 'unlock'` row stores the note and reason.

4. **Period switching clears stale in-memory data**
   - Switch repeatedly between two periods with different DTR/punch datasets.
   - Expected: no stale rows remain from the previous period in DTR and punch views.

5. **Unlock when local lock mirror is missing**
   - Remove or stale out local `payrollLocks` entry for a locked period.
   - Trigger unlock for that same period.
   - Expected: remote unlock still runs and succeeds; local mirror is reconciled.

6. **Remote unlock failure leaves snapshot locked**
   - Simulate unlock RPC failure (e.g., stale conflict or missing RPC migration).
   - Attempt unlock.
   - Expected: snapshot in memory/UI remains locked/finalized and is not partially switched to draft.

7. **Rapid click hardening**
   - Click period selector quickly across several values.
   - Click lock/unlock buttons rapidly.
   - Expected: no duplicate concurrent period loads; UI recovers to accurate lock state.

8. **Stale `updated_at` conflict for lock changes**
   - Capture period `updated_at` from Tab A.
   - Change lock state in Tab B.
   - Retry in Tab A using stale value.
   - Expected: stale conflict error returned, no overwrite, and latest period state is reloaded.

9. **Period switch in-flight lock behavior for dynamic controls**
   - Start switching periods while new controls are inserted into `#payrollWrapper` (e.g., modal or async-rendered button).
   - Expected: newly-added controls are disabled until switch completes.

10. **Migration/RPC missing behavior**
   - Run app without `set_payroll_period_lock` migration.
   - Trigger lock/unlock.
   - Expected: explicit migration/RPC unavailable message is shown.
