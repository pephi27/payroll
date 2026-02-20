# Push diagnosis notes

## Most likely problematic push

Based on recent history, the most likely push that introduced the current payroll period sync issue is:

- **PR #702 / commit `b4bbded`** — "Fix non-UUID period sync queries and cloud period upserts"

Why this stands out:

1. It was merged and then quickly reverted by **PR #704 / commit `84b817c`**.
2. The patch touched critical sync paths (`pollPeriodSync` query logic + cloud upsert behavior) in `index.html`.
3. The previous stable behavior (`eq('id', periodId)` query) assumes UUID-like IDs, while this app uses `start|end` identifiers from `periodIdFromRange`.

## Better solution than full revert

Instead of fully reverting #702, apply a focused fix:

1. Keep non-UUID-safe reads for `pollPeriodSync` (query by `period_start` + `period_end` when period id is not UUID-like).
2. Keep upserts schema-tolerant:
   - primary attempt with `period_start, period_end` conflict target
   - fallback minimal payload for older schemas
3. Add a tiny probe cache so polling does not spam failing queries repeatedly.
4. Add a console warning once + temporary backoff (e.g., 2 minutes) on repeated schema failures.

This keeps compatibility with both newer and older `payroll_periods` schemas, while reducing error noise.

## Fast verification commands

Run this in your local branch after implementing the focused fix:

```bash
git log --oneline --decorate -n 12
```

```bash
git show --stat b4bbded
```

```bash
git show --stat 84b817c
```

```bash
# If you have automated tests:
# npm test
# or your project-specific test command
```

## Optional bisect recipe

If you want to prove the exact regression point:

```bash
git bisect start
git bisect bad HEAD
git bisect good 6f112fa
# run your reproduction check each step
# mark each candidate with: git bisect good | git bisect bad
git bisect reset
```
