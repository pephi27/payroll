# PayrollPro

This project is a browser-based payroll and attendance tool.

## Split/Override Sync

Split flags and per-day schedule or project overrides are saved both locally and in a Supabase table named `split_overrides`. On start up the app fetches this table and hydrates the in-memory data so your split settings travel with your account across devices.

If the network request fails or you're offline, the app gracefully falls back to local storage and keeps working. The next successful save will sync any changes back to Supabase.

## Configuration

Supabase credentials are injected at build time and are not stored in the repository.

1. Set the environment variables `SUPABASE_URL` and `SUPABASE_ANON_KEY`.
2. Run `npm run build:config` to generate `config.js` with these values.
3. Serve `index.html` normally. The generated script exposes the credentials to the browser as `window.SUPABASE_URL` and `window.SUPABASE_ANON_KEY`.

In production deployments the same build step can be run during CI/CD, or a server-side endpoint can return a `config.js` script that sets these global variables.
