# PayrollPro

This project is a browser-based payroll and attendance tool.

## Split/Override Sync

Split flags and per-day schedule or project overrides are saved both locally and in a Supabase table named `split_overrides`. On start up the app fetches this table and hydrates the in-memory data so your split settings travel with your account across devices.

If the network request fails or you're offline, the app gracefully falls back to local storage and keeps working. The next successful save will sync any changes back to Supabase.
