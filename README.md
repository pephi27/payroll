# Payroll Backup and Restore

This project contains a simple browser-based payroll utility with Supabase integration.

## Restoring from Supabase Storage

1. Open the Payroll panel.
2. Click **List Backups** to fetch available backup files from the Supabase `backups` bucket.
3. Choose a file from the dropdown that appears.
4. Press **Restore Selected** to download the file and apply it using the existing restore logic.

You can still use **Restore Bundle** to load a local `.json` backup file manually.
