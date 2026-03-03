import { createClient } from 'https://esm.sh/@supabase/supabase-js@2?bundle';

function isSupabaseClient(client) {
  return !!(client && typeof client.from === 'function' && typeof client.channel === 'function');
}

let cachedSupabaseClient = null;

const DEFAULT_SUPABASE_URL = 'https://qzkzugzfpegozpiqutdv.supabase.co';
const DEFAULT_SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF6a3p1Z3pmcGVnb3pwaXF1dGR2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTU4MTc5MDMsImV4cCI6MjA3MTM5MzkwM30.mdFYuFjbRfsILWPkQQmVUCDR7dGqEo-mdPZ6iwolvGk';

export function getSupabaseClient() {
  if (isSupabaseClient(cachedSupabaseClient)) return cachedSupabaseClient;

  if (isSupabaseClient(window.supabase)) {
    cachedSupabaseClient = window.supabase;
    return cachedSupabaseClient;
  }

  const SUPABASE_URL = window.SUPABASE_URL || DEFAULT_SUPABASE_URL;
  const SUPABASE_KEY = window.SUPABASE_KEY || DEFAULT_SUPABASE_KEY;
  cachedSupabaseClient = createClient(SUPABASE_URL, SUPABASE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false }
  });
  window.supabase = cachedSupabaseClient;
  return cachedSupabaseClient;
}

export async function waitForSupabaseClient({ timeoutMs = 6000, intervalMs = 50 } = {}) {
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const client = getSupabaseClient();
    if (client) return client;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  return null;
}
