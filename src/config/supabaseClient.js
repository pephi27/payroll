import { createClient } from 'https://esm.sh/@supabase/supabase-js@2?bundle';

const SUPABASE_URL = window.SUPABASE_URL || 'https://qzkzugzfpegozpiqutdv.supabase.co';
const SUPABASE_KEY =
  window.SUPABASE_KEY ||
  window.SUPABASE_ANON_KEY ||
  '';

function buildSupabaseClient() {
  // Reuse the legacy singleton to avoid creating multiple GoTrueClient instances
  // in the same browser context.
  if (window.supabase && typeof window.supabase.from === 'function') {
    return window.supabase;
  }

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error(
      'Supabase client cannot be created because SUPABASE_URL/SUPABASE_KEY is missing.',
    );
  }

  const client = createClient(SUPABASE_URL, SUPABASE_KEY, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
      storageKey: `sb-${new URL(SUPABASE_URL).hostname}-auth-token`,
    },
    realtime: {
      params: { eventsPerSecond: 20 },
    },
  });

  window.supabase = client;
  return client;
}

export const supabase = buildSupabaseClient();
