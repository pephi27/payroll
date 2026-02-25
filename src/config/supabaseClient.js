import { createClient } from 'https://esm.sh/@supabase/supabase-js@2?bundle';

const SUPABASE_URL = window.SUPABASE_URL || 'https://qzkzugzfpegozpiqutdv.supabase.co';
const SUPABASE_KEY = window.SUPABASE_ANON_KEY || '<SECRET>';

export const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: {
    persistSession: false,
    autoRefreshToken: false,
  },
  realtime: {
    params: { eventsPerSecond: 20 },
  },
});
