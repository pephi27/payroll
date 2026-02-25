function isSupabaseClient(client) {
  return !!(client && typeof client.from === 'function' && typeof client.channel === 'function');
}

export function getSupabaseClient() {
  return isSupabaseClient(window.supabase) ? window.supabase : null;
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
