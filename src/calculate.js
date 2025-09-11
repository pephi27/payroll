// Utility helpers
function toMins(str){
  if (typeof str !== 'string') return NaN;
  const match = /^([0-9]{1,2}):([0-9]{2})$/.exec(str);
  if (!match) return NaN;
  const h = Number(match[1]);
  const m = Number(match[2]);
  if (h < 0 || h > 23 || m < 0 || m > 59) return NaN;
  return h*60 + m;
}
function minsToStr(mins){
  mins = ((mins % 1440) + 1440) % 1440;
  const h = Math.floor(mins/60).toString().padStart(2, '0');
  const m = (mins%60).toString().padStart(2, '0');
  return `${h}:${m}`;
}

// Existing helpers (placeholders if not provided elsewhere)
function bridgeMidnight(inStr, outStr){
  const inM = toMins(inStr);
  let outM = toMins(outStr);
  if (outM < inM) outM += 1440;
  return [inM, outM];
}

// Fetch the first punch for `empId` on the day after `dateStr` from Supabase.
// Environment variables SUPABASE_URL, SUPABASE_KEY and SUPABASE_PUNCH_TABLE
// control the query.  Returns a time string like "05:30" or null on failure.
async function defaultNextDayFirstPunch(empId, dateStr){
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_KEY || process.env.SUPABASE_ANON_KEY;
  const table = process.env.SUPABASE_PUNCH_TABLE || 'punches';
  if (!url || !key) return null;
  try {
    const d = new Date(dateStr);
    if (Number.isNaN(d.getTime())) return null;
    d.setDate(d.getDate() + 1);
    const nextDate = d.toISOString().slice(0, 10);
    const params = new URLSearchParams({
      select: 'time',
      emp_id: `eq.${empId}`,
      date: `eq.${nextDate}`,
      order: 'time',
      limit: '1'
    });
    const resp = await fetch(`${url}/rest/v1/${table}?${params.toString()}`, {
      headers: { apikey: key, Authorization: `Bearer ${key}` }
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    if (!Array.isArray(data) || data.length === 0) return null;
    const punch = data[0];
    const t = punch.time || punch.punch_time || punch.in;
    return (typeof t === 'string') ? t : null;
  } catch (e) {
    console.warn('__getNextDayFirstPunch failed', e);
    return null;
  }
}

let __getNextDayFirstPunch = defaultNextDayFirstPunch;
function __setNextDayFirstPunchFetcher(fn){
  __getNextDayFirstPunch = fn || defaultNextDayFirstPunch;
}

function isNextDay(inStr, outStr){
  return toMins(outStr) < toMins(inStr);
}

/**
 * Adjust in/out times that span midnight.
 * Returns normalized [in, out] strings respecting a 06:30 cutoff
 * or the first punch on the following day.
 */
async function adjustOvernight(inStr, outStr, empId, dateStr){
  if (!inStr || !outStr) return [inStr, outStr];
  if (isNaN(toMins(inStr)) || isNaN(toMins(outStr))) {
    throw new Error('Invalid time format');
  }
  let [inM, outM] = bridgeMidnight(inStr, outStr);
  if (isNextDay(inStr, outStr)) {
    const cutoff = 6 * 60 + 30; // 06:30 in minutes
    let limitM = cutoff;
    try {
      const nextPunchStr = await __getNextDayFirstPunch(empId, dateStr);
      if (typeof nextPunchStr === 'string') {
        const npM = toMins(nextPunchStr);
        if (!isNaN(npM)) {
          limitM = Math.min(npM, cutoff);
        }
      }
    } catch {
      // ignore fetch errors, fall back to cutoff
    }
    const maxOut = 1440 + limitM;
    if (outM > maxOut) outM = maxOut;
  }
  return [minsToStr(inM), minsToStr(outM)];
}

module.exports = { adjustOvernight, toMins, __setNextDayFirstPunchFetcher };
