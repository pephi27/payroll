// Utility helpers
function toMins(str){
  if (typeof str !== 'string') return NaN;
  const [h,m] = str.split(':').map(Number);
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

function __getNextDayFirstPunch(empId, dateStr){
  // Placeholder; real implementation should fetch next day punches.
  return null;
}

function isNextDay(inStr, outStr){
  return toMins(outStr) < toMins(inStr);
}

/**
 * Adjust in/out times that span midnight.
 * Returns normalized [in, out] strings respecting a 06:30 cutoff
 * or the first punch on the following day.
 */
function adjustOvernight(inStr, outStr, empId, dateStr){
  if (!inStr || !outStr) return [inStr, outStr];
  let [inM, outM] = bridgeMidnight(inStr, outStr);
  if (isNextDay(inStr, outStr)) {
    const cutoff = 6 * 60 + 30; // 06:30 in minutes
    const nextPunchStr = __getNextDayFirstPunch(empId, dateStr);
    let limitM = cutoff;
    if (typeof nextPunchStr === 'string') {
      const npM = toMins(nextPunchStr);
      if (!isNaN(npM)) {
        limitM = Math.min(npM, cutoff);
      }
    }
    const maxOut = 1440 + limitM;
    if (outM > maxOut) outM = maxOut;
  }
  return [minsToStr(inM), minsToStr(outM)];
}

module.exports = { adjustOvernight };
