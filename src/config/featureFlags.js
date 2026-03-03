const LOCAL_FLAGS_KEY = 'payroll_feature_flags';

function readLocalFlags() {
  try {
    const raw = window.localStorage?.getItem(LOCAL_FLAGS_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
    return parsed;
  } catch (_) {
    return null;
  }
}

function readRuntimeFlags() {
  const runtime = window.__PAYROLL_FLAGS;
  if (!runtime || typeof runtime !== 'object' || Array.isArray(runtime)) return null;
  return runtime;
}

export function getFeatureFlag(name, fallback = false) {
  const runtime = readRuntimeFlags();
  if (runtime && Object.prototype.hasOwnProperty.call(runtime, name)) {
    return !!runtime[name];
  }

  const local = readLocalFlags();
  if (local && Object.prototype.hasOwnProperty.call(local, name)) {
    return !!local[name];
  }

  return !!fallback;
}

