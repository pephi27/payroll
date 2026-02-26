import { getState, subscribe } from '../state/store.js';

function renderPeriodLockStatus(root, period) {
  const lockEl = root.querySelector('[data-payroll-lock-state]');
  if (!lockEl) return;

  lockEl.textContent = period?.is_locked ? 'Locked' : 'Open';
  lockEl.dataset.locked = period?.is_locked ? 'true' : 'false';
}

function renderPunchCount(root) {
  const countEl = root.querySelector('[data-punch-count]');
  if (!countEl) return;
  countEl.textContent = String(getState().dtrPunches.size);
}


function renderDiagnostics(root) {
  const state = getState();
  const diag = state.diagnostics;
  const period = state.currentPeriodId ? state.payrollPeriods.get(state.currentPeriodId) : null;

  const connected = root.querySelector('[data-health-supabase]');
  if (connected) connected.textContent = diag.supabaseConnected === null ? 'Unknown' : diag.supabaseConnected ? 'Connected' : 'Disconnected';

  const rt = root.querySelector('[data-health-realtime]');
  if (rt) rt.textContent = diag.realtimeStatus || 'idle';

  const periodEl = root.querySelector('[data-health-period]');
  if (periodEl) periodEl.textContent = state.currentPeriodId || 'None';

  const lockedEl = root.querySelector('[data-health-locked]');
  if (lockedEl) lockedEl.textContent = period ? (period.is_locked ? 'Yes' : 'No') : 'Unknown';

  const lastEventEl = root.querySelector('[data-health-last-event]');
  if (lastEventEl) {
    if (!diag.lastRealtimeEvent) {
      lastEventEl.textContent = 'None';
    } else {
      const evt = diag.lastRealtimeEvent;
      lastEventEl.textContent = `${evt.type} ${evt.table} @ ${evt.timestamp}`;
    }
  }
}

function ensureDiagnosticsPanel(root) {
  if (root.querySelector('[data-payroll-health-panel]')) return;

  const header = root.querySelector('header') || root;
  const toggle = document.createElement('button');
  toggle.type = 'button';
  toggle.className = 'bp-btn';
  toggle.textContent = 'Health';

  const panel = document.createElement('div');
  panel.dataset.payrollHealthPanel = 'true';
  panel.hidden = true;
  panel.style.marginTop = '8px';
  panel.style.padding = '8px';
  panel.style.border = '1px solid var(--bp-border, #2e3747)';
  panel.style.borderRadius = '8px';
  panel.style.fontSize = '12px';
  panel.innerHTML = `
    <div>Supabase: <strong data-health-supabase>Unknown</strong></div>
    <div>Realtime: <strong data-health-realtime>idle</strong></div>
    <div>Active Period: <strong data-health-period>None</strong></div>
    <div>Locked: <strong data-health-locked>Unknown</strong></div>
    <div>Last Event: <strong data-health-last-event>None</strong></div>
  `;

  toggle.addEventListener('click', () => {
    panel.hidden = !panel.hidden;
  });

  header.appendChild(toggle);
  header.appendChild(panel);
}

export function mountPayrollController(root) {
  ensureDiagnosticsPanel(root);

  const render = () => {
    const state = getState();
    const period = state.currentPeriodId ? state.payrollPeriods.get(state.currentPeriodId) : null;
    renderPeriodLockStatus(root, period);
    renderPunchCount(root);
    renderDiagnostics(root);
  };

  render();
  return subscribe(render);
}
