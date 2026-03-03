import { clearConflict, getState, subscribe } from '../state/store.js';

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

function renderConflictBanner(root) {
  const state = getState();
  const banner = root.querySelector('[data-payroll-conflict-banner]');
  if (!banner) return;

  const conflict = state.diagnostics.lastConflict;
  if (!conflict) {
    banner.hidden = true;
    return;
  }

  const message = banner.querySelector('[data-payroll-conflict-message]');
  if (message) {
    message.textContent = 'This record was updated by another user. Reloading latest row data.';
  }
  banner.hidden = false;
}

function applyLockedRule(el, isLocked) {
  if (el.dataset.payrollHealthToggle === 'true' || el.closest('[data-payroll-health-panel]')) return;
  if (el.dataset.allowWhenLocked === 'true') return;

  if (isLocked) {
    el.dataset.lockedDisabled = el.disabled ? 'already' : 'managed';
    if (!el.disabled) el.disabled = true;
    return;
  }

  if (el.dataset.lockedDisabled === 'managed') {
    el.disabled = false;
  }
  delete el.dataset.lockedDisabled;
}

function renderLockedInputState(root, period) {
  const isLocked = !!period?.is_locked;
  root.querySelectorAll('#payrollWrapper input, #payrollWrapper select, #payrollWrapper textarea, #payrollWrapper button')
    .forEach((el) => applyLockedRule(el, isLocked));
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

  const wrapper = root.querySelector('#payrollWrapper') || root;
  const anchor = wrapper.querySelector('header');

  const container = document.createElement('div');
  container.dataset.payrollHealthContainer = 'true';
  container.style.margin = '8px 0 10px';

  const conflictBanner = document.createElement('div');
  conflictBanner.dataset.payrollConflictBanner = 'true';
  conflictBanner.hidden = true;
  conflictBanner.style.marginBottom = '8px';
  conflictBanner.style.padding = '8px';
  conflictBanner.style.border = '1px solid #f59e0b';
  conflictBanner.style.borderRadius = '8px';
  conflictBanner.style.fontSize = '12px';
  conflictBanner.style.background = 'rgba(245,158,11,0.1)';
  conflictBanner.innerHTML = `
    <span data-payroll-conflict-message></span>
    <button type="button" class="bp-btn" data-payroll-conflict-dismiss style="margin-left:8px">Dismiss</button>
  `;

  const toggle = document.createElement('button');
  toggle.type = 'button';
  toggle.className = 'bp-btn';
  toggle.dataset.payrollHealthToggle = 'true';
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

  conflictBanner.querySelector('[data-payroll-conflict-dismiss]')?.addEventListener('click', () => {
    clearConflict();
  });

  container.appendChild(conflictBanner);
  container.appendChild(toggle);
  container.appendChild(panel);

  if (anchor && anchor.parentNode) {
    anchor.insertAdjacentElement('afterend', container);
  } else {
    wrapper.prepend(container);
  }
}

export function mountPayrollController(root) {
  let lastLockKey = null;
  let lockObserver = null;

  function refreshLockObserver(isLocked) {
    if (lockObserver) {
      lockObserver.disconnect();
      lockObserver = null;
    }
    if (!isLocked) return;

    const wrapper = root.querySelector('#payrollWrapper');
    if (!wrapper) return;

    lockObserver = new MutationObserver((mutations) => {
      for (const m of mutations) {
        for (const node of m.addedNodes || []) {
          if (!(node instanceof Element)) continue;

          if (node.matches('input, select, textarea, button')) {
            applyLockedRule(node, true);
          }

          node.querySelectorAll?.('input, select, textarea, button')
            .forEach((el) => applyLockedRule(el, true));
        }
      }
    });

    lockObserver.observe(wrapper, { childList: true, subtree: true });
  }

  const render = (state, change) => {
    void state;
    void change;
    ensureDiagnosticsPanel(root);
    const currentState = getState();
    const period = currentState.currentPeriodId ? currentState.payrollPeriods.get(currentState.currentPeriodId) : null;
    const isLocked = !!period?.is_locked;
    const lockKey = `${currentState.currentPeriodId || ''}:${isLocked ? '1' : '0'}`;
    renderPeriodLockStatus(root, period);
    renderPunchCount(root);
    renderDiagnostics(root);
    renderConflictBanner(root);

    if (lockKey !== lastLockKey) {
      renderLockedInputState(root, period);
      refreshLockObserver(isLocked);
      lastLockKey = lockKey;
    }
  };

  render();
  const unsub = subscribe(render);
  return () => {
    if (lockObserver) lockObserver.disconnect();
    lockObserver = null;
    unsub();
  };
}
