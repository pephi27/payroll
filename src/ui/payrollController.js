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

export function mountPayrollController(root) {
  const render = () => {
    const state = getState();
    const period = state.currentPeriodId ? state.payrollPeriods.get(state.currentPeriodId) : null;
    renderPeriodLockStatus(root, period);
    renderPunchCount(root);
  };

  render();
  return subscribe(render);
}
