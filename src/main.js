import { payrollService } from './services/payrollService.js';
import { setCurrentPeriod } from './state/store.js';
import { startRealtimeSubscriptions } from './realtime/subscriptions.js';
import { mountPayrollController } from './ui/payrollController.js';

let cleanupUi = null;
let cleanupRealtime = null;
let bootstrapped = false;

async function bootstrapPayrollApp() {
  if (bootstrapped) return;
  bootstrapped = true;

  try {
    const periods = await payrollService.loadPeriods();
    if (periods.length) {
      setCurrentPeriod(periods[0].id);
    }
  } catch (error) {
    // Do not crash UI boot when credentials or RLS are misconfigured.
    console.error('Payroll periods load failed', error);
  }

  const root = document.getElementById('panelPayroll') || document.body;
  cleanupUi = mountPayrollController(root);

  try {
    cleanupRealtime = startRealtimeSubscriptions();
  } catch (error) {
    console.error('Realtime bootstrap failed', error);
  }

  window.addEventListener('beforeunload', () => {
    if (typeof cleanupUi === 'function') cleanupUi();
    if (typeof cleanupRealtime === 'function') cleanupRealtime();
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', bootstrapPayrollApp);
} else {
  bootstrapPayrollApp().catch((error) => {
    console.error('Payroll bootstrap failed', error);
  });
}
