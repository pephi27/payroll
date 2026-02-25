import { payrollService } from './services/payrollService.js';
import { setCurrentPeriod } from './state/store.js';
import { startRealtimeSubscriptions } from './realtime/subscriptions.js';
import { mountPayrollController } from './ui/payrollController.js';

async function bootstrapPayrollApp() {
  const periods = await payrollService.loadPeriods();
  if (periods.length) {
    setCurrentPeriod(periods[0].id);
  }

  const root = document.getElementById('panelPayroll') || document.body;
  const cleanupUi = mountPayrollController(root);
  const cleanupRealtime = startRealtimeSubscriptions();

  window.addEventListener('beforeunload', () => {
    cleanupUi();
    cleanupRealtime();
  });
}

document.addEventListener('DOMContentLoaded', () => {
  bootstrapPayrollApp().catch((error) => {
    console.error('Payroll bootstrap failed', error);
  });
});
