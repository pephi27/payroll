export function calculateGrossPay({ hourlyRate, regularHours, overtimeHours, overtimeMultiplier = 1.25 }) {
  const regular = Number(hourlyRate || 0) * Number(regularHours || 0);
  const overtime = Number(hourlyRate || 0) * Number(overtimeHours || 0) * Number(overtimeMultiplier || 0);
  return regular + overtime;
}

export function calculatePayrollRow({ employee, attendance, loanDeductions = 0, contributionDeductions = 0 }) {
  const regularHours = Number(attendance?.regular_hours || 0);
  const overtimeHours = Number(attendance?.overtime_hours || 0);
  const hasWorkedTime = regularHours + overtimeHours > 0;

  const grossPay = calculateGrossPay({
    hourlyRate: employee.hourly_rate,
    regularHours,
    overtimeHours,
    overtimeMultiplier: employee.overtime_multiplier || 1.25,
  });

  const effectiveLoanDeductions = hasWorkedTime ? Number(loanDeductions || 0) : 0;
  const effectiveContributionDeductions = Number(contributionDeductions || 0);

  const netPay = grossPay - effectiveLoanDeductions - effectiveContributionDeductions;

  return {
    employee_id: employee.id,
    gross_pay: grossPay,
    loan_deductions: effectiveLoanDeductions,
    contribution_deductions: effectiveContributionDeductions,
    net_pay: netPay,
  };
}
