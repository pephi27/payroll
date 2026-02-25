export function calculateGrossPay({ hourlyRate, regularHours, overtimeHours, overtimeMultiplier = 1.25 }) {
  const regular = Number(hourlyRate || 0) * Number(regularHours || 0);
  const overtime = Number(hourlyRate || 0) * Number(overtimeHours || 0) * Number(overtimeMultiplier || 0);
  return regular + overtime;
}

export function calculatePayrollRow({ employee, attendance, loanDeductions = 0, contributionDeductions = 0 }) {
  const grossPay = calculateGrossPay({
    hourlyRate: employee.hourly_rate,
    regularHours: attendance.regular_hours,
    overtimeHours: attendance.overtime_hours,
    overtimeMultiplier: employee.overtime_multiplier || 1.25,
  });

  const netPay = grossPay - Number(loanDeductions || 0) - Number(contributionDeductions || 0);

  return {
    employee_id: employee.id,
    gross_pay: grossPay,
    loan_deductions: Number(loanDeductions || 0),
    contribution_deductions: Number(contributionDeductions || 0),
    net_pay: netPay,
  };
}
