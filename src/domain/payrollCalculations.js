const DEFAULT_OT_MULTIPLIER = 1.25;
const DEFAULT_ND_MULTIPLIER = 1.1;

export function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

export function roundToCents(value) {
  return Math.round(toNumber(value, 0) * 100) / 100;
}

function normalizePositiveNumber(value) {
  return Math.max(0, toNumber(value, 0));
}

function normalizeRangeTable(rows, valueKey) {
  if (!Array.isArray(rows)) return [];
  return rows
    .map((row) => ({
      min: toNumber(row?.min, 0),
      max: toNumber(row?.max, 0),
      [valueKey]: toNumber(row?.[valueKey], 0),
    }))
    .sort((a, b) => a.min - b.min);
}

export function lookupBracketValue(monthlyIncome, rows, valueKey) {
  const table = normalizeRangeTable(rows, valueKey);
  if (!table.length) return 0;
  const monthly = toNumber(monthlyIncome, 0);
  if (monthly <= table[0].min) return toNumber(table[0][valueKey], 0);
  for (const row of table) {
    if (monthly >= row.min && monthly <= row.max) {
      return toNumber(row[valueKey], 0);
    }
  }
  return toNumber(table[table.length - 1][valueKey], 0);
}

export function loanSharePerPeriod(rawLoanAmount, divisor = 2) {
  const loan = normalizePositiveNumber(rawLoanAmount);
  const periods = Math.max(1, toNumber(divisor, 1));
  return roundToCents(loan / periods);
}

export function normalizeAdditionalIncomeItems(items = []) {
  if (!Array.isArray(items)) return [];
  return items.map((item) => ({
    ...item,
    amount: roundToCents(normalizePositiveNumber(item?.amount)),
  }));
}

export function totalAdditionalIncome(items = []) {
  return roundToCents(normalizeAdditionalIncomeItems(items).reduce((sum, item) => sum + item.amount, 0));
}

export function normalizeOtherDeductionsItems(items = []) {
  if (!Array.isArray(items)) return [];
  return items.map((item) => ({
    ...item,
    amount: roundToCents(normalizePositiveNumber(item?.amount)),
  }));
}

export function totalOtherDeductions(items = []) {
  return roundToCents(normalizeOtherDeductionsItems(items).reduce((sum, item) => sum + item.amount, 0));
}

export function calculateOvertimePay({ hourlyRate, overtimeHours, overtimeMultiplier = DEFAULT_OT_MULTIPLIER }) {
  const pay = toNumber(hourlyRate, 0) * toNumber(overtimeHours, 0) * toNumber(overtimeMultiplier, DEFAULT_OT_MULTIPLIER);
  return roundToCents(pay);
}

export function calculateNightDifferentialPay({ hourlyRate, nightDiffHours = 0, nightDifferentialMultiplier = DEFAULT_ND_MULTIPLIER, precomputedNightDiffPay = null }) {
  if (precomputedNightDiffPay != null) return roundToCents(precomputedNightDiffPay);
  const premiumFactor = Math.max(0, toNumber(nightDifferentialMultiplier, DEFAULT_ND_MULTIPLIER) - 1);
  return roundToCents(toNumber(hourlyRate, 0) * toNumber(nightDiffHours, 0) * premiumFactor);
}

export function calculateContributionDeductions({ regularPay = 0, hourlyRate = 0, divisor = 2, flags = {}, hasCompensation = true, sssTable = [], pagibigTable = [], philhealthTable = [] }) {
  const monthly = toNumber(hourlyRate, 0) * 8 * 24;
  const safeDivisor = Math.max(1, toNumber(divisor, 1));
  const pagibigRate = lookupBracketValue(monthly, pagibigTable, 'rate');
  const philhealthRate = lookupBracketValue(monthly, philhealthTable, 'rate');
  const sssEmployeeShare = lookupBracketValue(monthly, sssTable, 'employee');

  const pagibig = hasCompensation && flags.pagibig !== false ? roundToCents(toNumber(regularPay, 0) * pagibigRate) : 0;
  const philhealth = hasCompensation && flags.philhealth !== false ? roundToCents(toNumber(regularPay, 0) * philhealthRate) : 0;
  const sss = hasCompensation && flags.sss !== false ? roundToCents(sssEmployeeShare / safeDivisor) : 0;

  return {
    monthly,
    pagibigRate,
    philhealthRate,
    sssEmployeeShare,
    pagibig,
    philhealth,
    sss,
  };
}

export function calculateLoanDeductions({ loanSSS = 0, loanPI = 0, divisor = 2, hasWorkedTime = true, applyLoansWithoutWorkedTime = false }) {
  const safeDivisor = Math.max(1, toNumber(divisor, 1));
  const allow = applyLoansWithoutWorkedTime || hasWorkedTime;
  return {
    loanSSS: allow ? roundToCents(toNumber(loanSSS, 0) / safeDivisor) : 0,
    loanPI: allow ? roundToCents(toNumber(loanPI, 0) / safeDivisor) : 0,
  };
}

export function buildPayrollRow(input = {}) {
  const {
    employeeId,
    regularHours = 0,
    overtimeHours = 0,
    regularAdjustmentHours = 0,
    overtimeAdjustmentHours = 0,
    hourlyRate = 0,
    overtimeMultiplier = DEFAULT_OT_MULTIPLIER,
    nightDiffHours = 0,
    nightDiffPay = 0,
    nightDifferentialMultiplier = DEFAULT_ND_MULTIPLIER,
    additionalIncomeTotal = 0,
    otherDeductionsTotal = 0,
    loanSSS = 0,
    loanPI = 0,
    vale = 0,
    valeWed = 0,
    divisor = 2,
    contributionFlags = {},
    sssTable = [],
    pagibigTable = [],
    philhealthTable = [],
    applyLoansWithoutWorkedTime = false,
  } = input;

  const regHours = toNumber(regularHours, 0);
  const otHours = toNumber(overtimeHours, 0) + toNumber(overtimeAdjustmentHours, 0);
  const regAdjHours = toNumber(regularAdjustmentHours, 0);
  const rate = toNumber(hourlyRate, 0);

  const regularPay = roundToCents(regHours * rate);
  const adjustmentPay = roundToCents(regAdjHours * rate);
  const overtimePayBase = calculateOvertimePay({ hourlyRate: rate, overtimeHours: otHours, overtimeMultiplier });
  const nightDiffComputed = calculateNightDifferentialPay({
    hourlyRate: rate,
    nightDiffHours,
    precomputedNightDiffPay: nightDiffPay,
    nightDifferentialMultiplier,
  });
  const overtimePay = roundToCents(overtimePayBase + nightDiffComputed);

  const normalizedAdditionalIncome = roundToCents(normalizePositiveNumber(additionalIncomeTotal));
  const normalizedOtherDeductions = roundToCents(normalizePositiveNumber(otherDeductionsTotal));

  const grossPay = roundToCents(regularPay + adjustmentPay + overtimePay + normalizedAdditionalIncome);
  const hasWorkedTime = regHours > 0 || otHours > 0 || regAdjHours > 0;
  const hasCompensation = hasWorkedTime || normalizedAdditionalIncome > 0;

  const contributions = calculateContributionDeductions({
    regularPay,
    hourlyRate: rate,
    divisor,
    flags: contributionFlags,
    hasCompensation,
    sssTable,
    pagibigTable,
    philhealthTable,
  });

  const loans = calculateLoanDeductions({
    loanSSS,
    loanPI,
    divisor,
    hasWorkedTime,
    applyLoansWithoutWorkedTime,
  });

  const deductionParts = {
    pagibig: contributions.pagibig,
    philhealth: contributions.philhealth,
    sss: contributions.sss,
    loanSSS: loans.loanSSS,
    loanPI: loans.loanPI,
    vale: roundToCents(toNumber(vale, 0)),
    valeWed: roundToCents(toNumber(valeWed, 0)),
    otherDeductions: normalizedOtherDeductions,
  };

  const totalDeductions = roundToCents(
    deductionParts.pagibig + deductionParts.philhealth + deductionParts.sss + deductionParts.loanSSS + deductionParts.loanPI + deductionParts.vale + deductionParts.valeWed + deductionParts.otherDeductions,
  );

  return {
    employee_id: employeeId,
    regular_hours: regHours,
    overtime_hours: otHours,
    night_diff_hours: toNumber(nightDiffHours, 0),
    hourly_rate: rate,
    regular_pay: regularPay,
    overtime_pay_base: overtimePayBase,
    night_diff_pay: nightDiffComputed,
    overtime_pay: overtimePay,
    adjustment_hours: regAdjHours,
    adjustment_pay: adjustmentPay,
    additional_income_total: normalizedAdditionalIncome,
    gross_pay: grossPay,
    pagibig_deduction: deductionParts.pagibig,
    philhealth_deduction: deductionParts.philhealth,
    sss_deduction: deductionParts.sss,
    loan_sss_deduction: deductionParts.loanSSS,
    loan_pagibig_deduction: deductionParts.loanPI,
    vale_deduction: deductionParts.vale,
    vale_wed_deduction: deductionParts.valeWed,
    other_deductions: deductionParts.otherDeductions,
    total_deductions: totalDeductions,
    net_pay: roundToCents(grossPay - totalDeductions),
  };
}

export function reducePayrollTotals(rows = []) {
  return rows.reduce((totals, row) => {
    totals.gross_pay = roundToCents(totals.gross_pay + toNumber(row?.gross_pay, 0));
    totals.total_deductions = roundToCents(totals.total_deductions + toNumber(row?.total_deductions, 0));
    totals.net_pay = roundToCents(totals.net_pay + toNumber(row?.net_pay, 0));
    totals.regular_pay = roundToCents(totals.regular_pay + toNumber(row?.regular_pay, 0));
    totals.overtime_pay = roundToCents(totals.overtime_pay + toNumber(row?.overtime_pay, 0));
    totals.adjustment_pay = roundToCents(totals.adjustment_pay + toNumber(row?.adjustment_pay, 0));
    totals.additional_income_total = roundToCents(totals.additional_income_total + toNumber(row?.additional_income_total, 0));
    return totals;
  }, {
    gross_pay: 0,
    total_deductions: 0,
    net_pay: 0,
    regular_pay: 0,
    overtime_pay: 0,
    adjustment_pay: 0,
    additional_income_total: 0,
  });
}

export function reduceOvertimeTotals(rows = []) {
  return rows.reduce((totals, row) => {
    totals.overtime_hours = roundToCents(totals.overtime_hours + toNumber(row?.overtime_hours, 0));
    totals.night_diff_pay = roundToCents(totals.night_diff_pay + toNumber(row?.night_diff_pay, 0));
    totals.overtime_pay = roundToCents(totals.overtime_pay + toNumber(row?.overtime_pay, 0));
    return totals;
  }, {
    overtime_hours: 0,
    night_diff_pay: 0,
    overtime_pay: 0,
  });
}

export function reduceDeductionTotals(rows = []) {
  return rows.reduce((totals, row) => {
    totals.pagibig = roundToCents(totals.pagibig + toNumber(row?.pagibig_deduction, 0));
    totals.philhealth = roundToCents(totals.philhealth + toNumber(row?.philhealth_deduction, 0));
    totals.sss = roundToCents(totals.sss + toNumber(row?.sss_deduction, 0));
    totals.loanSSS = roundToCents(totals.loanSSS + toNumber(row?.loan_sss_deduction, 0));
    totals.loanPI = roundToCents(totals.loanPI + toNumber(row?.loan_pagibig_deduction, 0));
    totals.other = roundToCents(totals.other + toNumber(row?.other_deductions, 0));
    totals.total = roundToCents(totals.total + toNumber(row?.total_deductions, 0));
    return totals;
  }, {
    pagibig: 0,
    philhealth: 0,
    sss: 0,
    loanSSS: 0,
    loanPI: 0,
    other: 0,
    total: 0,
  });
}

export function calculateGrossPay({ hourlyRate, regularHours, overtimeHours, overtimeMultiplier = DEFAULT_OT_MULTIPLIER }) {
  const regular = toNumber(hourlyRate, 0) * toNumber(regularHours, 0);
  const overtime = toNumber(hourlyRate, 0) * toNumber(overtimeHours, 0) * toNumber(overtimeMultiplier, DEFAULT_OT_MULTIPLIER);
  return roundToCents(regular + overtime);
}

export function calculatePayrollRow({ employee, attendance, loanDeductions = 0, contributionDeductions = 0 }) {
  const regularHours = toNumber(attendance?.regular_hours, 0);
  const overtimeHours = toNumber(attendance?.overtime_hours, 0);
  const hasWorkedTime = regularHours + overtimeHours > 0;

  const grossPay = calculateGrossPay({
    hourlyRate: employee?.hourly_rate,
    regularHours,
    overtimeHours,
    overtimeMultiplier: employee?.overtime_multiplier || DEFAULT_OT_MULTIPLIER,
  });

  const effectiveLoanDeductions = hasWorkedTime ? toNumber(loanDeductions, 0) : 0;
  const effectiveContributionDeductions = toNumber(contributionDeductions, 0);

  const netPay = grossPay - effectiveLoanDeductions - effectiveContributionDeductions;

  return {
    employee_id: employee?.id,
    gross_pay: roundToCents(grossPay),
    loan_deductions: roundToCents(effectiveLoanDeductions),
    contribution_deductions: roundToCents(effectiveContributionDeductions),
    net_pay: roundToCents(netPay),
  };
}
