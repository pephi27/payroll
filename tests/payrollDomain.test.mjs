import assert from 'node:assert/strict';
import {
  buildPayrollRow,
  reducePayrollTotals,
  reduceDeductionTotals,
  reduceOvertimeTotals,
  resolveNightDifferentialPay,
  calculatePrincipalLoanDeductionDecision,
  calculatePagibigLoanPerPeriod,
  totalAdditionalIncome,
  totalOtherDeductions,
} from '../src/domain/payrollCalculations.js';

const sssTable = [{ min: 0, max: 1e9, employee: 1000 }];
const pagibigTable = [{ min: 0, max: 1e9, rate: 0.02 }];
const philhealthTable = [{ min: 0, max: 1e9, rate: 0.025 }];

// regular pay only
const regularOnly = buildPayrollRow({
  employeeId: 'E1', regularHours: 40, hourlyRate: 100, divisor: 2,
  sssTable, pagibigTable, philhealthTable,
});
assert.equal(regularOnly.regular_pay, 4000);
assert.equal(regularOnly.overtime_pay, 0);

// regular + OT + ND
const withOt = buildPayrollRow({
  employeeId: 'E2', regularHours: 40, overtimeHours: 5, hourlyRate: 100,
  overtimeMultiplier: 1.25, nightDiffPay: 50, divisor: 2,
  sssTable, pagibigTable, philhealthTable,
});
assert.equal(withOt.overtime_pay_base, 625);
assert.equal(withOt.night_diff_pay, 50);
assert.equal(withOt.overtime_pay, 675);

// ND computation can be centralized from minute overlap input
const ndFromMinutes = resolveNightDifferentialPay({
  hourlyRate: 100,
  nightDiffMinutes: 120,
  settings: { enabled: true, multiplier: 1.1 },
  preferPrecomputed: false,
});
assert.equal(ndFromMinutes.pay, 20);
assert.equal(ndFromMinutes.source, 'minutes');

const ndDisabled = resolveNightDifferentialPay({
  hourlyRate: 100,
  nightDiffMinutes: 120,
  settings: { enabled: false, multiplier: 1.5 },
  precomputedNightDiffPay: 999,
  preferPrecomputed: true,
});
assert.equal(ndDisabled.pay, 0);
assert.equal(ndDisabled.source, 'disabled');

// additional income and other deductions
const incomeItemsTotal = totalAdditionalIncome([{ amount: '100' }, { amount: -20 }, { amount: 50 }]);
const otherDedTotal = totalOtherDeductions([{ amount: '40' }, { amount: -5 }]);
assert.equal(incomeItemsTotal, 150);
assert.equal(otherDedTotal, 40);

const withIncomeAndDeds = buildPayrollRow({
  employeeId: 'E3', regularHours: 40, hourlyRate: 100,
  additionalIncomeTotal: incomeItemsTotal,
  otherDeductionsTotal: otherDedTotal,
  sssTable, pagibigTable, philhealthTable,
});
assert.equal(withIncomeAndDeds.additional_income_total, 150);
assert.equal(withIncomeAndDeds.other_deductions, 40);

// contribution flags disable selected contributions
const withFlags = buildPayrollRow({
  employeeId: 'E4', regularHours: 40, hourlyRate: 100,
  contributionFlags: { pagibig: false, philhealth: true, sss: false },
  sssTable, pagibigTable, philhealthTable,
});
assert.equal(withFlags.pagibig_deduction, 0);
assert.equal(withFlags.sss_deduction, 0);
assert.ok(withFlags.philhealth_deduction > 0);

// loans with and without worked time behavior
const noWork = buildPayrollRow({
  employeeId: 'E5', loanSSS: 200, loanPI: 100, divisor: 2,
  sssTable, pagibigTable, philhealthTable,
});
assert.equal(noWork.loan_sss_deduction, 0);
assert.equal(noWork.loan_pagibig_deduction, 0);

const noWorkButApplyLoans = buildPayrollRow({
  employeeId: 'E6', loanSSS: 200, loanPI: 100, divisor: 2,
  applyLoansWithoutWorkedTime: true,
  sssTable, pagibigTable, philhealthTable,
});
assert.equal(noWorkButApplyLoans.loan_sss_deduction, 100);
assert.equal(noWorkButApplyLoans.loan_pagibig_deduction, 50);

// tracker-style principal decisioning handles remaining balance and stop-at-zero
const loanDecision = calculatePrincipalLoanDeductionDecision({
  active: true,
  principal: 1000,
  periodicAmount: 300,
  paidBefore: 900,
  baseline: 0,
  existingApplied: null,
});
assert.equal(loanDecision.desired, 100);
assert.equal(loanDecision.shouldDeactivate, false);

const loanDoneDecision = calculatePrincipalLoanDeductionDecision({
  active: true,
  principal: 1000,
  periodicAmount: 300,
  paidBefore: 1000,
  baseline: 0,
  existingApplied: null,
});
assert.equal(loanDoneDecision.shouldRun, false);
assert.equal(loanDoneDecision.shouldDeactivate, true);

assert.equal(calculatePagibigLoanPerPeriod({ active: true, monthly: 500, divisor: 2 }), 250);

// adjustment hours affect result
const withAdjustment = buildPayrollRow({
  employeeId: 'E7', regularHours: 40, regularAdjustmentHours: 2, hourlyRate: 100,
  sssTable, pagibigTable, philhealthTable,
});
assert.equal(withAdjustment.adjustment_pay, 200);

// zero-work scenario keeps contributions at 0 with no compensation
const zeroWork = buildPayrollRow({
  employeeId: 'E8', regularHours: 0, overtimeHours: 0, hourlyRate: 100,
  sssTable, pagibigTable, philhealthTable,
});
assert.equal(zeroWork.pagibig_deduction, 0);
assert.equal(zeroWork.philhealth_deduction, 0);
assert.equal(zeroWork.sss_deduction, 0);

// totals reducers
const totals = reducePayrollTotals([regularOnly, withOt]);
assert.equal(totals.gross_pay, regularOnly.gross_pay + withOt.gross_pay);
const dedTotals = reduceDeductionTotals([regularOnly, withOt]);
assert.equal(dedTotals.total, regularOnly.total_deductions + withOt.total_deductions);
const otTotals = reduceOvertimeTotals([regularOnly, withOt]);
assert.equal(otTotals.overtime_pay, regularOnly.overtime_pay + withOt.overtime_pay);

const deductionRows = [
  { vale_deduction: 100, vale_wed_deduction: 50, total_deductions: 150 },
  { vale_deduction: 20, vale_wed_deduction: 30, total_deductions: 50 },
];
const withValeTotals = reduceDeductionTotals(deductionRows);
assert.equal(withValeTotals.vale, 120);
assert.equal(withValeTotals.valeWed, 80);

const rowWithVale = buildPayrollRow({
  employeeId: 'E9',
  regularHours: 40,
  hourlyRate: 100,
  vale: 100,
  valeWed: 25,
  sssTable,
  pagibigTable,
  philhealthTable,
});
const combinedTotals = reduceDeductionTotals([rowWithVale]);
assert.equal(combinedTotals.vale, rowWithVale.vale_deduction);
assert.equal(combinedTotals.valeWed, rowWithVale.vale_wed_deduction);

console.log('payrollDomain tests passed');
