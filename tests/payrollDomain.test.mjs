import assert from 'node:assert/strict';
import {
  buildPayrollRow,
  reducePayrollTotals,
  reduceDeductionTotals,
  reduceOvertimeTotals,
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

console.log('payrollDomain tests passed');
