const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

function _parse(n){ var x=parseFloat(String(n||'').replace(/[^0-9.\-]/g,'')); return isNaN(x)?0:x; }
function _fmt(n){ var v = Math.round((n||0)*100)/100; try { return v.toLocaleString(undefined,{minimumFractionDigits:2, maximumFractionDigits:2}); } catch(e){ return v.toFixed(2); } }
function updatePayrollGrandTotals(){
  var tb = document.querySelector('#payrollTable tbody');
  var foot = document.querySelector('#payrollTotalsFoot');
  if (!tb || !foot) return;
  var t = {regHrs:0, otHrs:0, adjHrs:0, totalHrs:0, rate:0, regPay:0, otPay:0, grossPay:0, pagibig:0, philhealth:0, sss:0, loanSSS:0, loanPI:0, vale:0, valeWed:0, totalDed:0, adjAmt:0, bantay:0, netPay:0};
  var div = Number(divisor) || 1;
  tb.querySelectorAll('tr').forEach(function(tr){
    t.regHrs   += _parse(tr.querySelector('.regHrs')?.value);
    t.otHrs    += _parse(tr.querySelector('.otHrs')?.value);
    t.adjHrs   += _parse(tr.querySelector('.adjHrs')?.textContent);
    t.totalHrs += _parse(tr.querySelector('.totalHrs')?.textContent);
    t.rate     += _parse(tr.querySelector('.rate')?.value);
    t.regPay   += _parse(tr.querySelector('.regPay')?.textContent);
    t.otPay    += _parse(tr.querySelector('.otPay')?.textContent);
    t.grossPay += _parse(tr.querySelector('.grossPay')?.textContent);
    t.pagibig  += _parse(tr.querySelector('.pagibig')?.textContent);
    t.philhealth += _parse(tr.querySelector('.philhealth')?.textContent);
    t.sss      += _parse(tr.querySelector('.sss')?.textContent);
    t.loanSSS  += _parse(tr.querySelector('.loanSSS')?.value) / div;
    t.loanPI   += _parse(tr.querySelector('.loanPI')?.value) / div;
    t.vale     += _parse(tr.querySelector('.vale')?.value);
    t.valeWed  += _parse(tr.querySelector('.valeWed')?.value);
    t.totalDed += _parse(tr.querySelector('.totalDed')?.textContent);
    t.adjAmt   += _parse(tr.querySelector('.adjAmt')?.textContent);
    t.bantay  += _parse(tr.querySelector('.bantay')?.value);
    t.netPay   += _parse(tr.querySelector('.netPay')?.textContent);
  });
  Object.keys(t).forEach(function(k){
    var cell = foot.querySelector('[data-col="'+k+'"]');
    if (cell) cell.textContent = _fmt(t[k]);
  });
}

describe('updatePayrollGrandTotals', () => {
  test('sums total deductions from table rows in index.html', () => {
    const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
    const dom = new JSDOM(html);
    global.document = dom.window.document;
    global.divisor = 1;
    updatePayrollGrandTotals();
    const totalCell = dom.window.document.querySelector('#payrollTotalsFoot [data-col="totalDed"]');
    expect(totalCell.textContent).toBe('350.00');
  });
});
