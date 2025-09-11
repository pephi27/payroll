const test = require('node:test');
const assert = require('node:assert');
const { adjustOvernight, toMins, __setNextDayFirstPunchFetcher } = require('../src/calculate');

test('toMins parses valid times', () => {
  assert.strictEqual(toMins('07:30'), 450);
  assert.strictEqual(toMins('00:00'), 0);
  assert.strictEqual(toMins('23:59'), 1439);
});

test('toMins rejects invalid times', () => {
  assert.ok(Number.isNaN(toMins('24:00')));
  assert.ok(Number.isNaN(toMins('12:60')));
  assert.ok(Number.isNaN(toMins('7:5')));
  assert.ok(Number.isNaN(toMins('ab:cd')));
  assert.ok(Number.isNaN(toMins(123)));
});

test('adjustOvernight caps times past cutoff', async () => {
  assert.deepStrictEqual(
    await adjustOvernight('23:00', '08:00'),
    ['23:00', '06:30']
  );
});

test('adjustOvernight passes through valid overnight', async () => {
  assert.deepStrictEqual(
    await adjustOvernight('23:00', '05:00'),
    ['23:00', '05:00']
  );
});

test('adjustOvernight uses next-day punch before cutoff', async () => {
  __setNextDayFirstPunchFetcher(async () => '05:15');
  try {
    assert.deepStrictEqual(
      await adjustOvernight('23:00', '08:00', 1, '2024-01-01'),
      ['23:00', '05:15']
    );
  } finally {
    __setNextDayFirstPunchFetcher();
  }
});

test('adjustOvernight throws on invalid times', async () => {
  await assert.rejects(() => adjustOvernight('99:00', '05:00'));
  await assert.rejects(() => adjustOvernight('23:00', 'aa:bb'));
});
