const { adjustOvernight, __setNextDayFirstPunchFetcher } = require('../src/calculate');

describe('adjustOvernight', () => {
  test('returns same values for same-day shifts', async () => {
    await expect(adjustOvernight('09:00', '17:00')).resolves.toEqual(['09:00', '17:00']);
  });

  test('passes through shifts crossing midnight', async () => {
    await expect(adjustOvernight('23:00', '02:00')).resolves.toEqual(['23:00', '02:00']);
  });

  test('applies 06:30 cutoff when no next-day punch', async () => {
    await expect(adjustOvernight('23:00', '08:00')).resolves.toEqual(['23:00', '06:30']);
  });

  test('uses next-day punch before cutoff', async () => {
    __setNextDayFirstPunchFetcher(async () => '05:15');
    try {
      await expect(adjustOvernight('23:00', '08:00', 1, '2024-01-01')).resolves.toEqual(['23:00', '05:15']);
    } finally {
      __setNextDayFirstPunchFetcher();
    }
  });

  test('defaults to cutoff when next-day punch missing', async () => {
    __setNextDayFirstPunchFetcher(async () => null);
    try {
      await expect(adjustOvernight('23:00', '08:00', 1, '2024-01-01')).resolves.toEqual(['23:00', '06:30']);
    } finally {
      __setNextDayFirstPunchFetcher();
    }
  });

  test('throws on invalid times', async () => {
    await expect(adjustOvernight('99:00', '05:00')).rejects.toThrow('Invalid time format');
    await expect(adjustOvernight('23:00', 'aa:bb')).rejects.toThrow('Invalid time format');
  });
});
