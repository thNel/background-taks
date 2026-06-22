import { createTaskFingerprint } from './task-fingerprint';

describe('createTaskFingerprint', () => {
  it('is stable for equivalent payloads with different object key order', () => {
    expect(
      createTaskFingerprint('report', {
        filters: { to: '2026-06-22', from: '2026-06-01' },
        reportId: '42',
      }),
    ).toBe(
      createTaskFingerprint('report', {
        reportId: '42',
        filters: { from: '2026-06-01', to: '2026-06-22' },
      }),
    );
  });

  it('distinguishes task types and payload values', () => {
    const original = createTaskFingerprint('report', { reportId: '42' });

    expect(createTaskFingerprint('email', { reportId: '42' })).not.toBe(
      original,
    );
    expect(createTaskFingerprint('report', { reportId: '43' })).not.toBe(
      original,
    );
  });

  it('uses the JSON representation of values with toJSON', () => {
    const date = new Date('2026-06-22T10:00:00.000Z');

    expect(createTaskFingerprint('report', { date })).toBe(
      createTaskFingerprint('report', { date: date.toJSON() }),
    );
  });
});
