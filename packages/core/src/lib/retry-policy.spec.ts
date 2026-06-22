import {
  exponentialRetryDelay,
  fixedRetryDelay,
} from './retry-policy';

describe('retry delay helpers', () => {
  it('returns a fixed delay', () => {
    const getDelay = fixedRetryDelay(2_500);

    expect(getDelay({ retryNumber: 1 })).toBe(2_500);
    expect(getDelay({ retryNumber: 5 })).toBe(2_500);
  });

  it('applies capped exponential backoff', () => {
    const getDelay = exponentialRetryDelay({
      initialDelay: 1_000,
      maxDelay: 3_000,
    });

    expect(getDelay({ retryNumber: 1 })).toBe(1_000);
    expect(getDelay({ retryNumber: 2 })).toBe(2_000);
    expect(getDelay({ retryNumber: 3 })).toBe(3_000);
  });

  it('applies symmetric jitter around the calculated delay', () => {
    const random = vi.spyOn(Math, 'random');
    const getDelay = fixedRetryDelay(1_000, { jitter: 0.2 });

    random.mockReturnValueOnce(0).mockReturnValueOnce(1);

    expect(getDelay({ retryNumber: 1 })).toBe(800);
    expect(getDelay({ retryNumber: 1 })).toBe(1_200);
    random.mockRestore();
  });
});
