export interface RetryDelayContext {
  retryNumber: number;
}

export interface RetryJitterOptions {
  jitter?: number;
}

export interface ExponentialRetryDelayOptions extends RetryJitterOptions {
  initialDelay: number;
  maxDelay: number;
}

function validateDelay(name: string, value: number): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative finite number`);
  }
}

function validateJitter(jitter: number): void {
  if (!Number.isFinite(jitter) || jitter < 0 || jitter > 1) {
    throw new RangeError('jitter must be between 0 and 1');
  }
}

function applyJitter(delay: number, jitter: number): number {
  if (jitter === 0) {
    return delay;
  }

  const multiplier = 1 - jitter + 2 * jitter * Math.random();
  return Math.round(delay * multiplier);
}

export function fixedRetryDelay(
  delay: number,
  options: RetryJitterOptions = {},
): (context: RetryDelayContext) => number {
  validateDelay('delay', delay);
  const jitter = options.jitter ?? 0;
  validateJitter(jitter);

  return () => applyJitter(delay, jitter);
}

export function exponentialRetryDelay(
  options: ExponentialRetryDelayOptions,
): (context: RetryDelayContext) => number {
  validateDelay('initialDelay', options.initialDelay);
  validateDelay('maxDelay', options.maxDelay);
  if (options.maxDelay < options.initialDelay) {
    throw new RangeError('maxDelay must be greater than or equal to initialDelay');
  }
  const jitter = options.jitter ?? 0;
  validateJitter(jitter);

  return ({ retryNumber }) => {
    if (!Number.isInteger(retryNumber) || retryNumber < 1) {
      throw new RangeError('retryNumber must be a positive integer');
    }
    const delay = Math.min(
      options.initialDelay * 2 ** (retryNumber - 1),
      options.maxDelay,
    );
    return applyJitter(delay, jitter);
  };
}
