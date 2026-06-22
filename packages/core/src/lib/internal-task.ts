import type { BackgroundTaskStatus, TaskType } from './types';

export interface InternalTask<TTasks> {
  id: string;
  ownerKey: string;
  fingerprint: string;
  type: TaskType<TTasks>;
  payload: unknown;
  state?: unknown;
  status: BackgroundTaskStatus;
  result?: unknown;
  error?: unknown;
  createdAt: number;
  updatedAt: number;
  nextRunAt: number | null;
  consecutiveFailures: number;
  pauseRequested?: boolean;
  cancelRequested?: boolean;
}

export function cloneValue<T>(value: T): T {
  return structuredClone(value);
}

export function deepFreeze<T>(value: T): T {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) {
      deepFreeze(child);
    }
  }

  return value;
}
