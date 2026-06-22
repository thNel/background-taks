import { BackgroundTaskRegistry } from './background-task-registry';
import type { InternalTask } from './internal-task';

type TestTasks = {
  simple: {
    payload: { value: string };
    result: string;
  };
  stateful: {
    payload: { id: number };
    result: number;
    state?: { step: number };
  };
};

function createTask(
  overrides: Partial<InternalTask<TestTasks>> = {},
): InternalTask<TestTasks> {
  return {
    id: 'task-1',
    ownerKey: 'owner-1',
    fingerprint: 'simple:{"value":"hello"}',
    type: 'simple',
    payload: { value: 'hello' },
    status: 'queued',
    createdAt: 100,
    updatedAt: 100,
    nextRunAt: null,
    consecutiveFailures: 0,
    ...overrides,
  };
}

describe('BackgroundTaskRegistry', () => {
  it('owns tasks and narrows snapshots by runtime type', () => {
    const registry = new BackgroundTaskRegistry<TestTasks>();
    registry.add(createTask());

    expect(registry.getTaskAs('task-1', 'stateful')).toBeUndefined();
    expect(registry.getTaskAs('task-1', 'simple')?.payload.value).toBe(
      'hello',
    );
  });

  it('returns detached deeply frozen public snapshots', () => {
    const registry = new BackgroundTaskRegistry<TestTasks>();
    const task = createTask();
    registry.add(task);

    const snapshot = registry.getTask('task-1');
    task.payload = { value: 'changed' };

    expect(snapshot?.payload).toEqual({ value: 'hello' });
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(Object.isFrozen(snapshot?.payload)).toBe(true);
    expect(Object.isFrozen(registry.getTasks())).toBe(true);
  });

  it('counts every public status', () => {
    const registry = new BackgroundTaskRegistry<TestTasks>();
    registry.add(createTask({ id: 'queued', status: 'queued' }));
    registry.add(createTask({ id: 'running', status: 'running' }));
    registry.add(createTask({ id: 'failed', status: 'failed' }));

    expect(registry.getCounts()).toEqual({
      queued: 1,
      running: 1,
      paused: 0,
      completed: 0,
      failed: 1,
      canceled: 0,
    });
  });

  it('converts runtime tasks to status-free persisted records', () => {
    const registry = new BackgroundTaskRegistry<TestTasks>();
    const task = createTask({
      type: 'stateful',
      payload: { id: 42 },
      state: { step: 3 },
      status: 'running',
      result: 99,
      error: new Error('ignored'),
      nextRunAt: 500,
      consecutiveFailures: 2,
    });

    expect(registry.toPersistedTask(task)).toEqual({
      id: 'task-1',
      ownerKey: 'owner-1',
      fingerprint: 'simple:{"value":"hello"}',
      type: 'stateful',
      payload: { id: 42 },
      state: { step: 3 },
      createdAt: 100,
      updatedAt: 100,
      nextRunAt: 500,
      consecutiveFailures: 2,
    });
  });

  it('clears every runtime task', () => {
    const registry = new BackgroundTaskRegistry<TestTasks>();
    registry.add(createTask());

    registry.clear();

    expect(registry.getTasks()).toEqual([]);
  });
});
