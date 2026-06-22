import {
  BackgroundTaskRunner,
  ExecutorTimeoutError,
} from './background-task-runner';
import type { BackgroundTaskDefinition } from './task-definition';
import type { BackgroundTask } from './types';

type TestTasks = {
  test: {
    payload: { value: string };
    result: string;
    state?: { runs: number };
  };
};

function createTask(): BackgroundTask<TestTasks, 'test'> {
  return {
    id: 'task-1',
    ownerKey: 'owner-1',
    type: 'test',
    payload: { value: 'hello' },
    state: { runs: 0 },
    status: 'running',
    createdAt: 100,
    updatedAt: 100,
    nextRunAt: null,
    consecutiveFailures: 0,
  };
}

function createDefinition(
  execute: BackgroundTaskDefinition<TestTasks, 'test'>['execute'],
): BackgroundTaskDefinition<TestTasks, 'test'> {
  return {
    executionTimeout: 1_000,
    initialState: () => ({ runs: 0 }),
    execute,
  };
}

describe('BackgroundTaskRunner', () => {
  it('normalizes complete() into a completed outcome', async () => {
    const runner = new BackgroundTaskRunner<TestTasks>();

    const outcome = await runner.run(
      createTask(),
      createDefinition(({ task, complete }) =>
        complete({
          result: task.payload.value,
          state: { runs: task.state.runs + 1 },
        }),
      ),
    );

    expect(outcome).toEqual({
      type: 'completed',
      result: 'hello',
      state: { runs: 1 },
    });
  });

  it('normalizes scheduleNext() into a scheduled outcome', async () => {
    const runner = new BackgroundTaskRunner<TestTasks>();

    const outcome = await runner.run(
      createTask(),
      createDefinition(({ scheduleNext }) =>
        scheduleNext({ state: { runs: 1 } }),
      ),
    );

    expect(outcome).toEqual({
      type: 'scheduled',
      state: { runs: 1 },
    });
  });

  it('normalizes thrown executor errors into failed outcomes', async () => {
    const runner = new BackgroundTaskRunner<TestTasks>();
    const error = new Error('network');

    const outcome = await runner.run(
      createTask(),
      createDefinition(async () => {
        throw error;
      }),
    );

    expect(outcome).toEqual({ type: 'failed', error });
  });

  it('cooperatively aborts an active executor by task id', async () => {
    const runner = new BackgroundTaskRunner<TestTasks>();
    let receivedSignal: AbortSignal | undefined;
    const execution = runner.run(
      createTask(),
      createDefinition(({ signal, scheduleNext }) => {
        receivedSignal = signal;
        return new Promise((resolve) => {
          signal.addEventListener('abort', () => resolve(scheduleNext()));
        });
      }),
    );

    expect(runner.abort('task-1')).toBe(true);
    await expect(execution).resolves.toEqual({ type: 'scheduled' });
    expect(receivedSignal?.aborted).toBe(true);
    expect(runner.abort('task-1')).toBe(false);
  });

  it('aborts at the timeout but holds the execution until the executor settles', async () => {
    vi.useFakeTimers();
    const runner = new BackgroundTaskRunner<TestTasks>();
    let release: (() => void) | undefined;
    let receivedSignal: AbortSignal | undefined;
    let settled = false;
    const execution = runner
      .run(
        createTask(),
        createDefinition(({ signal, complete }) => {
          receivedSignal = signal;
          return new Promise((resolve) => {
            release = () => resolve(complete({ result: 'late' }));
          });
        }),
      )
      .finally(() => {
        settled = true;
      });

    await vi.advanceTimersByTimeAsync(1_000);

    expect(receivedSignal?.aborted).toBe(true);
    expect(receivedSignal?.reason).toBeInstanceOf(ExecutorTimeoutError);
    expect(settled).toBe(false);

    release?.();
    const outcome = await execution;

    expect(outcome.type).toBe('failed');
    expect(outcome.type === 'failed' && outcome.error).toBeInstanceOf(
      ExecutorTimeoutError,
    );
    vi.useRealTimers();
  });
});
