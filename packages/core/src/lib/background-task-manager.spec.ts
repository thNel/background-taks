import { BackgroundTaskManager } from './background-task-manager';
import type {
  BackgroundTaskCoordinator,
  BackgroundTaskCoordinatorEvent,
} from './background-task-coordinator';
import { MemoryTaskStorage } from './memory-task-storage';
import type { BackgroundTaskStorage, PersistedTask } from './storage';
import { createTaskFingerprint } from './task-fingerprint';
import type { BackgroundTaskDefinitions } from './task-definition';

type TestTasks = {
  simple: {
    payload: { value: string };
    result: { echoed: string };
  };
  stateful: {
    payload: { startAt: number };
    result: number;
    state?: { step: number };
  };
};

class TestCoordinator implements BackgroundTaskCoordinator {
  isLeader = false;
  fencingToken = 0;
  started = false;
  readonly published: unknown[] = [];
  private readonly listeners = new Set<
    (event: BackgroundTaskCoordinatorEvent) => void
  >();
  private peer: TestCoordinator | undefined;

  async start(): Promise<void> {
    this.started = true;
  }

  async stop(): Promise<void> {
    this.started = false;
    this.setLeader(false);
  }

  async validateLeadership(): Promise<boolean> {
    return this.isLeader;
  }

  publish(message: unknown): void {
    if (!this.started) {
      return;
    }
    this.published.push(message);
    this.peer?.emit({ type: 'message', message: structuredClone(message) });
  }

  subscribe(
    listener: (event: BackgroundTaskCoordinatorEvent) => void,
  ): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  setLeader(isLeader: boolean): void {
    this.isLeader = isLeader;
    this.fencingToken = isLeader ? this.fencingToken + 1 : 0;
    for (const listener of this.listeners) {
      listener({
        type: 'leadership.changed',
        isLeader,
        fencingToken: this.fencingToken,
      });
    }
  }

  emit(event: BackgroundTaskCoordinatorEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  connect(peer: TestCoordinator): void {
    this.peer = peer;
    peer.peer = this;
  }
}

function createDefinitions(
  overrides: {
    [TType in keyof TestTasks]?: Partial<
      BackgroundTaskDefinitions<TestTasks>[TType]
    >;
  } = {},
): BackgroundTaskDefinitions<TestTasks> {
  return {
    simple: {
      executionTimeout: 30_000,
      execute: async ({ task, complete }) =>
        complete({ result: { echoed: task.payload.value } }),
      ...overrides.simple,
    },
    stateful: {
      executionTimeout: 30_000,
      initialState: (payload) => ({ step: payload.startAt }),
      execute: async ({ task, complete }) =>
        complete({
          result: task.state.step + 1,
          state: { step: task.state.step + 1 },
        }),
      ...overrides.stateful,
    },
  };
}

async function waitForTaskStatus(
  manager: BackgroundTaskManager<TestTasks>,
  id: string,
  status: 'completed' | 'failed',
): Promise<void> {
  await vi.waitFor(() => {
    expect(manager.getTask(id)?.status).toBe(status);
  });
}

describe('BackgroundTaskManager', () => {
  it('requires a non-empty ownerKey', () => {
    expect(
      () =>
        new BackgroundTaskManager<TestTasks>({
          ownerKey: '   ',
          definitions: createDefinitions(),
          storage: new MemoryTaskStorage<TestTasks>(),
        }),
    ).toThrowError('ownerKey must not be empty');

    expect(
      () =>
        new BackgroundTaskManager<TestTasks>({
          ownerKey: undefined as unknown as string,
          definitions: createDefinitions(),
          storage: new MemoryTaskStorage<TestTasks>(),
        }),
    ).toThrowError('ownerKey must not be empty');
  });

  it('returns the existing active task for the same owner, type, and payload', async () => {
    const storage = new MemoryTaskStorage<TestTasks>();
    const manager = new BackgroundTaskManager<TestTasks>({
      ownerKey: 'owner-1',
      definitions: createDefinitions(),
      storage,
    });
    const events: string[] = [];
    manager.subscribe((event) => events.push(event.type));

    const first = await manager.enqueue({
      type: 'simple',
      payload: { value: 'same' },
    });
    const duplicate = await manager.enqueue({
      type: 'simple',
      payload: { value: 'same' },
    });

    expect(duplicate.id).toBe(first.id);
    expect(manager.getTasks()).toHaveLength(1);
    expect(await storage.getTasks('owner-1')).toHaveLength(1);
    expect(events).toEqual(['task.enqueued']);
  });

  it('restores only tasks belonging to its owner', async () => {
    const storage = new MemoryTaskStorage<TestTasks>();
    const createPersistedTask = (
      id: string,
      ownerKey: string,
    ): PersistedTask<TestTasks> => ({
      id,
      ownerKey,
      fingerprint: createTaskFingerprint('simple', { value: ownerKey }),
      type: 'simple',
      payload: { value: ownerKey },
      createdAt: 100,
      updatedAt: 100,
      nextRunAt: null,
      consecutiveFailures: 0,
    });
    await storage.putTask(createPersistedTask('owner-1-task', 'owner-1'));
    await storage.putTask(createPersistedTask('owner-2-task', 'owner-2'));
    const first = new BackgroundTaskManager<TestTasks>({
      ownerKey: 'owner-1',
      definitions: createDefinitions(),
      storage,
      coordinator: new TestCoordinator(),
    });
    const second = new BackgroundTaskManager<TestTasks>({
      ownerKey: 'owner-2',
      definitions: createDefinitions(),
      storage,
      coordinator: new TestCoordinator(),
    });

    await first.start();
    await second.start();

    expect(first.getTasks().map((task) => task.id)).toEqual(['owner-1-task']);
    expect(second.getTasks().map((task) => task.id)).toEqual(['owner-2-task']);
    expect(first.getTask('owner-1-task')?.ownerKey).toBe('owner-1');

    await first.stop();
    await second.stop();
  });

  it('persists an enqueued task before starting its executor', async () => {
    const memory = new MemoryTaskStorage<TestTasks>();
    let releaseWrite: (() => void) | undefined;
    const writeGate = new Promise<void>((resolve) => {
      releaseWrite = resolve;
    });
    const execute = vi.fn(async ({ task, complete }) =>
      complete({ result: { echoed: task.payload.value } }),
    );
    const storage: BackgroundTaskStorage<TestTasks> = {
      getTasks: (ownerKey) => memory.getTasks(ownerKey),
      putTaskIfAbsent: async (task: PersistedTask<TestTasks>) => {
        await writeGate;
        return memory.putTaskIfAbsent(task);
      },
      putTask: (task) => memory.putTask(task),
      deleteTask: (id) => memory.deleteTask(id),
      clear: () => memory.clear(),
    };
    const manager = new BackgroundTaskManager<TestTasks>({
      ownerKey: 'owner-1',
      definitions: createDefinitions({ simple: { execute } }),
      storage,
    });
    const eventTypes: string[] = [];
    manager.subscribe((event) => eventTypes.push(event.type));
    await manager.start();

    const enqueuePromise = manager.enqueue({
      type: 'simple',
      payload: { value: 'hello' },
    });
    await Promise.resolve();

    expect(execute).not.toHaveBeenCalled();

    releaseWrite?.();
    const enqueued = await enqueuePromise;
    await waitForTaskStatus(manager, enqueued.id, 'completed');

    expect(enqueued.id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
    expect(execute).toHaveBeenCalledTimes(1);
    expect(await memory.getTasks('owner-1')).toEqual([]);
    expect(eventTypes).toEqual([
      'manager.started',
      'task.enqueued',
      'task.started',
      'task.completed',
    ]);
  });

  it('initializes and updates optional durable state', async () => {
    const manager = new BackgroundTaskManager<TestTasks>({
      ownerKey: 'owner-1',
      definitions: createDefinitions(),
      storage: new MemoryTaskStorage<TestTasks>(),
    });
    await manager.start();

    const enqueued = await manager.enqueue({
      type: 'stateful',
      payload: { startAt: 4 },
    });
    await waitForTaskStatus(manager, enqueued.id, 'completed');

    const task = manager.getTaskAs(enqueued.id, 'stateful');
    expect(task?.state).toEqual({ step: 5 });
    expect(task?.result).toBe(5);
  });

  it('returns undefined when getTaskAs receives the wrong type', async () => {
    const manager = new BackgroundTaskManager<TestTasks>({
      ownerKey: 'owner-1',
      definitions: createDefinitions(),
      storage: new MemoryTaskStorage<TestTasks>(),
    });

    const task = await manager.enqueue({
      type: 'simple',
      payload: { value: 'hello' },
    });

    expect(manager.getTaskAs(task.id, 'stateful')).toBeUndefined();
    expect(manager.getTaskAs(task.id, 'simple')?.payload.value).toBe('hello');
  });

  it('builds an immutable snapshot with status counts', async () => {
    const manager = new BackgroundTaskManager<TestTasks>({
      ownerKey: 'owner-1',
      definitions: createDefinitions(),
      storage: new MemoryTaskStorage<TestTasks>(),
    });

    await manager.enqueue({
      type: 'simple',
      payload: { value: 'hello' },
    });

    const snapshot = manager.getSnapshot();

    expect(snapshot.status).toBe('stopped');
    expect(snapshot.counts.queued).toBe(1);
    expect(snapshot.tasks).toHaveLength(1);
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(Object.isFrozen(snapshot.tasks)).toBe(true);
  });

  describe('scheduler', () => {
    beforeEach(() => {
      vi.useFakeTimers({ now: 0 });
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    async function flushScheduler(): Promise<void> {
      for (let index = 0; index < 10; index += 1) {
        await vi.runAllTicks();
        await Promise.resolve();
      }
    }

    it('uses definition pollingInterval as a delay after executor completion', async () => {
      let executions = 0;
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            pollingInterval: 1_000,
            execute: async ({ task, complete, scheduleNext }) => {
              executions += 1;
              return executions === 1
                ? scheduleNext()
                : complete({ result: { echoed: task.payload.value } });
            },
          },
        }),
        storage: new MemoryTaskStorage<TestTasks>(),
      });
      await manager.start();

      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await flushScheduler();

      expect(executions).toBe(1);
      expect(manager.getTask(task.id)?.status).toBe('running');
      expect(manager.getTask(task.id)?.nextRunAt).toBe(1_000);

      await vi.advanceTimersByTimeAsync(999);
      expect(executions).toBe(1);

      await vi.advanceTimersByTimeAsync(1);
      await flushScheduler();

      expect(executions).toBe(2);
      expect(manager.getTask(task.id)?.status).toBe('completed');
    });

    it('limits only simultaneous executor calls', async () => {
      const releases: Array<() => void> = [];
      const execute = vi.fn(
        ({ task, complete }) =>
          new Promise<ReturnType<typeof complete>>((resolve) => {
            releases.push(() =>
              resolve(complete({ result: { echoed: task.payload.value } })),
            );
          }),
      );
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({ simple: { execute } }),
        storage: new MemoryTaskStorage<TestTasks>(),
        maxConcurrentExecutions: 3,
      });
      await manager.start();

      const tasks = await Promise.all(
        ['one', 'two', 'three', 'four'].map((value) =>
          manager.enqueue({ type: 'simple', payload: { value } }),
        ),
      );
      await flushScheduler();

      expect(execute).toHaveBeenCalledTimes(3);

      releases.shift()?.();
      await flushScheduler();

      expect(execute).toHaveBeenCalledTimes(4);

      for (const release of releases) {
        release();
      }
      await flushScheduler();
      expect(
        tasks.map((task) => manager.getTask(task.id)?.status),
      ).toEqual(['completed', 'completed', 'completed', 'completed']);
    });

    it('fails immediately when maxRetries is absent', async () => {
      const execute = vi.fn(async () => {
        throw new Error('network');
      });
      const storage = new MemoryTaskStorage<TestTasks>();
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({ simple: { execute } }),
        storage,
      });
      await manager.start();

      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await flushScheduler();

      expect(execute).toHaveBeenCalledTimes(1);
      expect(manager.getTask(task.id)?.status).toBe('failed');
      expect(await storage.getTasks('owner-1')).toEqual([]);
    });

    it('uses maxRetries as both retry switch and retry count', async () => {
      let executions = 0;
      const storage = new MemoryTaskStorage<TestTasks>();
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            maxRetries: 2,
            execute: async ({ task, complete }) => {
              executions += 1;
              if (executions < 3) {
                throw new Error('network');
              }
              return complete({ result: { echoed: task.payload.value } });
            },
          },
        }),
        storage,
      });
      await manager.start();

      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await flushScheduler();

      expect(executions).toBe(1);
      expect((await storage.getTasks('owner-1'))[0]).toMatchObject({
        consecutiveFailures: 1,
        nextRunAt: 1_000,
      });

      await vi.advanceTimersByTimeAsync(1_000);
      await flushScheduler();
      expect(executions).toBe(2);
      expect((await storage.getTasks('owner-1'))[0]).toMatchObject({
        consecutiveFailures: 2,
        nextRunAt: 3_000,
      });

      await vi.advanceTimersByTimeAsync(2_000);
      await flushScheduler();

      expect(executions).toBe(3);
      expect(manager.getTask(task.id)?.status).toBe('completed');
      expect(await storage.getTasks('owner-1')).toEqual([]);
    });

    it('fails immediately when retryPolicy classifies an error as terminal', async () => {
      const error = new Error('bad request');
      const shouldRetry = vi.fn(() => false);
      const getDelay = vi.fn(() => 1_000);
      const execute = vi.fn(async () => {
        throw error;
      });
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            maxRetries: 3,
            retryPolicy: { shouldRetry, getDelay },
            execute,
          },
        }),
        storage: new MemoryTaskStorage<TestTasks>(),
      });
      await manager.start();

      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await flushScheduler();

      expect(manager.getTask(task.id)?.status).toBe('failed');
      expect(execute).toHaveBeenCalledTimes(1);
      expect(shouldRetry).toHaveBeenCalledWith(
        error,
        expect.objectContaining({ retryNumber: 1, maxRetries: 3 }),
      );
      expect(getDelay).not.toHaveBeenCalled();
    });

    it('uses retryPolicy delay for the next retry', async () => {
      const getDelay = vi.fn(() => 2_500);
      let executions = 0;
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            maxRetries: 1,
            retryPolicy: { getDelay },
            execute: async ({ task, complete }) => {
              executions += 1;
              if (executions === 1) {
                throw new Error('temporary');
              }
              return complete({ result: { echoed: task.payload.value } });
            },
          },
        }),
        storage: new MemoryTaskStorage<TestTasks>(),
      });
      await manager.start();

      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await flushScheduler();

      expect(manager.getTask(task.id)?.nextRunAt).toBe(2_500);
      expect(getDelay).toHaveBeenCalledWith(
        expect.objectContaining({ retryNumber: 1, maxRetries: 1 }),
      );

      await vi.advanceTimersByTimeAsync(2_500);
      await flushScheduler();

      expect(executions).toBe(2);
      expect(manager.getTask(task.id)?.status).toBe('completed');
    });

    it('resets consecutive failures after a successful polling tick', async () => {
      let executions = 0;
      const storage = new MemoryTaskStorage<TestTasks>();
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            maxRetries: 1,
            pollingInterval: 5_000,
            execute: async ({ scheduleNext }) => {
              executions += 1;
              if (executions === 1) {
                throw new Error('network');
              }
              return scheduleNext();
            },
          },
        }),
        storage,
      });
      await manager.start();

      await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await flushScheduler();
      await vi.advanceTimersByTimeAsync(1_000);
      await flushScheduler();

      expect((await storage.getTasks('owner-1'))[0]).toMatchObject({
        consecutiveFailures: 0,
        nextRunAt: 6_000,
      });
    });
  });

  describe('controls', () => {
    it('pauses an active task after its aborted executor settles and resumes it', async () => {
      let executions = 0;
      let receivedSignal: AbortSignal | undefined;
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            pollingInterval: 0,
            execute: async ({ task, signal, complete, scheduleNext }) => {
              executions += 1;
              if (executions > 1) {
                return complete({ result: { echoed: task.payload.value } });
              }

              receivedSignal = signal;
              return new Promise((resolve) => {
                signal.addEventListener('abort', () => resolve(scheduleNext()));
              });
            },
          },
        }),
        storage: new MemoryTaskStorage<TestTasks>(),
      });
      await manager.start();
      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await vi.waitFor(() => expect(executions).toBe(1));

      await manager.pause(task.id);

      expect(receivedSignal?.aborted).toBe(true);
      expect(manager.getTask(task.id)?.status).toBe('paused');

      await manager.resume(task.id);
      await waitForTaskStatus(manager, task.id, 'completed');
      expect(executions).toBe(2);
    });

    it('allows successful completion to win over a pause request', async () => {
      let release: (() => void) | undefined;
      let receivedSignal: AbortSignal | undefined;
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            execute: ({ task, signal, complete }) => {
              receivedSignal = signal;
              return new Promise((resolve) => {
                release = () =>
                  resolve(complete({ result: { echoed: task.payload.value } }));
              });
            },
          },
        }),
        storage: new MemoryTaskStorage<TestTasks>(),
      });
      await manager.start();
      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await vi.waitFor(() => expect(receivedSignal).toBeDefined());

      const pausePromise = manager.pause(task.id);
      release?.();
      await pausePromise;

      expect(receivedSignal?.aborted).toBe(true);
      expect(manager.getTask(task.id)?.status).toBe('completed');
    });

    it('gives cancel priority over a completed executor result', async () => {
      let release: (() => void) | undefined;
      let receivedSignal: AbortSignal | undefined;
      const storage = new MemoryTaskStorage<TestTasks>();
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            execute: ({ task, signal, complete }) => {
              receivedSignal = signal;
              return new Promise((resolve) => {
                release = () =>
                  resolve(complete({ result: { echoed: task.payload.value } }));
              });
            },
          },
        }),
        storage,
      });
      await manager.start();
      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await vi.waitFor(() => expect(receivedSignal).toBeDefined());

      const cancelPromise = manager.cancel(task.id);
      release?.();
      await cancelPromise;

      expect(receivedSignal?.aborted).toBe(true);
      expect(manager.getTask(task.id)?.status).toBe('canceled');
      expect(manager.getTask(task.id)?.result).toBeUndefined();
      expect(await storage.getTasks('owner-1')).toEqual([]);
    });

    it('aborts active work and returns unfinished tasks to queued on stop', async () => {
      let receivedSignal: AbortSignal | undefined;
      const storage = new MemoryTaskStorage<TestTasks>();
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            pollingInterval: 1_000,
            execute: async ({ signal, scheduleNext }) => {
              receivedSignal = signal;
              return new Promise((resolve) => {
                signal.addEventListener('abort', () => resolve(scheduleNext()));
              });
            },
          },
        }),
        storage,
      });
      await manager.start();
      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await vi.waitFor(() => expect(receivedSignal).toBeDefined());

      await manager.stop();

      expect(receivedSignal?.aborted).toBe(true);
      expect(manager.getSnapshot().status).toBe('stopped');
      expect(manager.getTask(task.id)?.status).toBe('queued');
      expect(await storage.getTasks('owner-1')).toHaveLength(1);
    });

    it('stops active work before clearing storage and runtime tasks', async () => {
      let release: (() => void) | undefined;
      let receivedSignal: AbortSignal | undefined;
      const storage = new MemoryTaskStorage<TestTasks>();
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            execute: ({ task, signal, complete }) => {
              receivedSignal = signal;
              return new Promise((resolve) => {
                release = () =>
                  resolve(complete({ result: { echoed: task.payload.value } }));
              });
            },
          },
        }),
        storage,
      });
      await manager.start();
      await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await vi.waitFor(() => expect(receivedSignal).toBeDefined());

      const clearPromise = manager.clearStorage();
      release?.();
      await clearPromise;

      expect(receivedSignal?.aborted).toBe(true);
      expect(manager.getSnapshot().status).toBe('stopped');
      expect(manager.getTasks()).toEqual([]);
      expect(await storage.getTasks('owner-1')).toEqual([]);
    });
  });

  describe('recovery and browser signals', () => {
    afterEach(() => {
      vi.useRealTimers();
      vi.unstubAllGlobals();
    });

    it('restores persisted work as queued while preserving scheduler metadata', async () => {
      vi.useFakeTimers({ now: 1_000 });
      const execute = vi.fn(async ({ task, complete }) =>
        complete({ result: { echoed: task.payload.value } }),
      );
      const storage = new MemoryTaskStorage<TestTasks>();
      await storage.putTask({
        id: 'persisted-task',
        ownerKey: 'owner-1',
        fingerprint: createTaskFingerprint('simple', { value: 'hello' }),
        type: 'simple',
        payload: { value: 'hello' },
        createdAt: 100,
        updatedAt: 500,
        nextRunAt: 2_000,
        consecutiveFailures: 2,
      });
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({ simple: { execute } }),
        storage,
      });

      await manager.start();

      expect(manager.getTask('persisted-task')).toMatchObject({
        status: 'queued',
        nextRunAt: 2_000,
        consecutiveFailures: 2,
      });
      expect(execute).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1_000);
      await vi.runAllTicks();

      expect(execute).toHaveBeenCalledTimes(1);
      expect(manager.getTask('persisted-task')?.status).toBe('completed');
    });

    it('keeps unknown task types in storage and continues recovery', async () => {
      const storage = new MemoryTaskStorage<TestTasks>();
      await storage.putTask({
        id: 'removed-task',
        ownerKey: 'owner-1',
        fingerprint: createTaskFingerprint('removed', {}),
        type: 'removed',
        payload: {},
        createdAt: 100,
        updatedAt: 100,
        nextRunAt: null,
        consecutiveFailures: 0,
      } as unknown as PersistedTask<TestTasks>);
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions(),
        storage,
      });
      const events: string[] = [];
      manager.subscribe((event) => events.push(event.type));

      await manager.start();

      expect(manager.getTasks()).toEqual([]);
      expect((await storage.getTasks('owner-1')).map((task) => task.id)).toEqual([
        'removed-task',
      ]);
      expect(events).toEqual(['manager.recoveryError', 'manager.started']);
    });

    it('holds queued work offline and wakes it when the browser comes online', async () => {
      const browser = new EventTarget();
      const navigatorState = { onLine: false };
      vi.stubGlobal('window', browser);
      vi.stubGlobal('navigator', navigatorState);
      const execute = vi.fn(async ({ task, complete }) =>
        complete({ result: { echoed: task.payload.value } }),
      );
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({ simple: { execute } }),
        storage: new MemoryTaskStorage<TestTasks>(),
      });
      await manager.start();

      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await Promise.resolve();

      expect(execute).not.toHaveBeenCalled();
      expect(manager.getTask(task.id)?.status).toBe('queued');

      navigatorState.onLine = true;
      browser.dispatchEvent(new Event('online'));
      await waitForTaskStatus(manager, task.id, 'completed');

      expect(execute).toHaveBeenCalledTimes(1);
    });

    it('reconciles overdue polling work when the tab becomes visible', async () => {
      vi.useFakeTimers({ now: 0 });
      const browser = new EventTarget();
      const documentState = new EventTarget() as EventTarget & {
        visibilityState: string;
      };
      documentState.visibilityState = 'hidden';
      vi.stubGlobal('window', browser);
      vi.stubGlobal('navigator', { onLine: true });
      vi.stubGlobal('document', documentState);
      let executions = 0;
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            pollingInterval: 1_000,
            execute: async ({ task, complete, scheduleNext }) => {
              executions += 1;
              return executions === 1
                ? scheduleNext()
                : complete({ result: { echoed: task.payload.value } });
            },
          },
        }),
        storage: new MemoryTaskStorage<TestTasks>(),
      });
      await manager.start();
      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      for (let index = 0; index < 5; index += 1) {
        await vi.runAllTicks();
        await Promise.resolve();
      }
      expect(executions).toBe(1);

      vi.setSystemTime(1_000);
      documentState.visibilityState = 'visible';
      documentState.dispatchEvent(new Event('visibilitychange'));
      for (let index = 0; index < 5; index += 1) {
        await vi.runAllTicks();
        await Promise.resolve();
      }

      expect(executions).toBe(2);
      expect(manager.getTask(task.id)?.status).toBe('completed');
    });
  });

  describe('coordination', () => {
    it('keeps follower tasks queued and executes them after promotion', async () => {
      const coordinator = new TestCoordinator();
      const execute = vi.fn(async ({ task, complete }) =>
        complete({ result: { echoed: task.payload.value } }),
      );
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({ simple: { execute } }),
        storage: new MemoryTaskStorage<TestTasks>(),
        coordinator,
      });
      await manager.start();

      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await Promise.resolve();

      expect(execute).not.toHaveBeenCalled();
      expect(manager.getTask(task.id)?.status).toBe('queued');

      coordinator.setLeader(true);
      await waitForTaskStatus(manager, task.id, 'completed');
      expect(execute).toHaveBeenCalledTimes(1);
    });

    it('quiesces active work when leadership is lost', async () => {
      const coordinator = new TestCoordinator();
      coordinator.setLeader(true);
      let receivedSignal: AbortSignal | undefined;
      const manager = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions({
          simple: {
            pollingInterval: 1_000,
            execute: async ({ signal, scheduleNext }) => {
              receivedSignal = signal;
              return new Promise((resolve) => {
                signal.addEventListener('abort', () => resolve(scheduleNext()));
              });
            },
          },
        }),
        storage: new MemoryTaskStorage<TestTasks>(),
        coordinator,
      });
      await manager.start();
      const task = await manager.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await vi.waitFor(() => expect(receivedSignal).toBeDefined());

      coordinator.setLeader(false);

      await vi.waitFor(() =>
        expect(manager.getTask(task.id)?.status).toBe('queued'),
      );
      expect(receivedSignal?.aborted).toBe(true);
      expect(manager.getSnapshot().status).toBe('running');
    });

    it('preserves deduplication metadata when a follower becomes leader', async () => {
      const leaderCoordinator = new TestCoordinator();
      const followerCoordinator = new TestCoordinator();
      leaderCoordinator.connect(followerCoordinator);
      leaderCoordinator.setLeader(true);
      const storage = new MemoryTaskStorage<TestTasks>();
      const definitions = createDefinitions({
        simple: {
          pollingInterval: 60_000,
          execute: async ({ scheduleNext }) => scheduleNext(),
        },
      });
      const leader = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions,
        storage,
        coordinator: leaderCoordinator,
      });
      const follower = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions,
        storage,
        coordinator: followerCoordinator,
      });
      await leader.start();
      await follower.start();
      const original = await leader.enqueue({
        type: 'simple',
        payload: { value: 'same' },
      });
      await vi.waitFor(() =>
        expect(follower.getTask(original.id)?.nextRunAt).not.toBeNull(),
      );

      leaderCoordinator.setLeader(false);
      followerCoordinator.setLeader(true);
      for (let index = 0; index < 10; index += 1) {
        await Promise.resolve();
      }
      await follower.pause(original.id);

      const duplicate = await follower.enqueue({
        type: 'simple',
        payload: { value: 'same' },
      });

      expect(duplicate.id).toBe(original.id);
      expect(await storage.getTasks('owner-1')).toHaveLength(1);

      await leader.stop();
      await follower.stop();
    });

    it('executes a follower enqueue in the leader and syncs completion back', async () => {
      const leaderCoordinator = new TestCoordinator();
      const followerCoordinator = new TestCoordinator();
      leaderCoordinator.connect(followerCoordinator);
      leaderCoordinator.setLeader(true);
      const storage = new MemoryTaskStorage<TestTasks>();
      const execute = vi.fn(async ({ task, complete }) =>
        complete({ result: { echoed: task.payload.value } }),
      );
      const definitions = createDefinitions({ simple: { execute } });
      const leader = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions,
        storage,
        coordinator: leaderCoordinator,
      });
      const follower = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions,
        storage,
        coordinator: followerCoordinator,
      });
      await leader.start();
      await follower.start();

      const task = await follower.enqueue({
        type: 'simple',
        payload: { value: 'from-follower' },
      });

      await waitForTaskStatus(leader, task.id, 'completed');
      await waitForTaskStatus(follower, task.id, 'completed');

      expect(execute).toHaveBeenCalledTimes(1);
      expect(follower.getTaskAs(task.id, 'simple')?.result).toEqual({
        echoed: 'from-follower',
      });

      await leader.stop();
      await follower.stop();
    });

    it('propagates full runtime/storage clear to another tab', async () => {
      const firstCoordinator = new TestCoordinator();
      const secondCoordinator = new TestCoordinator();
      firstCoordinator.connect(secondCoordinator);
      firstCoordinator.setLeader(true);
      const storage = new MemoryTaskStorage<TestTasks>();
      const first = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions(),
        storage,
        coordinator: firstCoordinator,
      });
      const second = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions: createDefinitions(),
        storage,
        coordinator: secondCoordinator,
      });
      await first.start();
      await second.start();
      const task = await second.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await waitForTaskStatus(first, task.id, 'completed');
      await waitForTaskStatus(second, task.id, 'completed');

      await second.clearStorage();

      await vi.waitFor(() => expect(first.getTasks()).toEqual([]));
      expect(await storage.getTasks('owner-1')).toEqual([]);

      await first.stop();
      await second.stop();
    });

    it('broadcasts clear after active work is quiesced but before channel stop', async () => {
      const leaderCoordinator = new TestCoordinator();
      const followerCoordinator = new TestCoordinator();
      leaderCoordinator.connect(followerCoordinator);
      leaderCoordinator.setLeader(true);
      const storage = new MemoryTaskStorage<TestTasks>();
      const definitions = createDefinitions({
        simple: {
          pollingInterval: 1_000,
          execute: async ({ signal, scheduleNext }) =>
            new Promise((resolve) => {
              signal.addEventListener('abort', () => resolve(scheduleNext()));
            }),
        },
      });
      const leader = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions,
        storage,
        coordinator: leaderCoordinator,
      });
      const follower = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions,
        storage,
        coordinator: followerCoordinator,
      });
      await leader.start();
      await follower.start();
      await leader.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await vi.waitFor(() => expect(follower.getTasks()).toHaveLength(1));

      await leader.clearStorage();

      await vi.waitFor(() => expect(follower.getTasks()).toEqual([]));
      expect(leader.getSnapshot().status).toBe('stopped');

      await follower.stop();
    });

    it('aborts leader work before applying a hard reset from another tab', async () => {
      const leaderCoordinator = new TestCoordinator();
      const followerCoordinator = new TestCoordinator();
      leaderCoordinator.connect(followerCoordinator);
      leaderCoordinator.setLeader(true);
      const storage = new MemoryTaskStorage<TestTasks>();
      let receivedSignal: AbortSignal | undefined;
      const definitions = createDefinitions({
        simple: {
          pollingInterval: 1_000,
          execute: async ({ signal, scheduleNext }) => {
            receivedSignal = signal;
            return new Promise((resolve) => {
              signal.addEventListener('abort', () => resolve(scheduleNext()));
            });
          },
        },
      });
      const leader = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions,
        storage,
        coordinator: leaderCoordinator,
      });
      const follower = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions,
        storage,
        coordinator: followerCoordinator,
      });
      await leader.start();
      await follower.start();
      await leader.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await vi.waitFor(() => expect(receivedSignal).toBeDefined());

      await follower.clearStorage();

      await vi.waitFor(() => expect(receivedSignal?.aborted).toBe(true));
      await vi.waitFor(() => expect(leader.getTasks()).toEqual([]));
      expect(await storage.getTasks('owner-1')).toEqual([]);

      await leader.stop();
    });

    it('routes follower cancel to the active leader executor', async () => {
      const leaderCoordinator = new TestCoordinator();
      const followerCoordinator = new TestCoordinator();
      leaderCoordinator.connect(followerCoordinator);
      leaderCoordinator.setLeader(true);
      const storage = new MemoryTaskStorage<TestTasks>();
      let receivedSignal: AbortSignal | undefined;
      const definitions = createDefinitions({
        simple: {
          pollingInterval: 1_000,
          execute: async ({ signal, scheduleNext }) => {
            receivedSignal = signal;
            return new Promise((resolve) => {
              signal.addEventListener('abort', () => resolve(scheduleNext()));
            });
          },
        },
      });
      const leader = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions,
        storage,
        coordinator: leaderCoordinator,
      });
      const follower = new BackgroundTaskManager<TestTasks>({
        ownerKey: 'owner-1',
        definitions,
        storage,
        coordinator: followerCoordinator,
      });
      await leader.start();
      await follower.start();
      const task = await follower.enqueue({
        type: 'simple',
        payload: { value: 'hello' },
      });
      await vi.waitFor(() => expect(receivedSignal).toBeDefined());

      await follower.cancel(task.id);

      expect(receivedSignal?.aborted).toBe(true);
      expect(leader.getTask(task.id)?.status).toBe('canceled');
      expect(follower.getTask(task.id)?.status).toBe('canceled');

      await leader.stop();
      await follower.stop();
    });
  });
});
