import { BackgroundTaskScheduler } from './background-task-scheduler';

interface TestTask {
  id: string;
  status: 'queued' | 'running' | 'completed';
  nextRunAt: number | null;
  createdAt: number;
  priority?: number;
}

async function flushScheduler(): Promise<void> {
  for (let index = 0; index < 10; index += 1) {
    if (vi.isFakeTimers()) {
      await vi.runAllTicks();
    }
    await Promise.resolve();
  }
}

describe('BackgroundTaskScheduler', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('limits simultaneous executions and fills released slots', async () => {
    const tasks: TestTask[] = ['one', 'two', 'three', 'four'].map((id, index) => ({
      id,
      status: 'queued',
      nextRunAt: null,
      createdAt: index,
    }));
    const releases: Array<() => void> = [];
    const execute = vi.fn(
      (task: TestTask) =>
        new Promise<void>((resolve) => {
          releases.push(() => {
            task.status = 'completed';
            resolve();
          });
        }),
    );
    const scheduler = new BackgroundTaskScheduler({
      maxConcurrentExecutions: 3,
      getTasks: () => tasks,
      getPriority: (task) => task.priority ?? 0,
      execute,
    });

    scheduler.start();
    await flushScheduler();
    expect(execute).toHaveBeenCalledTimes(3);

    releases.shift()?.();
    await flushScheduler();
    expect(execute).toHaveBeenCalledTimes(4);

    for (const release of releases) {
      release();
    }
    await scheduler.stop();
  });

  it('wakes a future task when nextRunAt becomes due', async () => {
    vi.useFakeTimers({ now: 1_000 });
    const task: TestTask = {
      id: 'future',
      status: 'running',
      nextRunAt: 2_000,
      createdAt: 0,
    };
    const execute = vi.fn(async () => {
      task.status = 'completed';
    });
    const scheduler = new BackgroundTaskScheduler({
      maxConcurrentExecutions: 3,
      getTasks: () => [task],
      getPriority: (candidate) => candidate.priority ?? 0,
      execute,
    });

    scheduler.start();
    await flushScheduler();
    expect(execute).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1_000);
    await flushScheduler();
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('holds work offline and wakes it on the online event', async () => {
    const browser = new EventTarget();
    const navigatorState = { onLine: false };
    vi.stubGlobal('window', browser);
    vi.stubGlobal('navigator', navigatorState);
    const task: TestTask = {
      id: 'offline',
      status: 'queued',
      nextRunAt: null,
      createdAt: 0,
    };
    const execute = vi.fn(async () => {
      task.status = 'completed';
    });
    const scheduler = new BackgroundTaskScheduler({
      maxConcurrentExecutions: 3,
      getTasks: () => [task],
      getPriority: (candidate) => candidate.priority ?? 0,
      execute,
    });

    scheduler.start();
    await flushScheduler();
    expect(execute).not.toHaveBeenCalled();

    navigatorState.onLine = true;
    browser.dispatchEvent(new Event('online'));
    await flushScheduler();
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('reconciles overdue work when the document becomes visible', async () => {
    vi.useFakeTimers({ now: 1_000 });
    const documentState = new EventTarget() as EventTarget & {
      visibilityState: string;
    };
    documentState.visibilityState = 'hidden';
    vi.stubGlobal('document', documentState);
    const task: TestTask = {
      id: 'hidden',
      status: 'running',
      nextRunAt: 2_000,
      createdAt: 0,
    };
    const execute = vi.fn(async () => {
      task.status = 'completed';
    });
    const scheduler = new BackgroundTaskScheduler({
      maxConcurrentExecutions: 3,
      getTasks: () => [task],
      getPriority: (candidate) => candidate.priority ?? 0,
      execute,
    });
    scheduler.start();
    await flushScheduler();

    vi.setSystemTime(2_000);
    documentState.visibilityState = 'visible';
    documentState.dispatchEvent(new Event('visibilitychange'));
    await flushScheduler();

    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('runs higher priorities first and uses creation time as the FIFO tie-breaker', async () => {
    const tasks: TestTask[] = [
      {
        id: 'low',
        status: 'queued',
        nextRunAt: null,
        createdAt: 1,
        priority: 0,
      },
      {
        id: 'newer-high',
        status: 'queued',
        nextRunAt: null,
        createdAt: 3,
        priority: 10,
      },
      {
        id: 'older-high',
        status: 'queued',
        nextRunAt: null,
        createdAt: 2,
        priority: 10,
      },
    ];
    const order: string[] = [];
    const scheduler = new BackgroundTaskScheduler({
      maxConcurrentExecutions: 1,
      getTasks: () => tasks,
      getPriority: (task) => task.priority ?? 0,
      execute: async (task) => {
        order.push(task.id);
        task.status = 'completed';
      },
    });

    scheduler.start();
    await flushScheduler();
    await scheduler.stop();

    expect(order).toEqual(['older-high', 'newer-high', 'low']);
  });

  it('preserves enqueue order when priority and creation time are equal', async () => {
    const tasks: TestTask[] = ['z-first', 'a-second'].map((id) => ({
      id,
      status: 'queued',
      nextRunAt: null,
      createdAt: 1,
      priority: 0,
    }));
    const order: string[] = [];
    const scheduler = new BackgroundTaskScheduler({
      maxConcurrentExecutions: 1,
      getTasks: () => tasks,
      getPriority: (task) => task.priority ?? 0,
      execute: async (task) => {
        order.push(task.id);
        task.status = 'completed';
      },
    });

    scheduler.start();
    await flushScheduler();
    await scheduler.stop();

    expect(order).toEqual(['z-first', 'a-second']);
  });
});
