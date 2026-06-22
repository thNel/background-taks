export interface SchedulableTask {
  id: string;
  status: string;
  nextRunAt: number | null;
  createdAt: number;
}

export interface BackgroundTaskSchedulerOptions<TTask extends SchedulableTask> {
  maxConcurrentExecutions: number;
  getTasks: () => Iterable<TTask>;
  getPriority: (task: TTask) => number;
  execute: (task: TTask) => Promise<void>;
}

export class BackgroundTaskScheduler<TTask extends SchedulableTask> {
  private readonly maxConcurrentExecutions: number;
  private readonly getTasks: () => Iterable<TTask>;
  private readonly getPriority: (task: TTask) => number;
  private readonly execute: (task: TTask) => Promise<void>;
  private readonly executions = new Map<string, Promise<void>>();
  private running = false;
  private pumpScheduled = false;
  private wakeTimer: ReturnType<typeof setTimeout> | undefined;
  private browserListenersAttached = false;
  private readonly handleOnline = (): void => this.wake();
  private readonly handleVisibilityChange = (): void => {
    if (typeof document !== 'undefined' && document.visibilityState === 'visible') {
      this.wake();
    }
  };

  constructor(options: BackgroundTaskSchedulerOptions<TTask>) {
    if (
      !Number.isInteger(options.maxConcurrentExecutions) ||
      options.maxConcurrentExecutions < 1
    ) {
      throw new RangeError('maxConcurrentExecutions must be a positive integer');
    }

    this.maxConcurrentExecutions = options.maxConcurrentExecutions;
    this.getTasks = options.getTasks;
    this.getPriority = options.getPriority;
    this.execute = options.execute;
  }

  start(): void {
    if (this.running) {
      return;
    }

    this.running = true;
    this.attachBrowserListeners();
    this.wake();
  }

  async stop(abort?: (id: string) => void): Promise<void> {
    this.running = false;
    this.detachBrowserListeners();
    if (this.wakeTimer !== undefined) {
      clearTimeout(this.wakeTimer);
      this.wakeTimer = undefined;
    }

    if (abort) {
      for (const id of this.executions.keys()) {
        abort(id);
      }
    }

    await Promise.allSettled([...this.executions.values()]);
  }

  wake(): void {
    if (!this.running || this.pumpScheduled) {
      return;
    }

    this.pumpScheduled = true;
    queueMicrotask(() => {
      this.pumpScheduled = false;
      this.pump();
    });
  }

  getExecution(id: string): Promise<void> | undefined {
    return this.executions.get(id);
  }

  private pump(): void {
    if (!this.running || !this.isOnline()) {
      return;
    }

    if (this.wakeTimer !== undefined) {
      clearTimeout(this.wakeTimer);
      this.wakeTimer = undefined;
    }

    const now = Date.now();
    const candidates = [...this.getTasks()]
      .filter(
        (task) =>
          (task.status === 'queued' || task.status === 'running') &&
          !this.executions.has(task.id) &&
          (task.nextRunAt === null || task.nextRunAt <= now),
      )
      .sort(
        (left, right) =>
          this.getPriority(right) - this.getPriority(left) ||
          left.createdAt - right.createdAt,
      );

    for (const task of candidates) {
      if (this.executions.size >= this.maxConcurrentExecutions) {
        break;
      }

      const execution = this.execute(task);
      this.executions.set(task.id, execution);
      void execution.then(
        () => this.finishExecution(task.id, execution),
        () => this.finishExecution(task.id, execution),
      );
    }

    this.scheduleWakeTimer();
  }

  private finishExecution(id: string, execution: Promise<void>): void {
    if (this.executions.get(id) === execution) {
      this.executions.delete(id);
    }
    this.wake();
  }

  private scheduleWakeTimer(): void {
    if (!this.running || !this.isOnline()) {
      return;
    }

    const now = Date.now();
    let earliest: number | undefined;
    for (const task of this.getTasks()) {
      if (
        (task.status !== 'queued' && task.status !== 'running') ||
        this.executions.has(task.id) ||
        task.nextRunAt === null ||
        task.nextRunAt <= now
      ) {
        continue;
      }

      earliest =
        earliest === undefined
          ? task.nextRunAt
          : Math.min(earliest, task.nextRunAt);
    }

    if (earliest !== undefined) {
      this.wakeTimer = setTimeout(
        () => {
          this.wakeTimer = undefined;
          this.wake();
        },
        Math.max(0, earliest - now),
      );
    }
  }

  private isOnline(): boolean {
    return typeof navigator === 'undefined' || navigator.onLine !== false;
  }

  private attachBrowserListeners(): void {
    if (this.browserListenersAttached) {
      return;
    }

    if (typeof window !== 'undefined') {
      window.addEventListener('online', this.handleOnline);
    }
    if (typeof document !== 'undefined') {
      document.addEventListener(
        'visibilitychange',
        this.handleVisibilityChange,
      );
    }
    this.browserListenersAttached = true;
  }

  private detachBrowserListeners(): void {
    if (!this.browserListenersAttached) {
      return;
    }

    if (typeof window !== 'undefined') {
      window.removeEventListener('online', this.handleOnline);
    }
    if (typeof document !== 'undefined') {
      document.removeEventListener(
        'visibilitychange',
        this.handleVisibilityChange,
      );
    }
    this.browserListenersAttached = false;
  }
}
