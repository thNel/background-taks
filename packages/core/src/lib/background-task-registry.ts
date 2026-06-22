import { cloneValue, deepFreeze, type InternalTask } from './internal-task';
import type { PersistedTask } from './storage';
import type {
  AnyBackgroundTask,
  BackgroundTask,
  BackgroundTaskStatus,
  TaskType,
} from './types';

const taskStatuses: BackgroundTaskStatus[] = [
  'queued',
  'running',
  'paused',
  'completed',
  'failed',
  'canceled',
];

export class BackgroundTaskRegistry<TTasks> {
  private readonly tasks = new Map<string, InternalTask<TTasks>>();

  add(task: InternalTask<TTasks>): void {
    this.tasks.set(task.id, task);
  }

  has(id: string): boolean {
    return this.tasks.has(id);
  }

  getInternal(id: string): InternalTask<TTasks> | undefined {
    return this.tasks.get(id);
  }

  values(): IterableIterator<InternalTask<TTasks>> {
    return this.tasks.values();
  }

  clear(): void {
    this.tasks.clear();
  }

  getTasks(): readonly AnyBackgroundTask<TTasks>[] {
    return deepFreeze(
      [...this.tasks.values()].map((task) => this.toAnyTaskSnapshot(task)),
    );
  }

  getTask(id: string): AnyBackgroundTask<TTasks> | undefined {
    const task = this.tasks.get(id);
    return task ? this.toAnyTaskSnapshot(task) : undefined;
  }

  getTaskAs<TType extends TaskType<TTasks>>(
    id: string,
    type: TType,
  ): BackgroundTask<TTasks, TType> | undefined {
    const task = this.tasks.get(id);
    return task?.type === type ? this.toTaskSnapshot<TType>(task) : undefined;
  }

  getCounts(): Readonly<Record<BackgroundTaskStatus, number>> {
    const counts = Object.fromEntries(
      taskStatuses.map((status) => [status, 0]),
    ) as Record<BackgroundTaskStatus, number>;

    for (const task of this.tasks.values()) {
      counts[task.status] += 1;
    }

    return deepFreeze(counts);
  }

  toPersistedTask(task: InternalTask<TTasks>): PersistedTask<TTasks> {
    const persisted = {
      id: task.id,
      ownerKey: task.ownerKey,
      fingerprint: task.fingerprint,
      type: task.type,
      payload: cloneValue(task.payload),
      createdAt: task.createdAt,
      updatedAt: task.updatedAt,
      nextRunAt: task.nextRunAt,
      consecutiveFailures: task.consecutiveFailures,
      ...(task.state === undefined ? {} : { state: cloneValue(task.state) }),
    };

    return persisted as PersistedTask<TTasks>;
  }

  toAnyTaskSnapshot(task: InternalTask<TTasks>): AnyBackgroundTask<TTasks> {
    return this.toTaskSnapshot(task) as AnyBackgroundTask<TTasks>;
  }

  toTaskSnapshot<TType extends TaskType<TTasks>>(
    task: InternalTask<TTasks>,
  ): BackgroundTask<TTasks, TType> {
    const snapshot = {
      id: task.id,
      ownerKey: task.ownerKey,
      type: task.type,
      payload: cloneValue(task.payload),
      status: task.status,
      createdAt: task.createdAt,
      updatedAt: task.updatedAt,
      nextRunAt: task.nextRunAt,
      consecutiveFailures: task.consecutiveFailures,
      ...(task.state === undefined ? {} : { state: cloneValue(task.state) }),
      ...(task.result === undefined ? {} : { result: cloneValue(task.result) }),
      ...(task.error === undefined ? {} : { error: task.error }),
    };

    return deepFreeze(snapshot) as BackgroundTask<TTasks, TType>;
  }
}
