import type {
  BackgroundTaskStorage,
  PersistedTask,
  PutTaskIfAbsentResult,
} from './storage';

function clone<T>(value: T): T {
  return structuredClone(value);
}

export class MemoryTaskStorage<TTasks>
  implements BackgroundTaskStorage<TTasks>
{
  private readonly tasks = new Map<string, PersistedTask<TTasks>>();

  async getTasks(ownerKey: string): Promise<PersistedTask<TTasks>[]> {
    return [...this.tasks.values()]
      .filter((task) => task.ownerKey === ownerKey)
      .map(clone);
  }

  async putTaskIfAbsent(
    task: PersistedTask<TTasks>,
  ): Promise<PutTaskIfAbsentResult<TTasks>> {
    const existing = [...this.tasks.values()].find(
      (candidate) =>
        candidate.ownerKey === task.ownerKey &&
        candidate.fingerprint === task.fingerprint,
    );
    if (existing) {
      return { created: false, task: clone(existing) };
    }

    const stored = clone(task);
    this.tasks.set(task.id, stored);
    return { created: true, task: clone(stored) };
  }

  async putTask(task: PersistedTask<TTasks>): Promise<void> {
    this.tasks.set(task.id, clone(task));
  }

  async deleteTask(id: string): Promise<void> {
    this.tasks.delete(id);
  }

  async clear(): Promise<void> {
    this.tasks.clear();
  }
}
