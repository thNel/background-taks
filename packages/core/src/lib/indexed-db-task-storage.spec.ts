import { IDBFactory } from 'fake-indexeddb';
import { IndexedDbTaskStorage } from './indexed-db-task-storage';
import type { PersistedTask } from './storage';

type TestTasks = {
  test: {
    payload: { value: number };
    result: string;
  };
};

function createTask(id: string, value = 1): PersistedTask<TestTasks> {
  return {
    id,
    ownerKey: 'owner-1',
    fingerprint: `test:${value}`,
    type: 'test',
    payload: { value },
    createdAt: 100,
    updatedAt: 100,
    nextRunAt: null,
    consecutiveFailures: 0,
  };
}

describe('IndexedDbTaskStorage', () => {
  it('puts and replaces records by id', async () => {
    const indexedDB = new IDBFactory();
    const storage = new IndexedDbTaskStorage<TestTasks>({
      databaseName: 'put-test',
      indexedDB,
    });

    await storage.putTask(createTask('task-1', 1));
    await storage.putTask(createTask('task-1', 2));

    expect(await storage.getTasks('owner-1')).toEqual([createTask('task-1', 2)]);
  });

  it('deletes one internal record and clears the full store', async () => {
    const indexedDB = new IDBFactory();
    const storage = new IndexedDbTaskStorage<TestTasks>({
      databaseName: 'delete-test',
      indexedDB,
    });
    await storage.putTask(createTask('task-1'));
    await storage.putTask(createTask('task-2', 2));

    await storage.deleteTask('task-1');
    expect((await storage.getTasks('owner-1')).map((task) => task.id)).toEqual([
      'task-2',
    ]);

    await storage.clear();
    expect(await storage.getTasks('owner-1')).toEqual([]);
  });

  it('restores records through a new adapter instance', async () => {
    const indexedDB = new IDBFactory();
    const first = new IndexedDbTaskStorage<TestTasks>({
      databaseName: 'reopen-test',
      indexedDB,
    });
    await first.putTask(createTask('task-1'));

    const reopened = new IndexedDbTaskStorage<TestTasks>({
      databaseName: 'reopen-test',
      indexedDB,
    });

    expect(await reopened.getTasks('owner-1')).toEqual([createTask('task-1')]);
  });

  it('supports a configurable object store name', async () => {
    const indexedDB = new IDBFactory();
    const storage = new IndexedDbTaskStorage<TestTasks>({
      databaseName: 'custom-store-test',
      storeName: 'background-work',
      indexedDB,
    });

    await storage.putTask(createTask('task-1'));

    expect(await storage.getTasks('owner-1')).toEqual([createTask('task-1')]);
  });

  it('atomically returns an existing task for the same owner and fingerprint', async () => {
    const indexedDB = new IDBFactory();
    const storage = new IndexedDbTaskStorage<TestTasks>({
      databaseName: 'deduplicate-test',
      indexedDB,
    });
    const first = createTask('task-1');
    const duplicate = { ...createTask('task-2'), payload: { value: 99 } };

    const [firstResult, secondResult] = await Promise.all([
      storage.putTaskIfAbsent(first),
      storage.putTaskIfAbsent(duplicate),
    ]);

    expect([firstResult.created, secondResult.created].sort()).toEqual([
      false,
      true,
    ]);
    expect(firstResult.task.id).toBe(secondResult.task.id);
    expect(await storage.getTasks('owner-1')).toHaveLength(1);
  });

  it('isolates owner reads and allows the same fingerprint for another owner', async () => {
    const indexedDB = new IDBFactory();
    const storage = new IndexedDbTaskStorage<TestTasks>({
      databaseName: 'owner-test',
      indexedDB,
    });
    const first = createTask('task-1');
    const second = {
      ...createTask('task-2'),
      ownerKey: 'owner-2',
    };

    await storage.putTaskIfAbsent(first);
    await storage.putTaskIfAbsent(second);

    expect(await storage.getTasks('owner-1')).toEqual([first]);
    expect(await storage.getTasks('owner-2')).toEqual([second]);
  });
});
