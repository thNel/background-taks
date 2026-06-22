import { MemoryTaskStorage } from './memory-task-storage';
import type { PersistedTask } from './storage';

type TestTasks = {
  test: {
    payload: { value: number };
    result: string;
    state?: { step: number };
  };
};

function createTask(id: string, value = 1): PersistedTask<TestTasks> {
  return {
    id,
    ownerKey: 'owner-1',
    fingerprint: `test:${value}`,
    type: 'test',
    payload: { value },
    state: { step: 0 },
    createdAt: 100,
    updatedAt: 100,
    nextRunAt: null,
    consecutiveFailures: 0,
  };
}

describe('MemoryTaskStorage', () => {
  it('stores cloned records instead of caller-owned references', async () => {
    const storage = new MemoryTaskStorage<TestTasks>();
    const task = createTask('task-1');

    await storage.putTask(task);
    task.payload.value = 99;

    const [stored] = await storage.getTasks('owner-1');

    expect(stored.payload).toEqual({ value: 1 });
  });

  it('returns cloned records instead of internal references', async () => {
    const storage = new MemoryTaskStorage<TestTasks>();
    await storage.putTask(createTask('task-1'));

    const [firstRead] = await storage.getTasks('owner-1');
    firstRead.payload.value = 99;

    const [secondRead] = await storage.getTasks('owner-1');

    expect(secondRead.payload).toEqual({ value: 1 });
  });

  it('replaces a record with the same id', async () => {
    const storage = new MemoryTaskStorage<TestTasks>();

    await storage.putTask(createTask('task-1', 1));
    await storage.putTask(createTask('task-1', 2));

    const tasks = await storage.getTasks('owner-1');

    expect(tasks).toHaveLength(1);
    expect(tasks[0].payload).toEqual({ value: 2 });
  });

  it('deletes one record without exposing point deletion publicly', async () => {
    const storage = new MemoryTaskStorage<TestTasks>();
    await storage.putTask(createTask('task-1'));
    await storage.putTask(createTask('task-2', 2));

    await storage.deleteTask('task-1');

    expect((await storage.getTasks('owner-1')).map((task) => task.id)).toEqual([
      'task-2',
    ]);
  });

  it('clears every record', async () => {
    const storage = new MemoryTaskStorage<TestTasks>();
    await storage.putTask(createTask('task-1'));
    await storage.putTask(createTask('task-2', 2));

    await storage.clear();

    expect(await storage.getTasks('owner-1')).toEqual([]);
  });

  it('returns an existing active task for the same owner and fingerprint', async () => {
    const storage = new MemoryTaskStorage<TestTasks>();
    const first = createTask('task-1');
    const duplicate = { ...createTask('task-2'), payload: { value: 99 } };

    await expect(storage.putTaskIfAbsent(first)).resolves.toEqual({
      created: true,
      task: first,
    });
    await expect(storage.putTaskIfAbsent(duplicate)).resolves.toEqual({
      created: false,
      task: first,
    });
  });

  it('isolates owner reads and fingerprints while clear remains global', async () => {
    const storage = new MemoryTaskStorage<TestTasks>();
    const first = createTask('task-1');
    const second = {
      ...createTask('task-2'),
      ownerKey: 'owner-2',
    };

    await storage.putTaskIfAbsent(first);
    await storage.putTaskIfAbsent(second);

    expect(await storage.getTasks('owner-1')).toEqual([first]);
    expect(await storage.getTasks('owner-2')).toEqual([second]);

    await storage.clear();

    expect(await storage.getTasks('owner-1')).toEqual([]);
    expect(await storage.getTasks('owner-2')).toEqual([]);
  });
});
