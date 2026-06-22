import type {
  BackgroundTaskStorage,
  PersistedTask,
  PutTaskIfAbsentResult,
} from './storage';

const ownerIndexName = 'ownerKey';
const activeTaskIndexName = 'activeTask';

export interface IndexedDbTaskStorageOptions {
  databaseName: string;
  storeName?: string;
  version?: number;
  indexedDB?: IDBFactory;
}

export class IndexedDbTaskStorage<TTasks>
  implements BackgroundTaskStorage<TTasks>
{
  private readonly databaseName: string;
  private readonly storeName: string;
  private readonly version: number;
  private readonly indexedDB: IDBFactory;

  constructor(options: IndexedDbTaskStorageOptions) {
    if (!options.databaseName.trim()) {
      throw new TypeError('databaseName must not be empty');
    }
    if (options.storeName !== undefined && !options.storeName.trim()) {
      throw new TypeError('storeName must not be empty');
    }
    if (
      options.version !== undefined &&
      (!Number.isInteger(options.version) || options.version < 1)
    ) {
      throw new RangeError('version must be a positive integer');
    }

    const factory = options.indexedDB ?? globalThis.indexedDB;
    if (!factory) {
      throw new Error('IndexedDB is not available in this environment');
    }

    this.databaseName = options.databaseName;
    this.storeName = options.storeName ?? 'tasks';
    this.version = options.version ?? 2;
    this.indexedDB = factory;
  }

  async getTasks(ownerKey: string): Promise<PersistedTask<TTasks>[]> {
    const database = await this.openDatabase();
    try {
      return await new Promise<PersistedTask<TTasks>[]>((resolve, reject) => {
        const transaction = database.transaction(this.storeName, 'readonly');
        const request = transaction
          .objectStore(this.storeName)
          .index(ownerIndexName)
          .getAll(ownerKey);
        let records: PersistedTask<TTasks>[] = [];

        request.onsuccess = () => {
          records = request.result as PersistedTask<TTasks>[];
        };
        request.onerror = () => reject(request.error ?? new Error('Read failed'));
        transaction.oncomplete = () => resolve(records);
        transaction.onerror = () =>
          reject(transaction.error ?? new Error('Read transaction failed'));
        transaction.onabort = () =>
          reject(transaction.error ?? new Error('Read transaction aborted'));
      });
    } finally {
      database.close();
    }
  }

  async putTaskIfAbsent(
    task: PersistedTask<TTasks>,
  ): Promise<PutTaskIfAbsentResult<TTasks>> {
    const database = await this.openDatabase();
    try {
      return await new Promise<PutTaskIfAbsentResult<TTasks>>(
        (resolve, reject) => {
          const transaction = database.transaction(
            this.storeName,
            'readwrite',
          );
          const store = transaction.objectStore(this.storeName);
          const request = store
            .index(activeTaskIndexName)
            .get([task.ownerKey, task.fingerprint]);
          let result: PutTaskIfAbsentResult<TTasks> | undefined;

          request.onsuccess = () => {
            const existing = request.result as PersistedTask<TTasks> | undefined;
            if (existing) {
              result = { created: false, task: existing };
              return;
            }

            const addRequest = store.add(task);
            addRequest.onsuccess = () => {
              result = { created: true, task };
            };
            addRequest.onerror = () =>
              reject(addRequest.error ?? new Error('Task creation failed'));
          };
          request.onerror = () =>
            reject(request.error ?? new Error('Task lookup failed'));
          transaction.oncomplete = () => {
            if (!result) {
              reject(new Error('Task creation transaction produced no result'));
              return;
            }
            resolve(structuredClone(result));
          };
          transaction.onerror = () =>
            reject(
              transaction.error ?? new Error('Task creation transaction failed'),
            );
          transaction.onabort = () =>
            reject(
              transaction.error ??
                new Error('Task creation transaction aborted'),
            );
        },
      );
    } finally {
      database.close();
    }
  }

  async putTask(task: PersistedTask<TTasks>): Promise<void> {
    await this.runWrite((store) => store.put(task));
  }

  async deleteTask(id: string): Promise<void> {
    await this.runWrite((store) => store.delete(id));
  }

  async clear(): Promise<void> {
    await this.runWrite((store) => store.clear());
  }

  private async runWrite(
    operation: (store: IDBObjectStore) => IDBRequest,
  ): Promise<void> {
    const database = await this.openDatabase();
    try {
      await new Promise<void>((resolve, reject) => {
        const transaction = database.transaction(this.storeName, 'readwrite');
        const request = operation(transaction.objectStore(this.storeName));

        request.onerror = () =>
          reject(request.error ?? new Error('Write request failed'));
        transaction.oncomplete = () => resolve();
        transaction.onerror = () =>
          reject(transaction.error ?? new Error('Write transaction failed'));
        transaction.onabort = () =>
          reject(transaction.error ?? new Error('Write transaction aborted'));
      });
    } finally {
      database.close();
    }
  }

  private openDatabase(): Promise<IDBDatabase> {
    return new Promise((resolve, reject) => {
      const request = this.indexedDB.open(this.databaseName, this.version);

      request.onupgradeneeded = () => {
        const database = request.result;
        const store = database.objectStoreNames.contains(this.storeName)
          ? request.transaction?.objectStore(this.storeName)
          : database.createObjectStore(this.storeName, { keyPath: 'id' });
        if (!store) {
          return;
        }
        if (!store.indexNames.contains(ownerIndexName)) {
          store.createIndex(ownerIndexName, 'ownerKey');
        }
        if (!store.indexNames.contains(activeTaskIndexName)) {
          store.createIndex(
            activeTaskIndexName,
            ['ownerKey', 'fingerprint'],
            { unique: true },
          );
        }
      };
      request.onsuccess = () => {
        const database = request.result;
        if (!database.objectStoreNames.contains(this.storeName)) {
          database.close();
          reject(
            new Error(
              `IndexedDB object store "${this.storeName}" does not exist; increment the storage version to create it`,
            ),
          );
          return;
        }
        resolve(database);
      };
      request.onerror = () =>
        reject(request.error ?? new Error('Failed to open IndexedDB'));
      request.onblocked = () =>
        reject(new Error(`Opening IndexedDB "${this.databaseName}" was blocked`));
    });
  }
}
