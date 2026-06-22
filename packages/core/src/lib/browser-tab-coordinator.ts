import type {
  BackgroundTaskCoordinator,
  BackgroundTaskCoordinatorEvent,
  BroadcastChannelLike,
  BroadcastChannelMessageEvent,
} from './background-task-coordinator';

interface LeaderLease {
  key: 'leader';
  ownerId: string;
  expiresAt: number;
  fencingToken: number;
}

type ChannelEnvelope =
  | { scope: 'coordination'; source: string; type: 'lease.changed' }
  | { scope: 'application'; source: string; message: unknown };

export interface BrowserTabCoordinatorOptions {
  namespace: string;
  tabId?: string;
  indexedDB?: IDBFactory;
  heartbeatInterval?: number;
  leaseDuration?: number;
  broadcastChannelFactory?: (name: string) => BroadcastChannelLike;
}

const storeName = 'leases';
const leaseKey = 'leader';

export class BrowserTabCoordinator implements BackgroundTaskCoordinator {
  readonly tabId: string;

  private readonly namespace: string;
  private readonly indexedDB: IDBFactory;
  private readonly heartbeatInterval: number;
  private readonly leaseDuration: number;
  private readonly broadcastChannelFactory?: (
    name: string,
  ) => BroadcastChannelLike;
  private readonly listeners = new Set<
    (event: BackgroundTaskCoordinatorEvent) => void
  >();
  private channel: BroadcastChannelLike | undefined;
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  private started = false;
  private leader = false;
  private token = 0;
  private validation: Promise<boolean> | undefined;
  private readonly handleMessage = (
    event: BroadcastChannelMessageEvent,
  ): void => {
    const envelope = event.data as Partial<ChannelEnvelope> | null;
    if (!envelope || envelope.source === this.tabId) {
      return;
    }

    if (
      envelope.scope === 'coordination' &&
      envelope.type === 'lease.changed'
    ) {
      void this.validateLeadership().catch(() => undefined);
      return;
    }

    if (envelope.scope === 'application' && 'message' in envelope) {
      this.emit({ type: 'message', message: envelope.message });
    }
  };

  constructor(options: BrowserTabCoordinatorOptions) {
    if (!options.namespace.trim()) {
      throw new TypeError('namespace must not be empty');
    }

    const factory = options.indexedDB ?? globalThis.indexedDB;
    if (!factory) {
      throw new Error('IndexedDB is not available in this environment');
    }

    this.namespace = options.namespace;
    this.tabId = options.tabId ?? crypto.randomUUID();
    this.indexedDB = factory;
    this.heartbeatInterval = options.heartbeatInterval ?? 2_000;
    this.leaseDuration = options.leaseDuration ?? 6_000;

    if (
      !Number.isFinite(this.heartbeatInterval) ||
      this.heartbeatInterval <= 0
    ) {
      throw new RangeError('heartbeatInterval must be a positive number');
    }
    if (
      !Number.isFinite(this.leaseDuration) ||
      this.leaseDuration <= this.heartbeatInterval
    ) {
      throw new RangeError(
        'leaseDuration must be greater than heartbeatInterval',
      );
    }

    this.broadcastChannelFactory =
      options.broadcastChannelFactory ?? this.getDefaultChannelFactory();
  }

  get isLeader(): boolean {
    return this.leader;
  }

  get fencingToken(): number {
    return this.token;
  }

  async start(): Promise<void> {
    if (this.started) {
      return;
    }

    this.started = true;
    this.channel = this.broadcastChannelFactory?.(
      `@background-tasks:${this.namespace}`,
    );
    this.channel?.addEventListener('message', this.handleMessage);

    try {
      await this.validateLeadership();
      this.heartbeatTimer = setInterval(
        () => void this.heartbeat(),
        this.heartbeatInterval,
      );
    } catch (error) {
      this.started = false;
      this.channel?.removeEventListener('message', this.handleMessage);
      this.channel?.close();
      this.channel = undefined;
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (!this.started) {
      return;
    }

    this.started = false;
    if (this.heartbeatTimer !== undefined) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }

    if (this.leader) {
      await this.releaseLease();
    }
    this.setLeadership(false, 0);
    this.channel?.removeEventListener('message', this.handleMessage);
    this.channel?.close();
    this.channel = undefined;
  }

  validateLeadership(): Promise<boolean> {
    if (this.validation) {
      return this.validation;
    }

    const validation = this.acquireOrRenewLease().finally(() => {
      if (this.validation === validation) {
        this.validation = undefined;
      }
    });
    this.validation = validation;
    return validation;
  }

  publish(message: unknown): void {
    if (!this.started || !this.channel) {
      return;
    }

    try {
      this.channel.postMessage({
        scope: 'application',
        source: this.tabId,
        message,
      } satisfies ChannelEnvelope);
    } catch {
      // Coordination remains correct through the IndexedDB lease and pulses.
    }
  }

  subscribe(
    listener: (event: BackgroundTaskCoordinatorEvent) => void,
  ): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private async heartbeat(): Promise<void> {
    try {
      await this.validateLeadership();
      this.emit({ type: 'pulse', isLeader: this.leader });
    } catch {
      this.setLeadership(false, 0);
    }
  }

  private async acquireOrRenewLease(): Promise<boolean> {
    const database = await this.openDatabase();
    try {
      const lease = await new Promise<LeaderLease | undefined>(
        (resolve, reject) => {
          const transaction = database.transaction(storeName, 'readwrite');
          const store = transaction.objectStore(storeName);
          const getRequest = store.get(leaseKey);
          let nextLease: LeaderLease | undefined;

          getRequest.onsuccess = () => {
            const current = getRequest.result as LeaderLease | undefined;
            const now = Date.now();
            if (
              !current ||
              current.ownerId === this.tabId ||
              current.expiresAt <= now
            ) {
              nextLease = {
                key: leaseKey,
                ownerId: this.tabId,
                expiresAt: now + this.leaseDuration,
                fencingToken:
                  current?.ownerId === this.tabId
                    ? current.fencingToken
                    : (current?.fencingToken ?? 0) + 1,
              };
              store.put(nextLease);
            }
          };
          getRequest.onerror = () =>
            reject(getRequest.error ?? new Error('Failed to read leader lease'));
          transaction.oncomplete = () => resolve(nextLease);
          transaction.onerror = () =>
            reject(
              transaction.error ?? new Error('Leader lease transaction failed'),
            );
          transaction.onabort = () =>
            reject(
              transaction.error ?? new Error('Leader lease transaction aborted'),
            );
        },
      );

      const isLeader = lease?.ownerId === this.tabId;
      this.setLeadership(isLeader, lease?.fencingToken ?? 0);
      if (isLeader) {
        this.announceLeaseChange();
      }
      return isLeader;
    } finally {
      database.close();
    }
  }

  private async releaseLease(): Promise<void> {
    const database = await this.openDatabase();
    try {
      await new Promise<void>((resolve, reject) => {
        const transaction = database.transaction(storeName, 'readwrite');
        const store = transaction.objectStore(storeName);
        const getRequest = store.get(leaseKey);

        getRequest.onsuccess = () => {
          const current = getRequest.result as LeaderLease | undefined;
          if (current?.ownerId === this.tabId) {
            store.put({
              ...current,
              ownerId: '',
              expiresAt: 0,
            } satisfies LeaderLease);
          }
        };
        getRequest.onerror = () =>
          reject(getRequest.error ?? new Error('Failed to read leader lease'));
        transaction.oncomplete = () => resolve();
        transaction.onerror = () =>
          reject(transaction.error ?? new Error('Lease release failed'));
        transaction.onabort = () =>
          reject(transaction.error ?? new Error('Lease release aborted'));
      });
      this.announceLeaseChange();
    } finally {
      database.close();
    }
  }

  private announceLeaseChange(): void {
    try {
      this.channel?.postMessage({
        scope: 'coordination',
        source: this.tabId,
        type: 'lease.changed',
      } satisfies ChannelEnvelope);
    } catch {
      // Followers will detect the lease change on their next heartbeat.
    }
  }

  private setLeadership(isLeader: boolean, fencingToken: number): void {
    const changed = this.leader !== isLeader;
    this.leader = isLeader;
    this.token = isLeader ? fencingToken : 0;
    if (changed) {
      this.emit({
        type: 'leadership.changed',
        isLeader,
        fencingToken: this.token,
      });
    }
  }

  private emit(event: BackgroundTaskCoordinatorEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  private openDatabase(): Promise<IDBDatabase> {
    return new Promise((resolve, reject) => {
      const request = this.indexedDB.open(
        `@background-tasks-coordinator:${this.namespace}`,
        1,
      );
      request.onupgradeneeded = () => {
        if (!request.result.objectStoreNames.contains(storeName)) {
          request.result.createObjectStore(storeName, { keyPath: 'key' });
        }
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () =>
        reject(request.error ?? new Error('Failed to open coordination DB'));
      request.onblocked = () =>
        reject(new Error('Opening coordination DB was blocked'));
    });
  }

  private getDefaultChannelFactory():
    | ((name: string) => BroadcastChannelLike)
    | undefined {
    if (typeof BroadcastChannel === 'undefined') {
      return undefined;
    }
    return (name) => new BroadcastChannel(name);
  }
}
