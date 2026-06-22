import { IDBFactory } from 'fake-indexeddb';
import type { BroadcastChannelLike } from './background-task-coordinator';
import { BrowserTabCoordinator } from './browser-tab-coordinator';

class FakeBroadcastChannel implements BroadcastChannelLike {
  private static readonly channels = new Map<
    string,
    Set<FakeBroadcastChannel>
  >();

  private readonly listeners = new Set<
    (event: { data: unknown }) => void
  >();

  constructor(private readonly name: string) {
    const channels = FakeBroadcastChannel.channels.get(name) ?? new Set();
    channels.add(this);
    FakeBroadcastChannel.channels.set(name, channels);
  }

  postMessage(message: unknown): void {
    for (const channel of FakeBroadcastChannel.channels.get(this.name) ?? []) {
      if (channel === this) {
        continue;
      }
      const event = { data: structuredClone(message) };
      queueMicrotask(() => channel.receive(event));
    }
  }

  addEventListener(
    _type: 'message',
    listener: (event: { data: unknown }) => void,
  ): void {
    this.listeners.add(listener);
  }

  removeEventListener(
    _type: 'message',
    listener: (event: { data: unknown }) => void,
  ): void {
    this.listeners.delete(listener);
  }

  close(): void {
    FakeBroadcastChannel.channels.get(this.name)?.delete(this);
  }

  static reset(): void {
    FakeBroadcastChannel.channels.clear();
  }

  private receive(event: { data: unknown }): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}

describe('BrowserTabCoordinator', () => {
  const coordinators: BrowserTabCoordinator[] = [];

  afterEach(async () => {
    await Promise.all(coordinators.splice(0).map((item) => item.stop()));
    FakeBroadcastChannel.reset();
  });

  function createCoordinator(
    indexedDB: IDBFactory,
    namespace: string,
    tabId: string,
  ): BrowserTabCoordinator {
    const coordinator = new BrowserTabCoordinator({
      namespace,
      tabId,
      indexedDB,
      heartbeatInterval: 1_000,
      leaseDuration: 3_000,
      broadcastChannelFactory: (name) => new FakeBroadcastChannel(name),
    });
    coordinators.push(coordinator);
    return coordinator;
  }

  it('elects exactly one leader through the IndexedDB lease', async () => {
    const indexedDB = new IDBFactory();
    const first = createCoordinator(indexedDB, 'single-leader', 'tab-a');
    const second = createCoordinator(indexedDB, 'single-leader', 'tab-b');

    await Promise.all([first.start(), second.start()]);

    expect([first.isLeader, second.isLeader].filter(Boolean)).toHaveLength(1);
    const leader = first.isLeader ? first : second;
    expect(await leader.validateLeadership()).toBe(true);
    expect(leader.fencingToken).toBeGreaterThan(0);
  });

  it('releases leadership and allows a follower to take over', async () => {
    const indexedDB = new IDBFactory();
    const first = createCoordinator(indexedDB, 'takeover', 'tab-a');
    const second = createCoordinator(indexedDB, 'takeover', 'tab-b');
    await first.start();
    await second.start();
    expect(first.isLeader).toBe(true);
    expect(second.isLeader).toBe(false);
    const firstToken = first.fencingToken;

    await first.stop();

    await vi.waitFor(() => expect(second.isLeader).toBe(true));
    expect(second.fencingToken).toBeGreaterThan(firstToken);
  });

  it('transports application messages without exposing lease messages', async () => {
    const indexedDB = new IDBFactory();
    const first = createCoordinator(indexedDB, 'messages', 'tab-a');
    const second = createCoordinator(indexedDB, 'messages', 'tab-b');
    const received: unknown[] = [];
    second.subscribe((event) => {
      if (event.type === 'message') {
        received.push(event.message);
      }
    });
    await first.start();
    await second.start();

    first.publish({ type: 'task.changed', taskId: 'task-1' });
    await vi.waitFor(() => expect(received).toHaveLength(1));

    expect(received).toEqual([
      { type: 'task.changed', taskId: 'task-1' },
    ]);
  });
});
