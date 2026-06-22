import type {
  BackgroundTaskCoordinator,
  BackgroundTaskCoordinatorEvent,
} from './background-task-coordinator';
import { BackgroundTaskManagerCoordination } from './background-task-manager-coordination';

class PairedCoordinator implements BackgroundTaskCoordinator {
  isLeader = false;
  fencingToken = 0;
  started = false;
  private peer: PairedCoordinator | undefined;
  private readonly listeners = new Set<
    (event: BackgroundTaskCoordinatorEvent) => void
  >();

  async start(): Promise<void> {
    this.started = true;
  }
  async stop(): Promise<void> {
    this.started = false;
  }
  async validateLeadership(): Promise<boolean> {
    return this.isLeader;
  }
  publish(message: unknown): void {
    this.peer?.emit({ type: 'message', message: structuredClone(message) });
  }
  subscribe(
    listener: (event: BackgroundTaskCoordinatorEvent) => void,
  ): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  connect(peer: PairedCoordinator): void {
    this.peer = peer;
    peer.peer = this;
  }
  emit(event: BackgroundTaskCoordinatorEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}

describe('BackgroundTaskManagerCoordination', () => {
  it('transports task events and storage clear without rebroadcast details', async () => {
    const firstCoordinator = new PairedCoordinator();
    const secondCoordinator = new PairedCoordinator();
    firstCoordinator.connect(secondCoordinator);
    const received: unknown[] = [];
    let cleared = 0;
    const first = new BackgroundTaskManagerCoordination({
      coordinator: firstCoordinator,
    });
    const second = new BackgroundTaskManagerCoordination({
      coordinator: secondCoordinator,
      onTaskEvent: (event) => received.push(event),
      onStorageCleared: () => {
        cleared += 1;
      },
    });
    await first.start();
    await second.start();

    first.publishTaskEvent({ type: 'task.enqueued', task: { id: 'task-1' } });
    first.publishStorageCleared();
    await vi.waitFor(() => expect(received).toHaveLength(1));

    expect(received).toEqual([
      { type: 'task.enqueued', task: { id: 'task-1' } },
    ]);
    expect(cleared).toBe(1);
  });

  it('routes a follower command to the leader and waits for acknowledgment', async () => {
    const leaderCoordinator = new PairedCoordinator();
    const followerCoordinator = new PairedCoordinator();
    leaderCoordinator.connect(followerCoordinator);
    leaderCoordinator.isLeader = true;
    const commands: unknown[] = [];
    const leader = new BackgroundTaskManagerCoordination({
      coordinator: leaderCoordinator,
      onCommand: async (command, taskId) => {
        commands.push({ command, taskId });
      },
    });
    const follower = new BackgroundTaskManagerCoordination({
      coordinator: followerCoordinator,
    });
    await leader.start();
    await follower.start();

    await follower.forwardControl('cancel', 'task-1');

    expect(commands).toEqual([{ command: 'cancel', taskId: 'task-1' }]);
  });
});
