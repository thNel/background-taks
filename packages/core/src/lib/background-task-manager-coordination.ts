import type {
  BackgroundTaskCoordinator,
  BackgroundTaskCoordinatorEvent,
} from './background-task-coordinator';

export type TaskControlCommand = 'pause' | 'resume' | 'cancel';

type CoordinationMessage<TTaskEvent> =
  | {
      protocol: '@background-tasks/core/v1';
      type: 'task.event';
      event: TTaskEvent;
    }
  | {
      protocol: '@background-tasks/core/v1';
      type: 'storage.cleared';
    }
  | {
      protocol: '@background-tasks/core/v1';
      type: 'task.command';
      requestId: string;
      command: TaskControlCommand;
      taskId: string;
    }
  | {
      protocol: '@background-tasks/core/v1';
      type: 'task.command.ack';
      requestId: string;
      error?: string;
    };

export interface BackgroundTaskManagerCoordinationOptions<TTaskEvent> {
  coordinator: BackgroundTaskCoordinator;
  onLeadershipChange?: (isLeader: boolean) => void | Promise<void>;
  onTaskEvent?: (event: TTaskEvent) => void;
  onStorageCleared?: () => void | Promise<void>;
  onPulse?: (isLeader: boolean) => void | Promise<void>;
  onCommand?: (
    command: TaskControlCommand,
    taskId: string,
  ) => void | Promise<void>;
}

export class BackgroundTaskManagerCoordination<TTaskEvent = unknown> {
  private readonly coordinator: BackgroundTaskCoordinator;
  private readonly options: BackgroundTaskManagerCoordinationOptions<TTaskEvent>;
  private readonly pendingCommands = new Map<
    string,
    {
      resolve: () => void;
      reject: (error: Error) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  private unsubscribe: (() => void) | undefined;
  private work: Promise<void> = Promise.resolve();

  constructor(options: BackgroundTaskManagerCoordinationOptions<TTaskEvent>) {
    this.options = options;
    this.coordinator = options.coordinator;
  }

  get isLeader(): boolean {
    return this.coordinator.isLeader;
  }

  async start(): Promise<boolean> {
    await this.coordinator.start();
    this.unsubscribe = this.coordinator.subscribe((event) =>
      this.queueEvent(event),
    );
    return this.coordinator.isLeader;
  }

  async stop(): Promise<void> {
    this.unsubscribe?.();
    this.unsubscribe = undefined;
    await this.work;
    this.rejectPendingCommands(new Error('Task coordination stopped'));
    await this.coordinator.stop();
  }

  validateLeadership(): Promise<boolean> {
    return this.coordinator.validateLeadership();
  }

  publishTaskEvent(event: TTaskEvent): void {
    this.coordinator.publish({
      protocol: '@background-tasks/core/v1',
      type: 'task.event',
      event,
    } satisfies CoordinationMessage<TTaskEvent>);
  }

  publishStorageCleared(): void {
    this.coordinator.publish({
      protocol: '@background-tasks/core/v1',
      type: 'storage.cleared',
    } satisfies CoordinationMessage<TTaskEvent>);
  }

  forwardControl(
    command: TaskControlCommand,
    taskId: string,
  ): Promise<void> {
    const requestId = crypto.randomUUID();
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingCommands.delete(requestId);
        reject(
          new Error(
            `Timed out waiting for leader to ${command} task "${taskId}"`,
          ),
        );
      }, 30_000);
      this.pendingCommands.set(requestId, { resolve, reject, timer });
      this.coordinator.publish({
        protocol: '@background-tasks/core/v1',
        type: 'task.command',
        requestId,
        command,
        taskId,
      } satisfies CoordinationMessage<TTaskEvent>);
    });
  }

  private queueEvent(event: BackgroundTaskCoordinatorEvent): void {
    this.work = this.work
      .then(async () => {
        if (event.type === 'leadership.changed') {
          await this.options.onLeadershipChange?.(event.isLeader);
        } else if (event.type === 'message') {
          await this.applyMessage(event.message);
        } else if (event.type === 'pulse') {
          await this.options.onPulse?.(event.isLeader);
        }
      })
      .catch(() => undefined);
  }

  private async applyMessage(message: unknown): Promise<void> {
    const coordinationMessage = message as Partial<
      CoordinationMessage<TTaskEvent>
    > | null;
    if (coordinationMessage?.protocol !== '@background-tasks/core/v1') {
      return;
    }

    if (
      coordinationMessage.type === 'task.command.ack' &&
      'requestId' in coordinationMessage &&
      typeof coordinationMessage.requestId === 'string'
    ) {
      const pending = this.pendingCommands.get(coordinationMessage.requestId);
      if (!pending) {
        return;
      }
      clearTimeout(pending.timer);
      this.pendingCommands.delete(coordinationMessage.requestId);
      if (
        'error' in coordinationMessage &&
        typeof coordinationMessage.error === 'string'
      ) {
        pending.reject(new Error(coordinationMessage.error));
      } else {
        pending.resolve();
      }
      return;
    }

    if (
      coordinationMessage.type === 'task.command' &&
      this.coordinator.isLeader &&
      'requestId' in coordinationMessage &&
      'command' in coordinationMessage &&
      'taskId' in coordinationMessage &&
      typeof coordinationMessage.requestId === 'string' &&
      typeof coordinationMessage.taskId === 'string' &&
      (coordinationMessage.command === 'pause' ||
        coordinationMessage.command === 'resume' ||
        coordinationMessage.command === 'cancel')
    ) {
      let error: string | undefined;
      try {
        await this.options.onCommand?.(
          coordinationMessage.command,
          coordinationMessage.taskId,
        );
      } catch (cause) {
        error = cause instanceof Error ? cause.message : String(cause);
      }
      this.coordinator.publish({
        protocol: '@background-tasks/core/v1',
        type: 'task.command.ack',
        requestId: coordinationMessage.requestId,
        ...(error ? { error } : {}),
      } satisfies CoordinationMessage<TTaskEvent>);
      return;
    }

    if (coordinationMessage.type === 'storage.cleared') {
      await this.options.onStorageCleared?.();
      return;
    }

    if (
      coordinationMessage.type === 'task.event' &&
      'event' in coordinationMessage &&
      coordinationMessage.event !== undefined
    ) {
      this.options.onTaskEvent?.(coordinationMessage.event);
    }
  }

  private rejectPendingCommands(error: Error): void {
    for (const pending of this.pendingCommands.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pendingCommands.clear();
  }
}
