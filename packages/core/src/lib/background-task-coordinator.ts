export interface BroadcastChannelMessageEvent {
  readonly data: unknown;
}

export interface BroadcastChannelLike {
  postMessage(message: unknown): void;
  close(): void;
  addEventListener(
    type: 'message',
    listener: (event: BroadcastChannelMessageEvent) => void,
  ): void;
  removeEventListener(
    type: 'message',
    listener: (event: BroadcastChannelMessageEvent) => void,
  ): void;
}

export type BackgroundTaskCoordinatorEvent =
  | {
      type: 'leadership.changed';
      isLeader: boolean;
      fencingToken: number;
    }
  | { type: 'message'; message: unknown }
  | { type: 'pulse'; isLeader: boolean };

export interface BackgroundTaskCoordinator {
  readonly isLeader: boolean;
  readonly fencingToken: number;
  start(): Promise<void>;
  stop(): Promise<void>;
  validateLeadership(): Promise<boolean>;
  publish(message: unknown): void;
  subscribe(
    listener: (event: BackgroundTaskCoordinatorEvent) => void,
  ): () => void;
}

