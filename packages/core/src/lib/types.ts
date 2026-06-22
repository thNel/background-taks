export interface TaskContract {
  payload: unknown;
  result: unknown;
  state?: unknown;
}

export type TaskMap = Record<string, TaskContract>;

export type TaskType<TTasks> = Extract<keyof TTasks, string>;

export type TaskPayload<
  TTasks,
  TType extends TaskType<TTasks>,
> = TTasks[TType] extends { payload: infer TPayload } ? TPayload : never;

export type TaskResult<
  TTasks,
  TType extends TaskType<TTasks>,
> = TTasks[TType] extends { result: infer TResult } ? TResult : never;

export type TaskState<
  TTasks,
  TType extends TaskType<TTasks>,
> = 'state' extends keyof TTasks[TType]
  ? TTasks[TType] extends { state?: infer TState }
    ? TState
    : never
  : never;

export type HasTaskState<
  TTasks,
  TType extends TaskType<TTasks>,
> = 'state' extends keyof TTasks[TType] ? true : false;

export type BackgroundTaskStatus =
  | 'queued'
  | 'running'
  | 'paused'
  | 'completed'
  | 'failed'
  | 'canceled';

type TaskStateField<TTasks, TType extends TaskType<TTasks>> =
  HasTaskState<TTasks, TType> extends true
    ? { state: TaskState<TTasks, TType> }
    : { state?: never };

export type BackgroundTask<
  TTasks,
  TType extends TaskType<TTasks>,
> = {
  readonly id: string;
  readonly ownerKey: string;
  readonly type: TType;
  readonly payload: Readonly<TaskPayload<TTasks, TType>>;
  readonly status: BackgroundTaskStatus;
  readonly result?: TaskResult<TTasks, TType>;
  readonly error?: unknown;
  readonly createdAt: number;
  readonly updatedAt: number;
  readonly nextRunAt: number | null;
  readonly consecutiveFailures: number;
} & TaskStateField<TTasks, TType>;

export type AnyBackgroundTask<TTasks> = {
  [TType in TaskType<TTasks>]: BackgroundTask<TTasks, TType>;
}[TaskType<TTasks>];
