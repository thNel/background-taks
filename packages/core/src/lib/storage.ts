import type {
  HasTaskState,
  TaskPayload,
  TaskState,
  TaskType,
} from './types';

type PersistedTaskStateField<TTasks, TType extends TaskType<TTasks>> =
  HasTaskState<TTasks, TType> extends true
    ? { state: TaskState<TTasks, TType> }
    : { state?: never };

export type PersistedTaskFor<
  TTasks,
  TType extends TaskType<TTasks>,
> = {
  id: string;
  ownerKey: string;
  fingerprint: string;
  type: TType;
  payload: TaskPayload<TTasks, TType>;
  createdAt: number;
  updatedAt: number;
  nextRunAt: number | null;
  consecutiveFailures: number;
} & PersistedTaskStateField<TTasks, TType>;

export type PersistedTask<TTasks> = {
  [TType in TaskType<TTasks>]: PersistedTaskFor<TTasks, TType>;
}[TaskType<TTasks>];

export interface PutTaskIfAbsentResult<TTasks> {
  created: boolean;
  task: PersistedTask<TTasks>;
}

export interface BackgroundTaskStorage<TTasks> {
  getTasks(ownerKey: string): Promise<PersistedTask<TTasks>[]>;
  putTaskIfAbsent(
    task: PersistedTask<TTasks>,
  ): Promise<PutTaskIfAbsentResult<TTasks>>;
  putTask(task: PersistedTask<TTasks>): Promise<void>;
  deleteTask(id: string): Promise<void>;
  clear(): Promise<void>;
}
