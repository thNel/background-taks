import type {
  BackgroundTask,
  HasTaskState,
  TaskPayload,
  TaskResult,
  TaskState,
  TaskType,
} from './types';

const executionDirective = Symbol('BackgroundTaskExecutionDirective');

export interface CompleteDirective<TResult, TState = never> {
  readonly [executionDirective]: 'complete';
  readonly result: TResult;
  readonly state?: TState;
}

export interface ScheduleNextDirective<TState = never> {
  readonly [executionDirective]: 'schedule-next';
  readonly state?: TState;
}

export type ExecutionDirective<TResult, TState = never> =
  | CompleteDirective<TResult, TState>
  | ScheduleNextDirective<TState>;

type CompleteInput<TTasks, TType extends TaskType<TTasks>> = {
  result: TaskResult<TTasks, TType>;
} & (HasTaskState<TTasks, TType> extends true
  ? { state?: TaskState<TTasks, TType> }
  : { state?: never });

type ScheduleNext<TTasks, TType extends TaskType<TTasks>> =
  HasTaskState<TTasks, TType> extends true
    ? (input?: {
        state?: TaskState<TTasks, TType>;
      }) => ScheduleNextDirective<TaskState<TTasks, TType>>
    : () => ScheduleNextDirective;

export interface BackgroundTaskExecutionContext<
  TTasks,
  TType extends TaskType<TTasks>,
> {
  readonly task: BackgroundTask<TTasks, TType>;
  readonly signal: AbortSignal;
  readonly complete: (
    input: CompleteInput<TTasks, TType>,
  ) => CompleteDirective<
    TaskResult<TTasks, TType>,
    TaskState<TTasks, TType>
  >;
  readonly scheduleNext: ScheduleNext<TTasks, TType>;
}

export interface BackgroundTaskRetryContext<
  TTasks,
  TType extends TaskType<TTasks>,
> {
  readonly task: BackgroundTask<TTasks, TType>;
  readonly retryNumber: number;
  readonly maxRetries: number;
}

export interface BackgroundTaskRetryDelayContext<
  TTasks,
  TType extends TaskType<TTasks>,
> extends BackgroundTaskRetryContext<TTasks, TType> {
  readonly error: unknown;
}

export interface BackgroundTaskRetryPolicy<
  TTasks,
  TType extends TaskType<TTasks>,
> {
  shouldRetry?: (
    error: unknown,
    context: BackgroundTaskRetryContext<TTasks, TType>,
  ) => boolean;
  getDelay?: (
    context: BackgroundTaskRetryDelayContext<TTasks, TType>,
  ) => number;
}

type InitialStateDefinition<TTasks, TType extends TaskType<TTasks>> =
  HasTaskState<TTasks, TType> extends true
    ? {
        initialState: (
          payload: Readonly<TaskPayload<TTasks, TType>>,
        ) => TaskState<TTasks, TType>;
      }
    : { initialState?: never };

export type BackgroundTaskDefinition<
  TTasks,
  TType extends TaskType<TTasks>,
> = {
  priority?: number;
  executionTimeout: number;
  pollingInterval?: number;
  maxRetries?: number;
  retryPolicy?: BackgroundTaskRetryPolicy<TTasks, TType>;
  execute(
    context: BackgroundTaskExecutionContext<TTasks, TType>,
  ):
    | ExecutionDirective<
        TaskResult<TTasks, TType>,
        TaskState<TTasks, TType>
      >
    | Promise<
        ExecutionDirective<
          TaskResult<TTasks, TType>,
          TaskState<TTasks, TType>
        >
      >;
} & InitialStateDefinition<TTasks, TType>;

export type BackgroundTaskDefinitions<TTasks> = {
  [TType in TaskType<TTasks>]: BackgroundTaskDefinition<TTasks, TType>;
};

export function createCompleteDirective<TResult, TState>(input: {
  result: TResult;
  state?: TState;
}): CompleteDirective<TResult, TState> {
  return {
    [executionDirective]: 'complete',
    ...input,
  };
}

export function createScheduleNextDirective<TState>(input?: {
  state?: TState;
}): ScheduleNextDirective<TState> {
  return {
    [executionDirective]: 'schedule-next',
    ...input,
  };
}

export function isCompleteDirective<TResult, TState>(
  directive: ExecutionDirective<TResult, TState>,
): directive is CompleteDirective<TResult, TState> {
  return directive[executionDirective] === 'complete';
}
