import {
  createCompleteDirective,
  createScheduleNextDirective,
  isCompleteDirective,
  type BackgroundTaskDefinition,
} from './task-definition';
import type {
  BackgroundTask,
  TaskResult,
  TaskState,
  TaskType,
} from './types';

export type BackgroundTaskRunnerOutcome<TResult, TState> =
  | { type: 'completed'; result: TResult; state?: TState }
  | { type: 'scheduled'; state?: TState }
  | { type: 'failed'; error: unknown };

export class ExecutorTimeoutError extends Error {
  readonly taskId: string;
  readonly timeout: number;

  constructor(taskId: string, timeout: number) {
    super(`Background task executor "${taskId}" timed out after ${timeout}ms`);
    this.name = 'ExecutorTimeoutError';
    this.taskId = taskId;
    this.timeout = timeout;
  }
}

export class BackgroundTaskRunner<TTasks> {
  private readonly controllers = new Map<string, AbortController>();

  async run<TType extends TaskType<TTasks>>(
    task: BackgroundTask<TTasks, TType>,
    definition: BackgroundTaskDefinition<TTasks, TType>,
  ): Promise<
    BackgroundTaskRunnerOutcome<
      TaskResult<TTasks, TType>,
      TaskState<TTasks, TType>
    >
  > {
    const controller = new AbortController();
    this.controllers.set(task.id, controller);
    let timeoutError: ExecutorTimeoutError | undefined;
    const timeout = setTimeout(() => {
      timeoutError = new ExecutorTimeoutError(
        task.id,
        definition.executionTimeout,
      );
      controller.abort(timeoutError);
    }, definition.executionTimeout);

    try {
      const directive = await definition.execute({
        task,
        signal: controller.signal,
        complete: (input) => createCompleteDirective(input),
        scheduleNext: ((input?: { state?: TaskState<TTasks, TType> }) =>
          createScheduleNextDirective(input)) as never,
      });

      const state =
        'state' in directive && directive.state !== undefined
          ? { state: directive.state }
          : {};

      if (timeoutError) {
        return { type: 'failed', error: timeoutError };
      }

      if (isCompleteDirective(directive)) {
        return {
          type: 'completed',
          result: directive.result,
          ...state,
        };
      }

      return { type: 'scheduled', ...state };
    } catch (error) {
      return { type: 'failed', error: timeoutError ?? error };
    } finally {
      clearTimeout(timeout);
      if (this.controllers.get(task.id) === controller) {
        this.controllers.delete(task.id);
      }
    }
  }

  abort(id: string): boolean {
    const controller = this.controllers.get(id);
    if (!controller) {
      return false;
    }

    controller.abort();
    return true;
  }
}
