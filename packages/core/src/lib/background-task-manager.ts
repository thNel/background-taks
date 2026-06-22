import { BackgroundTaskRegistry } from './background-task-registry';
import type { BackgroundTaskCoordinator } from './background-task-coordinator';
import {
  BackgroundTaskManagerCoordination,
  type TaskControlCommand,
} from './background-task-manager-coordination';
import {
  BackgroundTaskRunner,
  type BackgroundTaskRunnerOutcome,
} from './background-task-runner';
import { BackgroundTaskScheduler } from './background-task-scheduler';
import { cloneValue, deepFreeze, type InternalTask } from './internal-task';
import type { BackgroundTaskStorage } from './storage';
import { createTaskFingerprint } from './task-fingerprint';
import {
  type BackgroundTaskDefinition,
  type BackgroundTaskDefinitions,
} from './task-definition';
import type {
  AnyBackgroundTask,
  BackgroundTask,
  BackgroundTaskStatus,
  TaskPayload,
  TaskType,
} from './types';

export type BackgroundTaskManagerStatus = 'stopped' | 'running' | 'stopping';

type TaskEventType =
  | 'task.enqueued'
  | 'task.started'
  | 'task.updated'
  | 'task.completed'
  | 'task.failed'
  | 'task.paused'
  | 'task.resumed'
  | 'task.canceled';

type CoordinatedTaskEvent<TTasks> = {
  type: TaskEventType;
  task: AnyBackgroundTask<TTasks>;
};

export type BackgroundTaskManagerEvent<TTasks> =
  | {
      type: 'manager.started' | 'manager.stopped' | 'manager.storageCleared';
    }
  | { type: TaskEventType; task: AnyBackgroundTask<TTasks> }
  | {
      type: 'manager.recoveryError';
      taskId: string;
      taskType: string;
      error: Error;
    };

export interface BackgroundTaskManagerSnapshot<TTasks> {
  readonly status: BackgroundTaskManagerStatus;
  readonly tasks: readonly AnyBackgroundTask<TTasks>[];
  readonly counts: Readonly<Record<BackgroundTaskStatus, number>>;
}

export interface BackgroundTaskManagerOptions<TTasks> {
  ownerKey: string;
  definitions: BackgroundTaskDefinitions<TTasks>;
  storage: BackgroundTaskStorage<TTasks>;
  maxConcurrentExecutions?: number;
  coordinator?: BackgroundTaskCoordinator;
}

export type EnqueueBackgroundTaskInput<
  TTasks,
  TType extends TaskType<TTasks>,
> = {
  type: TType;
  payload: TaskPayload<TTasks, TType>;
};

export class BackgroundTaskManager<TTasks> {
  private readonly ownerKey: string;
  private readonly definitions: BackgroundTaskDefinitions<TTasks>;
  private readonly storage: BackgroundTaskStorage<TTasks>;
  private readonly coordination:
    | BackgroundTaskManagerCoordination<CoordinatedTaskEvent<TTasks>>
    | undefined;
  private readonly registry = new BackgroundTaskRegistry<TTasks>();
  private readonly runner = new BackgroundTaskRunner<TTasks>();
  private readonly scheduler: BackgroundTaskScheduler<InternalTask<TTasks>>;
  private readonly listeners = new Set<
    (event: BackgroundTaskManagerEvent<TTasks>) => void
  >();
  private managerStatus: BackgroundTaskManagerStatus = 'stopped';
  private executionEnabled = false;

  constructor(options: BackgroundTaskManagerOptions<TTasks>) {
    if (typeof options.ownerKey !== 'string' || !options.ownerKey.trim()) {
      throw new TypeError('ownerKey must not be empty');
    }

    this.ownerKey = options.ownerKey;
    this.definitions = options.definitions;
    this.storage = options.storage;
    this.coordination = options.coordinator
      ? new BackgroundTaskManagerCoordination({
          coordinator: options.coordinator,
          onLeadershipChange: (isLeader) =>
            this.handleLeadershipChange(isLeader),
          onTaskEvent: (event) => this.applyCoordinatedTaskEvent(event),
          onStorageCleared: () => this.applyCoordinatedStorageClear(),
          onPulse: async (isLeader) => {
            if (isLeader) {
              await this.reconcilePersistedTasks();
              this.scheduler.wake();
            }
          },
          onCommand: (command, taskId) =>
            this.applyCoordinatedCommand(command, taskId),
        })
      : undefined;
    this.scheduler = new BackgroundTaskScheduler({
      maxConcurrentExecutions: options.maxConcurrentExecutions ?? 3,
      getTasks: () => this.registry.values(),
      getPriority: (task) => this.definitions[task.type].priority ?? 0,
      execute: (task) => this.execute(task),
    });

    for (const definition of Object.values(this.definitions)) {
      const typedDefinition = definition as BackgroundTaskDefinition<
        TTasks,
        TaskType<TTasks>
      >;
      if (
        !Number.isFinite(typedDefinition.executionTimeout) ||
        typedDefinition.executionTimeout <= 0
      ) {
        throw new RangeError('executionTimeout must be a positive finite number');
      }
      if (
        typedDefinition.priority !== undefined &&
        !Number.isFinite(typedDefinition.priority)
      ) {
        throw new RangeError('priority must be a finite number');
      }
      if (
        typedDefinition.maxRetries !== undefined &&
        (!Number.isInteger(typedDefinition.maxRetries) ||
          typedDefinition.maxRetries < 0)
      ) {
        throw new RangeError('maxRetries must be a non-negative integer');
      }
      if (
        typedDefinition.pollingInterval !== undefined &&
        (!Number.isFinite(typedDefinition.pollingInterval) ||
          typedDefinition.pollingInterval < 0)
      ) {
        throw new RangeError('pollingInterval must be a non-negative number');
      }
    }
  }

  async start(): Promise<void> {
    if (this.managerStatus === 'running') {
      return;
    }

    await this.reconcilePersistedTasks();

    this.managerStatus = 'running';
    if (this.coordination) {
      this.executionEnabled = await this.coordination.start();
    } else {
      this.executionEnabled = true;
    }
    this.emit({ type: 'manager.started' });
    if (this.executionEnabled) {
      this.scheduler.start();
    }
  }

  async stop(): Promise<void> {
    if (this.managerStatus === 'stopped') {
      return;
    }

    if (this.managerStatus === 'stopping') {
      await this.scheduler.stop();
      return;
    }

    this.managerStatus = 'stopping';
    this.executionEnabled = false;
    await this.quiesceExecutions();
    await this.coordination?.stop();
    this.managerStatus = 'stopped';
    this.emit({ type: 'manager.stopped' });
  }

  async pause(id: string): Promise<void> {
    if (this.shouldForwardControl()) {
      return this.forwardControl('pause', id);
    }
    return this.pauseLocal(id);
  }

  private async pauseLocal(id: string): Promise<void> {
    const task = this.registry.getInternal(id);
    if (!task || this.isTerminal(task.status) || task.status === 'paused') {
      return;
    }

    task.pauseRequested = true;
    const execution = this.scheduler.getExecution(id);
    if (execution) {
      this.runner.abort(id);
      await execution;
      return;
    }

    task.pauseRequested = false;
    task.status = 'paused';
    task.updatedAt = Date.now();
    await this.storage.putTask(this.registry.toPersistedTask(task));
    this.emit({ type: 'task.paused', task: this.registry.toAnyTaskSnapshot(task) });
    this.scheduler.wake();
  }

  async resume(id: string): Promise<void> {
    if (this.shouldForwardControl()) {
      return this.forwardControl('resume', id);
    }
    return this.resumeLocal(id);
  }

  private async resumeLocal(id: string): Promise<void> {
    const task = this.registry.getInternal(id);
    if (!task || task.status !== 'paused') {
      return;
    }

    task.pauseRequested = false;
    task.status =
      this.executionEnabled && task.nextRunAt !== null
        ? 'running'
        : 'queued';
    task.updatedAt = Date.now();
    await this.storage.putTask(this.registry.toPersistedTask(task));
    this.emit({ type: 'task.resumed', task: this.registry.toAnyTaskSnapshot(task) });
    this.scheduler.wake();
  }

  async cancel(id: string): Promise<void> {
    if (this.shouldForwardControl()) {
      return this.forwardControl('cancel', id);
    }
    return this.cancelLocal(id);
  }

  private async cancelLocal(id: string): Promise<void> {
    const task = this.registry.getInternal(id);
    if (!task || this.isTerminal(task.status)) {
      return;
    }

    task.cancelRequested = true;
    const execution = this.scheduler.getExecution(id);
    if (execution) {
      this.runner.abort(id);
      await execution;
      return;
    }

    await this.finalizeCanceled(task);
  }

  async clearStorage(): Promise<void> {
    const hasUnfinishedTasks = [...this.registry.values()].some(
      (task) => !this.isTerminal(task.status),
    );
    const shouldStop =
      hasUnfinishedTasks && this.managerStatus !== 'stopped';
    if (shouldStop) {
      this.managerStatus = 'stopping';
      this.executionEnabled = false;
      await this.quiesceExecutions();
    }

    await this.storage.clear();
    this.registry.clear();
    this.emit({ type: 'manager.storageCleared' });

    if (shouldStop) {
      await this.coordination?.stop();
      this.managerStatus = 'stopped';
      this.emit({ type: 'manager.stopped' });
    }
  }

  async enqueue<TType extends TaskType<TTasks>>(
    input: EnqueueBackgroundTaskInput<TTasks, TType>,
  ): Promise<BackgroundTask<TTasks, TType>> {
    const definition = this.definitions[input.type];
    const now = Date.now();
    const payload = cloneValue(input.payload);
    const task: InternalTask<TTasks> = {
      id: crypto.randomUUID(),
      ownerKey: this.ownerKey,
      fingerprint: createTaskFingerprint(input.type, payload),
      type: input.type,
      payload,
      status: 'queued',
      createdAt: now,
      updatedAt: now,
      nextRunAt: null,
      consecutiveFailures: 0,
    };

    if ('initialState' in definition && definition.initialState) {
      task.state = cloneValue(definition.initialState(payload as never));
    }

    const stored = await this.storage.putTaskIfAbsent(
      this.registry.toPersistedTask(task),
    );
    const existing = this.registry.getInternal(stored.task.id);
    const activeTask = existing ??
      (stored.created
        ? task
        : ({ ...cloneValue(stored.task), status: 'queued' } as InternalTask<TTasks>));

    if (!existing) {
      this.registry.add(activeTask);
    }
    const snapshot = this.registry.toTaskSnapshot<TType>(activeTask);
    if (stored.created) {
      this.emit({
        type: 'task.enqueued',
        task: snapshot as unknown as AnyBackgroundTask<TTasks>,
      });
    }

    if (this.managerStatus === 'running') {
      this.scheduler.wake();
    }

    return snapshot;
  }

  getTasks(): readonly AnyBackgroundTask<TTasks>[] {
    return this.registry.getTasks();
  }

  getTask(id: string): AnyBackgroundTask<TTasks> | undefined {
    return this.registry.getTask(id);
  }

  getTaskAs<TType extends TaskType<TTasks>>(
    id: string,
    type: TType,
  ): BackgroundTask<TTasks, TType> | undefined {
    return this.registry.getTaskAs(id, type);
  }

  getSnapshot(): BackgroundTaskManagerSnapshot<TTasks> {
    const tasks = this.getTasks();

    return deepFreeze({
      status: this.managerStatus,
      tasks,
      counts: this.registry.getCounts(),
    });
  }

  subscribe(
    listener: (event: BackgroundTaskManagerEvent<TTasks>) => void,
  ): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private async execute(task: InternalTask<TTasks>): Promise<void> {
    if (
      !this.executionEnabled ||
      this.managerStatus !== 'running' ||
      (task.status !== 'queued' && task.status !== 'running')
    ) {
      return;
    }

    if (
      this.coordination &&
      !(await this.coordination.validateLeadership())
    ) {
      this.executionEnabled = false;
      void this.handleLeadershipChange(false);
      return;
    }

    const isFirstExecution = task.status === 'queued';
    task.status = 'running';
    task.nextRunAt = null;
    task.updatedAt = Date.now();
    if (isFirstExecution) {
      this.emit({
        type: 'task.started',
        task: this.registry.toAnyTaskSnapshot(task),
      });
    }

    const definition = this.definitions[
      task.type
    ] as BackgroundTaskDefinition<TTasks, TaskType<TTasks>>;
    const outcome = await this.runner.run(
      this.registry.toAnyTaskSnapshot(task),
      definition,
    );

    if (outcome.type === 'failed') {
      await this.applyFailedOutcome(task, definition, outcome.error);
      return;
    }

    try {
      if (task.cancelRequested) {
        await this.finalizeCanceled(task);
      } else if (outcome.type === 'completed') {
        await this.applyCompletedOutcome(task, outcome);
      } else if (task.pauseRequested) {
        await this.applyPausedOutcome(task, definition, outcome);
      } else if (!this.executionEnabled || this.managerStatus !== 'running') {
        await this.applyStoppedOutcome(task, definition, outcome);
      } else {
        await this.applyScheduledOutcome(task, definition, outcome);
      }
    } catch (error) {
      await this.applyFailedOutcome(task, definition, error);
    }
  }

  private async applyCompletedOutcome(
    task: InternalTask<TTasks>,
    outcome: Extract<
      BackgroundTaskRunnerOutcome<unknown, unknown>,
      { type: 'completed' }
    >,
  ): Promise<void> {
    this.applyOutcomeState(task, outcome);
    task.updatedAt = Date.now();
    task.status = 'completed';
    task.result = cloneValue(outcome.result);
    task.nextRunAt = null;
    task.consecutiveFailures = 0;
    await this.storage.deleteTask(task.id);
    this.emit({
      type: 'task.completed',
      task: this.registry.toAnyTaskSnapshot(task),
    });
  }

  private async applyScheduledOutcome(
    task: InternalTask<TTasks>,
    definition: BackgroundTaskDefinition<TTasks, TaskType<TTasks>>,
    outcome: Extract<
      BackgroundTaskRunnerOutcome<unknown, unknown>,
      { type: 'scheduled' }
    >,
  ): Promise<void> {
    this.applyOutcomeState(task, outcome);
    this.prepareNextRun(task, definition);
    task.status = 'running';
    task.updatedAt = Date.now();
    await this.storage.putTask(this.registry.toPersistedTask(task));
    this.emit({
      type: 'task.updated',
      task: this.registry.toAnyTaskSnapshot(task),
    });
  }

  private getRetryDelay(consecutiveFailures: number): number {
    return Math.min(1_000 * 2 ** (consecutiveFailures - 1), 30_000);
  }

  private async applyPausedOutcome(
    task: InternalTask<TTasks>,
    definition: BackgroundTaskDefinition<TTasks, TaskType<TTasks>>,
    outcome: Extract<
      BackgroundTaskRunnerOutcome<unknown, unknown>,
      { type: 'scheduled' }
    >,
  ): Promise<void> {
    this.applyOutcomeState(task, outcome);
    this.prepareNextRun(task, definition);
    task.pauseRequested = false;
    task.status = 'paused';
    task.updatedAt = Date.now();
    await this.storage.putTask(this.registry.toPersistedTask(task));
    this.emit({
      type: 'task.paused',
      task: this.registry.toAnyTaskSnapshot(task),
    });
  }

  private async applyStoppedOutcome(
    task: InternalTask<TTasks>,
    definition: BackgroundTaskDefinition<TTasks, TaskType<TTasks>>,
    outcome: Extract<
      BackgroundTaskRunnerOutcome<unknown, unknown>,
      { type: 'scheduled' }
    >,
  ): Promise<void> {
    this.applyOutcomeState(task, outcome);
    this.prepareNextRun(task, definition);
    task.status = 'queued';
    task.updatedAt = Date.now();
    await this.storage.putTask(this.registry.toPersistedTask(task));
  }

  private applyOutcomeState(
    task: InternalTask<TTasks>,
    outcome: { state?: unknown },
  ): void {
    if (outcome.state !== undefined) {
      task.state = cloneValue(outcome.state);
    }
  }

  private async applyFailedOutcome(
    task: InternalTask<TTasks>,
    definition: BackgroundTaskDefinition<TTasks, TaskType<TTasks>>,
    error: unknown,
  ): Promise<void> {
    if (task.cancelRequested) {
      await this.finalizeCanceled(task);
    } else if (task.pauseRequested) {
      task.pauseRequested = false;
      task.status = 'paused';
      task.updatedAt = Date.now();
      await this.storage.putTask(this.registry.toPersistedTask(task));
      this.emit({
        type: 'task.paused',
        task: this.registry.toAnyTaskSnapshot(task),
      });
    } else if (!this.executionEnabled || this.managerStatus !== 'running') {
      task.status = 'queued';
      task.updatedAt = Date.now();
      await this.storage.putTask(this.registry.toPersistedTask(task));
    } else {
      const maxRetries = definition.maxRetries ?? 0;
      if (task.consecutiveFailures < maxRetries) {
        const retryNumber = task.consecutiveFailures + 1;
        const retryContext = {
          task: this.registry.toAnyTaskSnapshot(task),
          retryNumber,
          maxRetries,
        };
        let shouldRetry: boolean;
        try {
          shouldRetry =
            definition.retryPolicy?.shouldRetry?.(error, retryContext) ?? true;
        } catch (policyError) {
          await this.finalizeFailed(task, policyError);
          return;
        }

        if (!shouldRetry) {
          await this.finalizeFailed(task, error);
          return;
        }

        let retryDelay: number;
        try {
          retryDelay =
            definition.retryPolicy?.getDelay?.({
              ...retryContext,
              error,
            }) ?? this.getRetryDelay(retryNumber);
          if (!Number.isFinite(retryDelay) || retryDelay < 0) {
            throw new RangeError(
              'retryPolicy.getDelay must return a non-negative finite number',
            );
          }
        } catch (policyError) {
          await this.finalizeFailed(task, policyError);
          return;
        }

        task.consecutiveFailures = retryNumber;
        task.status = 'running';
        task.nextRunAt = Date.now() + retryDelay;
        task.updatedAt = Date.now();
        await this.storage.putTask(this.registry.toPersistedTask(task));
        this.emit({
          type: 'task.updated',
          task: this.registry.toAnyTaskSnapshot(task),
        });
      } else {
        await this.finalizeFailed(task, error);
      }
    }
  }

  private async finalizeFailed(
    task: InternalTask<TTasks>,
    error: unknown,
  ): Promise<void> {
    task.status = 'failed';
    task.error = error;
    task.nextRunAt = null;
    task.updatedAt = Date.now();
    await this.storage.deleteTask(task.id);
    this.emit({
      type: 'task.failed',
      task: this.registry.toAnyTaskSnapshot(task),
    });
  }

  private prepareNextRun(
    task: InternalTask<TTasks>,
    definition: BackgroundTaskDefinition<TTasks, TaskType<TTasks>>,
  ): void {
    if (definition.pollingInterval === undefined) {
      throw new Error(
        `Task definition "${task.type}" must set pollingInterval before returning scheduleNext()`,
      );
    }

    task.consecutiveFailures = 0;
    task.nextRunAt = Date.now() + definition.pollingInterval;
  }

  private async finalizeCanceled(task: InternalTask<TTasks>): Promise<void> {
    task.cancelRequested = false;
    task.pauseRequested = false;
    task.status = 'canceled';
    task.result = undefined;
    task.error = undefined;
    task.nextRunAt = null;
    task.updatedAt = Date.now();
    await this.storage.deleteTask(task.id);
    this.emit({
      type: 'task.canceled',
      task: this.registry.toAnyTaskSnapshot(task),
    });
  }

  private shouldForwardControl(): boolean {
    return Boolean(
      this.coordination &&
        this.managerStatus === 'running' &&
        !this.executionEnabled,
    );
  }

  private forwardControl(
    command: TaskControlCommand,
    taskId: string,
  ): Promise<void> {
    return this.coordination?.forwardControl(command, taskId) ?? Promise.resolve();
  }

  private async handleLeadershipChange(isLeader: boolean): Promise<void> {
    if (this.managerStatus !== 'running') {
      return;
    }

    this.executionEnabled = isLeader;
    if (isLeader) {
      await this.reconcilePersistedTasks();
      this.scheduler.start();
      this.scheduler.wake();
      return;
    }

    await this.quiesceExecutions();
  }

  private async quiesceExecutions(): Promise<void> {
    await this.scheduler.stop((id) => this.runner.abort(id));
    for (const task of this.registry.values()) {
      if (task.status === 'running') {
        task.status = 'queued';
        task.updatedAt = Date.now();
        await this.storage.putTask(this.registry.toPersistedTask(task));
      }
    }
  }

  private applyCoordinatedTaskEvent(event: CoordinatedTaskEvent<TTasks>): void {
    const task = event.task;
    if (
      !task ||
      task.ownerKey !== this.ownerKey ||
      !Object.prototype.hasOwnProperty.call(this.definitions, task.type)
    ) {
      return;
    }

    this.registry.add({
      ...cloneValue(task),
      fingerprint: createTaskFingerprint(task.type, task.payload),
    } as InternalTask<TTasks>);
    this.emit(event, false);
    if (this.executionEnabled) {
      this.scheduler.wake();
    }
  }

  private async applyCoordinatedStorageClear(): Promise<void> {
    const shouldResume = this.managerStatus === 'running';
    this.executionEnabled = false;
    await this.scheduler.stop((id) => this.runner.abort(id));
    await this.storage.clear();
    this.registry.clear();
    this.emit({ type: 'manager.storageCleared' }, false);

    this.executionEnabled = shouldResume && (this.coordination?.isLeader ?? true);
    if (this.executionEnabled) {
      this.scheduler.start();
    }
  }

  private async applyCoordinatedCommand(
    command: TaskControlCommand,
    taskId: string,
  ): Promise<void> {
    if (command === 'pause') {
      await this.pauseLocal(taskId);
    } else if (command === 'resume') {
      await this.resumeLocal(taskId);
    } else {
      await this.cancelLocal(taskId);
    }
  }

  private async reconcilePersistedTasks(): Promise<void> {
    const persistedTasks = await this.storage.getTasks(this.ownerKey);
    for (const persisted of persistedTasks) {
      if (
        !Object.prototype.hasOwnProperty.call(this.definitions, persisted.type)
      ) {
        this.emit({
          type: 'manager.recoveryError',
          taskId: persisted.id,
          taskType: persisted.type,
          error: new Error(
            `No background task definition is registered for "${persisted.type}"`,
          ),
        });
        continue;
      }

      if (!this.registry.has(persisted.id)) {
        this.registry.add({
          ...cloneValue(persisted),
          status: 'queued',
        } as InternalTask<TTasks>);
      }
    }
  }

  private isTerminal(status: BackgroundTaskStatus): boolean {
    return (
      status === 'completed' || status === 'failed' || status === 'canceled'
    );
  }

  private emit(
    event: BackgroundTaskManagerEvent<TTasks>,
    broadcast = true,
  ): void {
    for (const listener of this.listeners) {
      listener(event);
    }

    if (!broadcast || !this.coordination) {
      return;
    }

    if ('task' in event) {
      this.coordination.publishTaskEvent(event);
    } else if (event.type === 'manager.storageCleared') {
      this.coordination.publishStorageCleared();
    }
  }
}
