# Background Tasks

Durable background task runtime for browser applications.

Background Tasks is an open-source TypeScript library for creating, storing,
recovering, and executing long-running browser tasks. It is designed for
frontend applications that need to keep task state across reloads, coordinate
multiple tabs, reduce duplicate work, and expose lifecycle events so each
application can build its own UI.

## Status

This project is in early development. The first implementation target is
`@background-tasks/core`.

## Goals

- Framework-agnostic core runtime.
- Durable storage with IndexedDB.
- Memory storage for tests and demos.
- Task registry, scheduler, runner, retry policies, and cleanup.
- Multi-tab coordination with leader election.
- Operation keys for local duplicate protection.
- Event stream for application UI, logs, and observability.
- React adapter without built-in visual components.
- Testing utilities and examples.

## Non-goals

- Built-in modals, toasts, notification centers, or status pages.
- Backend queues or server-side job processing.
- Replacement for backend idempotency.
- Service Worker based permanent polling.

## Packages

| Package | Purpose |
| --- | --- |
| `@background-tasks/core` | Framework-agnostic task runtime. |
| `@background-tasks/react` | Planned React lifecycle and hooks adapter. |
| `@background-tasks/testing` | Planned testing utilities. |

## Development

This repository uses Nx with npm workspaces.

```bash
npm install
npm run build
npm run test -- --run
npm run lint
```

## License

MIT
