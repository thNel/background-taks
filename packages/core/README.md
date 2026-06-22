# @background-tasks/core

Framework-agnostic durable background task runtime for browser applications.

This package contains the core task model, registry, manager lifecycle,
scheduler, execution loop, storage contracts, events, and browser coordination
primitives. Active tasks are isolated by owner and deduplicated atomically by
task type and payload.

## Status

Early development. Public APIs may change before the first stable release.

## Building

```bash
npx nx build core
```

## Running unit tests

```bash
npx nx test core --run
```

## License

MIT
