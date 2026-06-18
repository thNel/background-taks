# Contributing

This project is in early development, so the public API is not stable yet.

## Local Setup

```bash
npm install
npm run build
npm run test -- --run
npm run lint
```

## Development Principles

- Keep `core` framework-agnostic.
- Do not add built-in visual UI components.
- Prefer explicit interfaces over hidden global state.
- Keep browser limitations documented.
- Add tests with behavior changes.

## Pull Requests

Before opening a pull request, run:

```bash
npm run build
npm run test -- --run
npm run lint
```

Include a short explanation of the problem, the change, and any limitations.
