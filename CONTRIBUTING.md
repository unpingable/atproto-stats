# Contributing

This project is intentionally small and local-first. Keep changes focused.

## Standards

- Python: 3.12+
- Formatting/linting: black + ruff conventions
- Tests: pytest MUST pass before merging

## Pull Request Guidelines

1. Keep PRs scoped to one concern.
2. Include test updates for behavior changes.
3. Document any schema or CLI flag changes in `README.md`.
4. Avoid adding heavyweight dependencies unless there is a clear runtime need.

## Data and Privacy

- Do not commit `.env`, session tokens, or local DB files.
- Keep the tool local-only by default. No external telemetry.

## Stability Rules

- Preserve idempotent sync behavior.
- Preserve auditable metrics in `summary.json`.
- If classification semantics change, add/adjust fixture-driven tests.
