# Contributing to dataiku-sdk

Thanks for your interest in improving the Dataiku DSS SDK & CLI. This guide covers the local workflow and the checks every change must pass.

## Prerequisites

- [Bun](https://bun.sh) >= 1.4.0 — primary package manager, source runtime, and test runner
- [Node.js](https://nodejs.org) >= 22.15.0 with npm — published CLI runtime and npm release tooling
- Git

## Setup

```text
git clone https://github.com/clssck/dataiku-sdk
cd dataiku-sdk
bun install --frozen-lockfile
```

## Project layout

- `src/` — the SDK client and resources (`src/resources/*`) plus the CLI (`src/cli/*`, entry `bin/dss.js`)
- `packages/types/` — TypeBox schemas and their derived TypeScript types (`@dataiku/types`), re-exported through `src/schemas.ts`
- `tests/` — unit and (gated) integration tests, run with `bun test`

## Everyday commands

| Task                    | Command                                                    |
| ----------------------- | ---------------------------------------------------------- |
| Type-check              | `bun run check` (`tsc --noEmit`)                            |
| Build                   | `bun run build`                                            |
| Lint                    | `bun run lint` — autofix with `bun run lint:fix`            |
| Format                  | `bun run format` — verify with `bun run format:check`       |
| Test                    | `bun test`                                                 |
| Packaged platform smoke | `bun run test:platform`                                    |

## Checks your change must pass

Before opening a pull request, all of these must be green:

1. `bun run check` — TypeScript is clean (no errors)
2. `bun run format:check` — dprint formatting is applied
3. `bun run lint` — oxlint reports no errors
4. `bun test` — the unit suite passes
5. `bun run test:platform` — the packed artifact runs under Bun and Node, preserves JSON errors, and installs the skill

## Tests

- Unit tests run by default with `bun test`.
- **Integration tests** talk to a live DSS instance and are skipped unless explicitly enabled:
  - `RUN_DATAIKU_INTEGRATION=1` — read-only integration tests (needs `DATAIKU_URL` and `DATAIKU_API_KEY`)
  - `RUN_DATAIKU_INTEGRATION_MUTATING=1` — additionally run mutating tests, which create and delete **scratch** resources
  - Convenience scripts: `bun run test:integration`, `test:integration:mutating`, `test:integration:rigorous`
- Never point mutating tests at a project you care about — use a throwaway instance/project.
- New behavior should ship with a test. Prefer tests that defend real contracts (exit codes, error taxonomy, output shape) over implementation details.

## Commit messages

This repository uses [Conventional Commits](https://www.conventionalcommits.org/). Each commit is **one coherent concern**:

```
<type>(<scope>): <imperative summary>
```

Types: `fix`, `feat`, `refactor`, `test`, `docs`, `chore`. Add a scope (e.g. `fix(scenario): …`) when it narrows the subsystem. Use a commit body only when rationale, risk, or follow-up needs explanation.

## The machine-readable CLI contract

The CLI exposes a stable, agent-facing contract via `dss commands run --json` and `dss agent contract`. Treat these outputs as a compatibility boundary — do not change command names, flags, exit codes, or the contract shape without a deliberate, versioned reason.

## Pull requests

- Branch from `main`, keep the change focused, and keep the checks above green.
- Fill out the pull request template.
- Behavioral changes are versioned per [semver](https://semver.org/) at release time (patch for fixes, minor for features).

## Contribution permission and licensing

The project [license](LICENSE) does not permit modification without prior written approval, including changes made only for private or internal use. Before modifying the source for a proposed contribution, request and receive written approval from clssck through the [project repository](https://github.com/clssck/dataiku-sdk).

By submitting an approved contribution, you represent that you have the right to do so and grant clssck a perpetual, worldwide, non-exclusive, irrevocable, royalty-free, transferable, sublicensable, and relicensable copyright license to use, reproduce, modify, prepare derivative works from, publish, distribute, perform, display, and otherwise exploit that contribution. This contributor grant lets the Licensor incorporate and license the contribution; it does not grant the contributor any permission beyond the written approval and the project license.
