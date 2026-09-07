# Coding and project libraries

## Project Git

UAT=`create-branch --duplicate-project`; pull=rebase; checkout=switch. Remotes: DSS Git rules and
SSH/server credentials. `--password-env`; `future-wait`.

## Coding and libraries

Use `project-library` for internal `lib/`; `project-git list-libraries` for external Git repos.
Safe edit: `get-bytes` → `diff` → `put --expect-sha256 HASH`; create with
`--if-not-exists`. Paths reject traversal.

Run Python with `code run --file`; inspect/diff/backup recipes before `recipe run --dry-run`.
Notebook saves report hashes and accept `--expect-hash`; output clearing uses DSS DELETE, while
`unload-jupyter --all` composes session deletes. Inspect code-env definitions/logs before updates;
guard webapp/API settings with `--expect-hash`. `exact:false` means live state was not guessed:
use command `--dry-run`. Public APIs expose no plugin authoring, notebook execute/checkpoints, or
webapp/API delete; use project Git, code/recipes, or the DSS UI.
