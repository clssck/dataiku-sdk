# Coding, Git, libraries

- Git: UAT=`create-branch --duplicate-project`; pull=rebase; checkout=switch. Remotes use DSS Git rules and SSH/server credentials. `--password-env`; `future-wait`.
- Internal `lib/`: `project-library`. External Git: `project-git list-libraries`. Safe edit: `get-bytes` → `diff` → `put --expect-sha256 HASH`; create with `--if-not-exists`. Paths reject traversal.
- Python: `code run --file`. Inspect/diff/backup recipes before `recipe run --dry-run`.
- Grouping/prepare: build with `recipe create` flags (`--group-by`, `--aggregate`; `--rename`, `--formula`, `--filter`, ...). Other visual payloads (join): no published schema; template = `recipe get-payload` after create. Join `MANUAL` selection uses top-level `selectedColumns: [{name, table, alias?}]` (`table` = input index). DSS-engine computed columns run as H2 SQL: no GREL `+` concat, use `SQL` mode `||`. Grouping `first` needs an ordering column.
- Notebook saves return hashes; guard with `--expect-hash`. Clear outputs via DSS DELETE; `unload-jupyter --all` composes session deletes.
- Inspect code-env definitions/logs before updates; guard webapp/API settings with `--expect-hash`. `exact:false`: live state not guessed; use command `--dry-run`.
- Plugin authoring: `dss plugin` create-dev, move-to-dev, contents-put, push/pull. No notebook execute/checkpoints or webapp/API delete; use the DSS UI.
