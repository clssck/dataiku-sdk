import { execFileSync, } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

// ---------------------------------------------------------------------------
// Agent definitions
// ---------------------------------------------------------------------------

export interface AgentDef {
	/** Display name */
	name: string;
	/** CLI binary name (for `which` detection) */
	binary: string;
	/** Config directory relative to HOME (for fallback detection) */
	configDir: string;
	/** Require config dir to exist even when binary is found (disambiguates shared binary names) */
	configDirRequired?: boolean;
	/** Skill path relative to HOME (global install) */
	globalPath: (home: string,) => string;
	/** Skill path relative to CWD (project install). null = not supported. */
	projectPath: string | null;
	/** File to write inside the skill directory */
	filename: string;
	/** Content generator: standard SKILL.md or Cursor MDC */
	content: () => string;
}

const SKILL_BODY = `# Dataiku DSS agent CLI

Use \`dss\` when an agent needs to inspect or change Dataiku DSS resources: projects, datasets, recipes, jobs, scenarios, folders, notebooks, SQL, variables, code envs, and connections.
If the installed \`dss\` binary is unavailable but the repository checkout is the current workspace, prefer \`bun --no-env-file src/cli.ts ...\` or \`bun --no-env-file ./bin/dss.js ...\`. Node users can invoke the same cross-runtime launcher as \`node ./bin/dss.js ...\`; when \`dist/\` has not been built, it delegates to Bun. From another working directory, pass the checkout's absolute \`bin/dss.js\` path to Bun or Node.
\`--no-env-file\` disables Bun's automatic preloading only; the CLI still applies its documented \`.env\` handling unless \`DATAIKU_DISABLE_ENV=1\` is set.

## Contract

- Command results write exactly one JSON value to stdout, including \`doctor\`, \`batch\`, and \`cleanup\` failure reports; use the exit code and result fields.
- Dispatch/runtime failures write one JSONL error event to stderr with \`type:"error"\`, \`ok:false\`, \`error\`, \`code\`, and \`exitCode\`, while stdout stays empty.
- Non-fatal warnings and \`--verbose\` HTTP traces are JSONL stderr events (\`type:"warning"\` / \`type:"trace"\`), never prose.
- No prompts, help screens, tables, banners, or prose output are part of the contract.
- Exit codes: 0 success, 1 usage/configuration error, 2 DSS or internal error, 3 transient/retryable DSS error, 4 a failed long-running result or synchronous assertion.
- Pass \`--json\` for compact single-line JSON on stdout; use it for agent discovery and other JSON results to minimize tokens. Without it, success JSON is pretty-printed.
- \`--raw\` is the only stdout escape hatch: recipe payload commands emit raw bytes to stdout unless \`--output PATH\` is also set; with \`--output\`, stdout is the JSON string equal to \`PATH\` and the file receives exact raw bytes.
- \`--fields a,b,c\` projects those fields from object or array-of-objects results; dotted paths (\`a.b.c\`) drill into nested objects, and missing fields become \`null\`; string and scalar results pass through unchanged.

## Discover commands

\`\`\`text
dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility --json
dss commands run --fields dataset.create --json
dss commands run --fields dataset.create.usage,dataset.create.description,dataset.create.flags,dataset.create.examples --json
dss commands run --fields dataset --json
dss agent contract --fields commands.actions --json
\`\`\`

Bootstrap with the scoped \`agent contract\` call above: \`protocol,agentContractVersion,cli,stdio,planning,compatibility\` cover protocol/schema compatibility, stderr event semantics, preferred discovery commands, non-JSON escape hatches, planning rules, and compatibility guarantees in ~250 tokens. Request \`schemas\` (the JSON schemas) or \`commands\` (the contract's discovery section) only when you actually need them.

Before choosing command syntax, look the action up in the command registry: \`--fields RESOURCE.ACTION\` returns one complete entry, and appended \`.FIELD\` paths return only the metadata you need. Prefer the four-field projection \`usage,description,flags,examples\` by default — it is the smallest self-sufficient starting point for constructing an invocation. The registry entry is the canonical schema for flags, positional arguments, side effects, auth requirements, output shape, idempotency, dry-run support, structured examples, payload schemas, unsafe outputs, cleanup hints, and exit codes.

\`--fields\` takes a comma-separated list, so request several actions in one call. Each key is echoed exactly as requested (\`--fields dataset.create\` returns \`{"dataset.create": {...}}\`). Enumerate every resource and action with \`dss agent contract --fields commands.actions --json\` (~1k tokens); the hundreds-of-thousands-token unscoped registry is compatibility-only. An unknown resource or action exits with code 1 and a JSON error envelope on stderr containing the valid options.
Credential lookup order is flags first, then \`DATAIKU_*\` environment variables, then saved credentials.
Set \`DATAIKU_DISABLE_ENV=1\` when a test must ignore both \`.env\` files and \`DATAIKU_*\` environment variables.
When \`.env\` loading is enabled, the CLI reads \`.env\` from the command's current working directory first and then the CLI build/root directory; the invocation directory wins on conflicting keys. Put test-specific \`.env\` files in the directory where you invoke \`dss\`.
For disposable agent tests, set \`DSS_CONFIG_DIR\` to a temporary directory so saved credentials never touch the real profile.

## Planning, safety, and generic inputs

- Before any registry entry with \`sideEffect:"write"\`, run the exact argv with \`--plan\` first. Planning is local: it returns the derived operation without resolving credentials or calling DSS. Check \`destructive\`, \`idempotency\`, \`async\`, \`unsafeOutputs\`, and \`exitCodes\` before execution.
- Use \`--dry-run\` only when that action's registry entry has \`dryRun:true\`. \`--plan\` explains the operation; \`--dry-run\` exercises the action-specific simulation path. Never add an unsupported flag.
- When a create/upload action advertises \`--record-cleanup\`, pass \`--record-cleanup cleanup.jsonl\`. \`dss cleanup --file cleanup.jsonl\` previews the recorded cleanup steps in reverse order and does not mutate DSS; add \`--apply\` only after checking the preview.
- For JSON payload actions, follow \`inputContract\`, \`requiredFlags\`, and \`requiredOneOf\`. Prefer \`--data-file PATH\` or \`--stdin\` when advertised instead of inline \`--data\`; this preserves exact JSON across shells and keeps large or sensitive payloads out of process arguments.
- Authenticated actions advertise \`--request-timeout MS\` and \`--retries N\`. Long-running actions separately advertise controls such as \`--timeout MS\`, \`--poll-interval MS\`, and log limits; use only the flags in that action's registry entry.
- Before live mutation tests, use \`dss fixtures --json\` to discover compatible test resources instead of guessing project objects.

## Authentication

Prefer environment variables for ephemeral agent runs. Use the syntax for the active shell:

POSIX shell:

\`\`\`sh
export DATAIKU_URL=https://dss.example.com
export DATAIKU_API_KEY=your-api-key
export DATAIKU_PROJECT_KEY=MYPROJ
\`\`\`

PowerShell:

\`\`\`powershell
$env:DATAIKU_URL = "https://dss.example.com"
$env:DATAIKU_API_KEY = "your-api-key"
$env:DATAIKU_PROJECT_KEY = "MYPROJ"
\`\`\`

Windows Command Prompt:

\`\`\`bat
set "DATAIKU_URL=https://dss.example.com"
set "DATAIKU_API_KEY=your-api-key"
set "DATAIKU_PROJECT_KEY=MYPROJ"
\`\`\`

To persist credentials for later invocations:

\`\`\`bash
dss auth login --url https://dss.example.com --api-key YOUR_KEY --project-key MYPROJ
\`\`\`

The command saves credentials and returns \`{"saved":true,"path":"..."}\`. \`DSS_CONFIG_DIR\` wins when set. Otherwise credentials use \`XDG_CONFIG_HOME/dataiku/credentials.json\`, the \`dataiku/credentials.json\` directory under \`APPDATA\` on Windows, or \`~/.config/dataiku/credentials.json\`.
\`auth login\` validates by listing accessible projects before saving credentials, so the API key must be allowed to call DSS project-list APIs.

TLS flags: \`--insecure\` disables certificate verification; \`--ca-cert PATH\` adds a PEM CA bundle. Environment equivalents: \`NODE_TLS_REJECT_UNAUTHORIZED\`, \`NODE_EXTRA_CA_CERTS\`.

## Common workflows

\`\`\`text
dss version
dss project list
dss dataset list --project-key MYPROJ
dss recipe get-payload compute_orders --raw --output code.py --project-key MYPROJ
dss recipe diff compute_orders --file code.py --project-key MYPROJ
dss recipe set-payload compute_orders --file code.py --project-key MYPROJ
dss job build-and-wait orders --include-logs --project-key MYPROJ
dss sql query --connection analytics --sql "select 1" --project-key MYPROJ
dss batch --data-file steps.json
\`\`\`
For fake-DSS smoke tests, return project lists as JSON arrays such as \`[{"projectKey":"MYPROJ","name":"My Project"}]\` from \`/public/api/projects/\`; recipe payload commands read \`/public/api/projects/<PROJECT>/recipes/<NAME>?includePayload=true\` and expect a JSON object shaped like \`{"recipe":{"name":"<NAME>","type":"python"},"payload":"..."}\`.

## Application release safety

Treat app release as explicit validate, compare, version, successor, verify, and permission gates.
The public app-manifest \`version\` field is raw metadata: writing it is NOT a publish transaction.
New instances inherit the template's raw \`version\`; existing instances are never upgraded in
place — create an additive successor (the old instance is preserved), verify it, then retire the
old one as a separate guarded step. Never infer private publish/recreate/rename,
recipient-sharing, or UI-click operations that the public DSS API does not expose.

\`\`\`text
dss app validate-manifest --project-key APP_TEMPLATE
dss app compare-manifest APP_ID --project-key RELEASE_INSTANCE
dss app manifest-version --project-key APP_TEMPLATE
dss app set-manifest-version --manifest-version 1.4.0 --project-key APP_TEMPLATE
# targetProjectKey in instance.json must be confirmed absent
dss app create-instance APP_ID --data-file instance.json --wait --record-cleanup cleanup.jsonl
dss app create-successor-instance APP_ID --from RELEASE_INSTANCE --to RELEASE_INSTANCE_V2 --copy-permissions --record-cleanup cleanup.jsonl
dss app verify-instance APP_ID --project-key RELEASE_INSTANCE_V2 --expect-version 1.4.0
dss cleanup --file cleanup.jsonl
dss app permissions-snapshot --project-key RELEASE_INSTANCE --output permissions.json
dss app permissions-diff --project-key RELEASE_INSTANCE --file permissions.json
dss app permissions-restore --project-key RELEASE_INSTANCE --file permissions.json --dry-run
\`\`\`

An invalid manifest exits non-zero with the validation result in the structured error details.
Review the cleanup preview before \`dss cleanup --file cleanup.jsonl --apply\`. Every new entry is
bound to the canonical DSS URL. App cleanup records a \`creationTag\` hash observed after the DSS
future identifies its target key; unconfirmed creation stops unresolved. The future target and
later \`creationTag\` are independent, non-atomic observations because the public API exposes
neither an immutable project ID joined to the future nor a conditional DELETE. Cleanup rechecks
type and \`creationTag\` immediately before deletion, rejecting detected key reuse but unable to
eliminate replacement in the remaining check-to-DELETE gap. Apply validates the full ledger before
any request and rejects legacy, mixed-server, mismatched-server, or unbound app cleanup entries.
The successor's cleanup ledger entry targets only the new project key;
the predecessor is never targeted. Some DSS deployments return 403 for an unknown target project:
instance creation requires confirmed target absence before POST, so an inaccessible target absent
from the visible project list is still rejected as unconfirmed rather than risking cleanup against
a pre-existing project. A definitive create rejection never produces a cleanup entry.

\`app set-manifest-version\` reports
\`concurrencyControl:"client-side-non-atomic-stale-read-check"\`. \`--expect-hash SHA256\` refuses the
write when the manifest already changed, but the PUT itself stays unconditional: this command can
overwrite a writer that commits inside the read-then-write window, and the post-write read cannot
detect that lost update when this command's payload wins. Never treat the hash as a serializing lock.
An ambiguous manifest PUT or verification read exits non-zero with \`persisted:null\`,
\`after:null\`, and \`outcome:"indeterminate"\`; it never claims success or rejection.

The API key authenticates public REST only. \`app verify-instance\` reports \`apiReady:true\`,
\`status:"API_VERIFIED_UI_PENDING"\`, and \`uiPublicationVerified:false\`. Its \`visual-ui\` gate
requires a pre-authenticated SSO browser session or dedicated UI test identity and evidence from
exercising the affected tiles, forms, and actions; the CLI never marks that external gate complete.
Permission snapshots have an integrity hash bound to the canonical DSS URL, project key, and
observed concrete project incarnation from \`creationTag\`; diff and restore reject detected server,
key, or incarnation mismatches. DSS exposes no conditional permission PUT, so these client-side checks
narrow and detect key-reuse races but cannot serialize the final check with the write. Snapshots
contain access-control identities; keep them mode \`0600\` and commit them only when repository
policy permits.

## Confirming mutations

Mutations print a small JSON ack to stdout and exit 0 on success (e.g. \`{"updated":"NAME","resource":"recipe"}\`); on failure they print the error envelope to stderr and exit non-zero. The exit code is the source of truth.

- For portable multi-step writes, prefer \`dss batch\` (payload: a JSON array of argv arrays). It runs fail-fast, returns one envelope with per-step \`ok\`/\`result\`/\`error\`, and exits non-zero if any step fails.
- If separate processes are required, inspect each exit code and stop before launching the next step. Do not rely on shell chaining: syntax differs between POSIX shells, Windows PowerShell 5.1, PowerShell 7, and Command Prompt.
- Never pipe a mutation into a command that prints a fixed string or merges stderr: a pipeline may return the helper's exit code and report a failed mutation as success.
- To branch in code, key off the exit code or the JSON ack on stdout — never a hardcoded label.

## Platform & debugging notes

- Pass code and SQL via \`--file\`/\`--sql-file\`, not inline: shells (especially PowerShell) mangle quotes, \`$\`, and newlines in multi-line snippets.
- On a non-UTF-8 console (e.g. Windows cp1252), don't print non-ASCII results; write them to a UTF-8 file and read that, or use \`--output PATH\`.
- Build failures: \`dss job log <id> --errors-only\` surfaces just error/traceback lines, and \`--output PATH\` saves the full log to a file. Logs are one long line with JVM noise; the \`Error in Python process: At line <N>\` marker maps \`<N>\` straight to your recipe payload's source line.
- Schema changes aren't automatic for code recipes: after changing a python/SQL/prepare recipe's output columns run \`dss dataset refresh-schema\` (or rebuild) before downstream reads, and \`dss dataset validate-build\` to catch file-backed misconfig before launching a build. Exception: \`dss recipe create --type sync\` copies the input schema onto a schemaless dataset output at create time (see \`syncOutputSchemaPropagated\` in the result), so a fresh sync output builds populated without a manual refresh.
- \`dss dataset download\` is capped (default 100k rows) and returns \`{ path, rows, truncated, limit }\`: check \`truncated\` and raise \`--limit N\` when you need more — treat it as a sample, not a guaranteed full export. Without \`--output\` the file lands in the current working directory (and a \`dataset_download_default_location\` warning names that path); pass \`--output PATH\` to control the destination. For very large tables, aggregate in SQL or read inside a recipe instead.
- \`dss api-service list-packages SERVICE\` first verifies the parent service through its settings. A \`not_found\` error means the service is missing (verify the service ID and project key); an empty array means the service exists but has no deployable packages. Do not reinterpret \`not_found\` as an empty package list or retry the lower-level packages route.
- \`dss notebook clear-jupyter-outputs NAME --dry-run\` returns the full \`current\` and \`next\` notebook states. Applying it clears \`outputs\` and resets \`execution_count\` to \`0\` only on code cells, preserving markdown and raw cells unchanged.
- \`dss sql query ... --dataset PROJECT.NAME\` first queries through the dataset. If DSS rejects that dataset connection as neither SQL nor HDFS, the CLI reads dataset metadata and retries with \`params.connection\` (using its schema or catalog as the database when available). A readable dataset with no usable connection exits with \`validation_failed\` and advises \`--connection\`; dataset metadata \`404\` and \`403\` errors propagate as \`not_found\` and \`permission_denied\`.

## Error envelope

For dispatch/runtime failures, parse stderr as JSONL; the final line is an error event:

\`\`\`json
{
  "type": "error",
  "ok": false,
  "error": "Missing API key.",
  "code": "usage_error",
  "category": "usage",
  "exitCode": 1,
  "resource": "dataset",
  "action": "list"
}
\`\`\`

\`doctor\`, \`batch\`, and \`cleanup\` report command-level failures on stdout and leave stderr empty. Use the exit code before choosing a stream.
Use \`code\`, \`category\`, \`exitCode\`, \`retryable\`, \`status\`, and \`details\` for recovery logic. Do not scrape message text when a structured field is available.
Treat \`details.body\` as sanitized metadata only: it contains at most \`requestId\`/\`request_id\`/\`errorId\`/\`elapsedMs\` plus locally trusted target/timing fields, not the arbitrary DSS response body. \`details.statusText\` is canonical text derived from the numeric status, not a remote reason phrase. Base recovery on top-level \`code\`, \`category\`, \`status\`, \`retryable\`, \`requestId\`, \`hint\`, and \`details.dssCategory\`; never parse or expose assumed server-body fields.
`;

const SKILL_FRONTMATTER = `---
name: dataiku-dss
description: >-
  Agent-only JSON CLI for Dataiku DSS. Use to inspect or mutate DSS projects,
  datasets, recipes, jobs, scenarios, folders, notebooks, SQL, variables,
  code envs, and connections. Discover scoped command metadata with
  dss commands run --fields RESOURCE.ACTION --json.
---

`;

function skillContent(): string {
	return SKILL_FRONTMATTER + SKILL_BODY;
}

export const AGENTS: Record<string, AgentDef> = {
	claude: {
		name: "Claude Code",
		binary: "claude",
		configDir: ".claude",
		globalPath: (home,) => path.join(home, ".claude", "skills", "dataiku-dss",),
		projectPath: ".claude/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	codex: {
		name: "Codex",
		binary: "codex",
		configDir: ".codex",
		globalPath: (home,) => path.join(home, ".codex", "skills", "dataiku-dss",),
		projectPath: ".codex/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	cursor: {
		name: "Cursor",
		binary: "cursor",
		configDir: ".cursor",
		globalPath: (home,) => path.join(home, ".cursor", "skills", "dataiku-dss",),
		projectPath: ".cursor/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	pi: {
		name: "Pi",
		binary: "pi",
		configDir: ".pi",
		globalPath: (home,) => path.join(home, ".pi", "agent", "skills", "dataiku-dss",),
		projectPath: ".pi/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	omp: {
		name: "OhMyPi",
		binary: "omp",
		configDir: path.join(".omp", "agent",),
		configDirRequired: true,
		globalPath: (home,) => path.join(home, ".omp", "agent", "skills", "dataiku-dss",),
		projectPath: ".omp/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
};

// ---------------------------------------------------------------------------
// Agent detection
// ---------------------------------------------------------------------------

function binaryExists(name: string,): boolean {
	const cmd = process.platform === "win32" ? "where" : "which";
	try {
		execFileSync(cmd, [name,], { stdio: "pipe", },);
		return true;
	} catch {
		return false;
	}
}

export interface DetectedAgent {
	id: string;
	def: AgentDef;
	via: "binary" | "config-dir" | "flag";
}

export function detectAgents(): DetectedAgent[] {
	const home = os.homedir();
	const found: DetectedAgent[] = [];
	for (const [id, def,] of Object.entries(AGENTS,)) {
		const hasBinary = binaryExists(def.binary,);
		const hasConfigDir = fs.existsSync(path.join(home, def.configDir,),);
		if (hasBinary && (!def.configDirRequired || hasConfigDir)) {
			found.push({ id, def, via: "binary", },);
		} else if (hasConfigDir) {
			found.push({ id, def, via: "config-dir", },);
		}
	}
	return found;
}

// ---------------------------------------------------------------------------
// Skill installation
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Workspace root detection
// ---------------------------------------------------------------------------

const WORKSPACE_MARKERS = [".git",];

/**
 * Walk upward from startDir looking for strong project markers.
 * Agent config directories are install targets, not workspace roots.
 */
export function findWorkspaceRoot(startDir: string,): string {
	let dir = startDir;
	for (let i = 0; i < 20; i++) {
		for (const marker of WORKSPACE_MARKERS) {
			if (fs.existsSync(path.join(dir, marker,),)) return dir;
		}
		const parent = path.dirname(dir,);
		if (parent === dir) break;
		dir = parent;
	}
	return startDir;
}

export interface InstallResult {
	agent: string;
	path: string;
	via: DetectedAgent["via"];
}

export function planSkillInstalls(
	agents: DetectedAgent[],
	opts: { global: boolean; cwd: string; },
): InstallResult[] {
	const home = os.homedir();
	const results: InstallResult[] = [];

	for (const { id, def, via, } of agents) {
		const dir = opts.global
			? def.globalPath(home,)
			: def.projectPath
			? path.join(opts.cwd, def.projectPath,)
			: undefined;
		if (!dir) continue;
		results.push({ agent: id, path: path.join(dir, def.filename,), via, },);
	}

	return results;
}

export function installSkill(
	agents: DetectedAgent[],
	opts: { global: boolean; cwd: string; },
): InstallResult[] {
	const results = planSkillInstalls(agents, opts,);

	for (const result of results) {
		const def = AGENTS[result.agent];
		if (!def) continue;
		fs.mkdirSync(path.dirname(result.path,), { recursive: true, },);
		fs.writeFileSync(result.path, def.content(), "utf-8",);
	}

	return results;
}
