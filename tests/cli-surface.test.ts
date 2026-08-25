import { describe, expect, it, } from "bun:test";
import { execFile, } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath, } from "node:url";
import { promisify, } from "node:util";

const exec = promisify(execFile,);
const SDK_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url,),), "..",);
const CLI_PATH = path.join(SDK_ROOT, "src/cli.ts",);
const BUN = process.execPath;
const CLI_MAX_BUFFER_BYTES = 16 * 1024 * 1024;

type CommandRegistryEntry = {
	resource: string;
	action: string;
	usage: string;
	description?: string;
	examples?: string[];
	structuredExamples: Array<{ shell: string; argv?: string[]; payload?: unknown; }>;
	flags: Array<
		{ name: string; kind: "boolean" | "value"; valueType?: string; enumValues?: string[]; }
	>;
	positionals: string[];
	sideEffect: "read" | "write" | "auth";
	outputShape: "object" | "array" | "string" | "void";
	inputContract: { stdin?: boolean; dataFlag?: boolean; dataFileFlag?: boolean; };
	destructive: "none" | "reversible" | "destructive";
	producesLocalFile: boolean;
	mutatesDss: boolean;
	async: "none" | "job" | "future";
	idempotency: "safe" | "convergent" | "if-not-exists" | "if-exists" | "none";
	dryRun: boolean;
	requiredFlags: string[];
	optionalFlags: string[];
	requiredOneOf?: Array<{ oneOf: string[][]; }>;
	payloadSchema?: {
		stdin?: boolean;
		dataFlag?: boolean;
		dataFileFlag?: boolean;
		jsonShape?: "object" | "array";
	};
	examplePayload?: unknown;
	unsafeOutputs?: Array<
		{ condition: string; kind: string; detail: string; safeAlternative?: string; }
	>;
	schemas: {
		argv: Record<string, unknown>;
		input?: Record<string, unknown>;
		output: Record<string, unknown>;
	};
	cleanupCommand?: string;
	exitCodes: {
		ok: 0;
		usage: 1;
		error: 2;
		transient: 3;
		longRunningFailure?: 4;
		assertionFailure?: 4;
	};
	cleanupHint?: string;
	requiresAuth: boolean;
	requiresProject: boolean;
	agentContractVersion: number;
};

type CommandRegistry = Record<string, Record<string, CommandRegistryEntry>>;

const EXPECTED_COMMANDS: Record<string, string[]> = {
	code: ["run",],
	project: [
		"list",
		"get",
		"metadata",
		"flow",
		"map",
		"create",
		"delete",
		"duplicate",
		"export",
		"import",
		"permissions-get",
		"permissions-set",
		"settings-get",
		"settings-set",
	],
	"flow-zone": ["list", "find", "get", "create", "update", "delete", "move", "organize", "graph",],
	dashboard: ["list", "get", "export", "create", "update", "delete",],
	"data-quality": [
		"rules",
		"get-rule",
		"status",
		"create-rule",
		"update-rule",
		"delete-rule",
		"status-by-partition",
		"last-results",
		"history",
		"project-status",
		"project-timeline",
		"compute",
	],
	future: ["get", "peek", "wait", "abort",],
	dataset: [
		"list",
		"get",
		"schema",
		"source",
		"validate-build",
		"refresh-schema",
		"preview",
		"metadata",
		"download",
		"create",
		"delete",
		"update",
		"clone",
		"rename",
		"list-partitions",
		"clear",
	],
	recipe: [
		"list",
		"get",
		"delete",
		"run",
		"validate-graph",
		"download",
		"download-code",
		"create",
		"diff",
		"update",
		"add-input",
		"remove-input",
		"get-payload",
		"cat",
		"set-payload",
		"clone",
		"restore",
		"assert-unchanged",
	],
	job: [
		"list",
		"get",
		"summary",
		"log",
		"log-url",
		"build",
		"build-and-wait",
		"wait",
		"monitor",
		"watch",
		"abort",
	],
	scenario: ["list", "get", "run", "run-and-wait", "status", "delete", "create", "update",],
	folder: [
		"list",
		"create",
		"get",
		"update",
		"delete",
		"contents",
		"download",
		"upload",
		"delete-file",
	],
	variable: ["get", "set",],
	connection: ["list", "infer", "schemas", "tables",],
	insight: ["list", "get", "create", "update", "delete",],
	"code-env": [
		"list",
		"get",
		"get-definition",
		"create",
		"set-definition",
		"set-packages",
		"update-packages",
		"set-jupyter",
		"delete",
		"usages",
	],
	sql: ["query",],
	notebook: [
		"list-jupyter",
		"get-jupyter",
		"save-jupyter",
		"delete-jupyter",
		"clear-jupyter-outputs",
		"sessions-jupyter",
		"unload-jupyter",
		"list-sql",
		"get-sql",
		"save-sql",
		"delete-sql",
		"history-sql",
		"clear-sql-history",
	],
	wiki: ["settings", "list", "get", "create", "update", "delete",],
	auth: ["login",],
	doctor: ["run",],
	commands: ["run",],
	agent: ["contract",],
	version: ["run",],
	"install-skill": ["run",],
	cleanup: ["run",],
	fixtures: ["run",],
	batch: ["run",],
	app: [
		"list",
		"manifest",
		"manifest-version",
		"instances",
		"create-instance",
		"create-successor-instance",
		"instance-manifest",
		"save-instance-manifest",
		"set-manifest-version",
		"validate-manifest",
		"verify-instance",
		"compare-manifest",
		"delete-instance",
		"permissions-snapshot",
		"permissions-diff",
		"permissions-restore",
		"business-app-instance-permissions",
	],
	"business-app": [
		"list",
		"get",
		"settings",
		"save-settings",
		"instances",
		"create-instance",
		"upgrade-instance",
		"install-from-archive",
	],
	webapp: [
		"list",
		"get-settings",
		"create",
		"update-settings",
		"stop-backend",
		"restart-backend",
		"backend-state",
	],
	analysis: ["list", "get", "create", "delete",],
	"ml-task": [
		"create",
		"status",
		"get-settings",
		"set-settings",
		"train",
		"list-models",
		"model-details",
		"deploy",
		"delete",
	],
	"saved-model": ["list", "get", "list-versions", "version-details", "set-active", "delete",],
	"model-evaluation-store": ["list", "get", "create", "list-evaluations", "delete",],
	"api-service": [
		"list",
		"create",
		"get-settings",
		"save-settings",
		"add-prediction-endpoint",
		"list-packages",
		"package-summary",
		"create-package",
		"delete-package",
		"download-package",
		"publish-package",
	],
	"api-deployer": [
		"list-infras",
		"create-infra",
		"get-infra",
		"delete-infra",
		"list-stages",
		"list-services",
		"create-service",
		"get-service",
		"delete-service",
		"publish-version",
		"delete-version",
		"list-deployments",
		"create-deployment",
		"get-deployment",
		"deployment-status",
		"deployment-settings",
		"save-deployment-settings",
		"deploy",
		"delete-deployment",
	],
	bundle: [
		"list-exported",
		"export",
		"delete-exported",
		"download-exported",
		"publish",
		"list-imported",
		"import-from-archive",
		"import-from-stream",
		"activate",
		"preload",
		"delete-imported",
	],
	"project-deployer": [
		"list-projects",
		"create-project",
		"upload-bundle",
		"project-status",
		"list-deployments",
		"create-deployment",
		"get-deployment",
		"deployment-status",
		"save-deployment-settings",
		"deploy",
		"delete-deployment",
		"list-infras",
		"create-infra",
	],
	"project-library": [
		"list",
		"get",
		"get-bytes",
		"create-file",
		"create-folder",
		"put",
		"delete",
		"rename",
		"move",
	],
	"streaming-endpoint": ["list", "get", "create", "update-settings", "delete",],
	"continuous-activity": ["list", "status", "start", "stop",],
	statistics: [
		"list-worksheets",
		"get-worksheet",
		"create-worksheet",
		"update-worksheet",
		"delete-worksheet",
		"run-worksheet",
		"run-card",
		"run-computation",
	],
	discussion: ["list", "get", "create", "reply",],
	workspace: ["list", "get", "create", "update-settings", "delete", "list-objects", "add-object",],
	metrics: ["dataset-get", "dataset-compute", "dataset-history", "folder-get",],
	meaning: ["list", "get", "create", "update", "delete",],
};

async function dss(args: string[],): Promise<{ stdout: string; stderr: string; }> {
	return exec(BUN, ["run", CLI_PATH, ...args,], {
		cwd: SDK_ROOT,
		env: {
			...process.env,
			DATAIKU_URL: "",
			DATAIKU_API_KEY: "",
			DATAIKU_PROJECT_KEY: "",
		},
		maxBuffer: CLI_MAX_BUFFER_BYTES,
	},);
}

function expectedCleanupCommand(deleteUsage: string,): string {
	const base = deleteUsage.replace(/\[[^\]]*\]/g, " ",).replace(/\s+/g, " ",).trim();
	if (deleteUsage.includes("--if-exists",)) return `${base} --if-exists`;
	return base;
}
/**
 * Default discovery prints a compact resource -> action-name summary, so stdout is
 * never the full registry.
 */
async function readActionSummary(): Promise<Record<string, string[]>> {
	const { stdout, stderr, } = await dss(["commands", "run",],);
	expect(stderr,).toBe("",);
	return JSON.parse(stdout,) as Record<string, string[]>;
}

/**
 * Exports the entire registry through `--output` (stdout only carries the written
 * path) and removes the temporary directory even when an assertion throws.
 */
async function exportedCommandRegistry(): Promise<CommandRegistry> {
	const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dss-commands-",),);
	const outputPath = path.join(dir, "registry.json",);
	try {
		const { stdout, stderr, } = await dss(["commands", "run", "--output", outputPath,],);
		expect(stderr,).toBe("",);
		expect(JSON.parse(stdout,), "commands run --output echoes the written path",).toMatchObject({
			path: outputPath,
		},);
		return JSON.parse(fs.readFileSync(outputPath, "utf-8",),) as CommandRegistry;
	} finally {
		fs.rmSync(dir, { recursive: true, force: true, },);
	}
}

/**
 * Scoped metadata for whole resources; resource-level selectors keep the nested
 * `registry[resource][action]` shape.
 */
async function scopedCommandRegistry(resources: string[],): Promise<CommandRegistry> {
	const { stdout, stderr, } = await dss(["commands", "run", "--fields", resources.join(",",),],);
	expect(stderr,).toBe("",);
	return JSON.parse(stdout,) as CommandRegistry;
}

describe("CLI command surface", () => {
	it("exposes every supported resource/action in the machine-readable registry", async () => {
		const summary = await readActionSummary();
		const registry = await exportedCommandRegistry();
		expect(Object.keys(summary,).sort(),).toEqual(Object.keys(EXPECTED_COMMANDS,).sort(),);
		expect(Object.keys(registry,).sort(),).toEqual(Object.keys(EXPECTED_COMMANDS,).sort(),);
		for (const [resource, actions,] of Object.entries(EXPECTED_COMMANDS,)) {
			expect(summary[resource], `${resource} summary actions`,).toEqual(actions.slice().sort(),);
			expect(Object.keys(registry[resource] ?? {},).sort(),).toEqual(actions.slice().sort(),);
			for (const action of actions) {
				const meta = registry[resource]?.[action];
				const usageOmitsRunAction = action === "run"
					&& ["doctor", "install-skill", "cleanup", "fixtures", "version", "batch",].includes(resource,);
				const expectedUsagePrefix = usageOmitsRunAction
					? `dss ${resource}`
					: `dss ${resource} ${action}`;
				expect(meta?.usage, `${resource} ${action} usage`,).toContain(expectedUsagePrefix,);
				expect(meta?.description?.length ?? 0, `${resource} ${action} description`,).toBeGreaterThan(
					0,
				);
				expect(meta?.resource, `${resource} ${action} resource`,).toBe(resource,);
				expect(meta?.action, `${resource} ${action} action`,).toBe(action,);
				expect(Array.isArray(meta?.flags,), `${resource} ${action} flags`,).toBe(true,);
				expect(Array.isArray(meta?.positionals,), `${resource} ${action} positionals`,).toBe(true,);
				expect(["object", "array", "string", "void",], `${resource} ${action} outputShape`,).toContain(
					meta?.outputShape,
				);
				expect(
					meta?.inputContract && typeof meta.inputContract === "object",
					`${resource} ${action} inputContract`,
				).toBe(true,);
				expect(["none", "reversible", "destructive",], `${resource} ${action} destructive`,).toContain(
					meta?.destructive,
				);
				expect(typeof meta?.producesLocalFile, `${resource} ${action} producesLocalFile`,).toBe(
					"boolean",
				);
				expect(typeof meta?.mutatesDss, `${resource} ${action} mutatesDss`,).toBe("boolean",);
				expect(["none", "job", "future",], `${resource} ${action} async`,).toContain(meta?.async,);
				expect(
					["safe", "convergent", "if-not-exists", "if-exists", "none",],
					`${resource} ${action} idempotency`,
				).toContain(meta?.idempotency,);
				expect(typeof meta?.dryRun, `${resource} ${action} dryRun`,).toBe("boolean",);
				expect(Array.isArray(meta?.requiredFlags,), `${resource} ${action} requiredFlags`,).toBe(true,);
				expect(Array.isArray(meta?.optionalFlags,), `${resource} ${action} optionalFlags`,).toBe(true,);
				expect(meta?.agentContractVersion, `${resource} ${action} agentContractVersion`,).toBe(2,);
				expect(Array.isArray(meta?.structuredExamples,), `${resource} ${action} structuredExamples`,)
					.toBe(
						true,
					);
				expect(
					meta?.schemas && typeof meta.schemas === "object",
					`${resource} ${action} schemas`,
				).toBe(true,);
				expect(
					meta?.schemas.argv && typeof meta.schemas.argv === "object",
					`${resource} ${action} argv schema`,
				).toBe(true,);
				expect(
					meta?.schemas.output && typeof meta.schemas.output === "object",
					`${resource} ${action} output schema`,
				).toBe(true,);
				for (const flag of meta?.requiredFlags ?? []) {
					expect(
						meta?.flags.some((registered,) => registered.name === flag),
						`${resource} ${action} required flag ${flag} registered`,
					).toBe(true,);
				}
				for (const flag of meta?.optionalFlags ?? []) {
					expect(
						meta?.flags.some((registered,) => registered.name === flag),
						`${resource} ${action} optional flag ${flag} registered`,
					).toBe(true,);
				}
				for (const choice of meta?.requiredOneOf ?? []) {
					expect(
						choice.oneOf.length,
						`${resource} ${action} requiredOneOf alternatives`,
					).toBeGreaterThan(1,);
					for (const alternative of choice.oneOf.flat()) {
						expect(
							meta?.flags.some((registered,) => registered.name === alternative),
							`${resource} ${action} oneOf flag ${alternative} registered`,
						).toBe(true,);
						expect(
							meta?.requiredFlags.includes(alternative,),
							`${resource} ${action} oneOf flag ${alternative} not also required`,
						).toBe(false,);
						expect(
							meta?.optionalFlags.includes(alternative,),
							`${resource} ${action} oneOf flag ${alternative} not also optional`,
						).toBe(false,);
					}
				}
				expect(meta?.exitCodes, `${resource} ${action} exitCodes`,).toEqual({
					ok: 0,
					usage: 1,
					error: 2,
					transient: 3,
					...(meta?.async !== "none" || (resource === "batch" && action === "run")
						? { longRunningFailure: 4, }
						: {}),
					...((resource === "recipe" && action === "assert-unchanged")
							|| (resource === "batch" && action === "run")
						? { assertionFailure: 4, }
						: {}),
				},);
			}
		}

		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const [action, meta,] of Object.entries(actions,)) {
				if (!action.startsWith("create",)) continue;
				if (resource === "project") continue;
				const deleteAction = action === "create-rule" ? "delete-rule" : "delete";
				const deleteMeta = registry[resource]?.[deleteAction];
				if (!deleteMeta) continue;
				expect(meta.cleanupCommand, `${resource} ${action} cleanupCommand`,).toBe(
					expectedCleanupCommand(deleteMeta.usage,),
				);
			}
		}

		expect(registry.dataset.delete.sideEffect,).toBe("write",);
		expect(registry.dataset.delete.requiresProject,).toBe(true,);
		expect(registry.dataset.delete.flags,).toContainEqual(
			expect.objectContaining({ name: "dry-run", kind: "boolean", },),
		);
		expect(registry.dataset.delete.dryRun,).toBe(true,);
		expect(registry.dataset.create.flags,).toContainEqual(
			expect.objectContaining({ name: "dry-run", kind: "boolean", },),
		);
		expect(registry.dataset.create.flags,).toContainEqual(
			expect.objectContaining({ name: "if-not-exists", kind: "boolean", },),
		);
		expect(registry.dataset.create.requiredFlags,).toEqual(["name", "connection", "type",],);
		expect(registry.dataset.create.optionalFlags,).toContain("dry-run",);
		expect(registry.dataset.create.cleanupCommand,).toBe("dss dataset delete <name> --if-exists",);
		expect(registry.dataset.create.idempotency,).toBe("if-not-exists",);
		expect(registry.app["create-instance"].cleanupCommand,).toBeUndefined();
		expect(registry.app["create-instance"].cleanupHint,).toContain("--record-cleanup",);
		expect(registry.app["create-instance"].async,).toBe("future",);
		expect(registry.app["create-successor-instance"].cleanupCommand,).toBeUndefined();
		expect(registry.app["create-successor-instance"].cleanupHint,).toContain("dss cleanup --file",);
		expect(registry.app["permissions-snapshot"].description,).toContain(
			"commit it only when repository policy permits",
		);
		expect(registry.app["permissions-snapshot"].unsafeOutputs,).toContainEqual(
			expect.objectContaining({
				kind: "local-file",
				detail: expect.stringContaining("access-control identities",),
				safeAlternative: expect.stringContaining("outside version control",),
			},),
		);
		expect(registry.app["create-successor-instance"].async,).toBe("future",);
		expect(registry.app["delete-instance"].async,).toBe("future",);
		expect(registry.app["delete-instance"].exitCodes.longRunningFailure,).toBe(4,);
		expect(registry.app["delete-instance"].idempotency,).toBe("convergent",);
		expect(
			registry.app["delete-instance"].flags.some((flag,) => flag.name === "if-exists"),
			"delete-instance must converge without an --if-exists flag",
		).toBe(false,);
		expect(registry.batch.run.async,).toBe("none",);
		expect(registry.batch.run.exitCodes,).toMatchObject({ longRunningFailure: 4, },);
		expect(registry.app["create-successor-instance"].sideEffect,).toBe("write",);
		expect(registry.app["create-successor-instance"].flags.some((flag,) => flag.name === "wait"),)
			.toBe(
				false,
			);
		expect(
			registry.app["create-successor-instance"].flags.some((flag,) =>
				flag.name === "copy-permissions"
			),
		).toBe(true,);
		expect(registry.app["manifest-version"].sideEffect,).toBe("read",);
		expect(registry.app["verify-instance"].sideEffect,).toBe("read",);
		expect(registry.app["set-manifest-version"].sideEffect,).toBe("write",);
		expect(registry.app["set-manifest-version"].idempotency,).toBe("none",);
		expect(registry.dataset.update.payloadSchema,).toEqual({
			stdin: true,
			dataFlag: true,
			dataFileFlag: true,
			jsonShape: "object",
		},);
		expect(registry.dataset.update.examplePayload,).toEqual({ tags: ["production",], },);
		expect(registry.variable.set.requiredFlags,).toEqual([],);
		expect(registry.variable.set.requiredOneOf,).toEqual([
			{ oneOf: [["standard",], ["local",],], },
		],);
		expect(registry.variable.set.optionalFlags,).not.toContain("standard",);
		expect(registry.variable.set.optionalFlags,).not.toContain("local",);
		expect(registry.project.list.sideEffect,).toBe("read",);
		expect(registry.project.list.requiresProject,).toBe(false,);
		expect(registry.wiki.settings.sideEffect,).toBe("read",);
		expect(registry.wiki.settings.requiresProject,).toBe(true,);
		expect(registry.wiki.create.flags,).toContainEqual(
			expect.objectContaining({ name: "dry-run", kind: "boolean", },),
		);
		expect(registry.dashboard.create.flags,).toContainEqual(
			expect.objectContaining({ name: "dry-run", kind: "boolean", },),
		);
		expect(registry.dashboard.create.cleanupCommand,).toBe("dss dashboard delete <id> --if-exists",);
		expect(registry.insight.create.flags,).toContainEqual(
			expect.objectContaining({ name: "dry-run", kind: "boolean", },),
		);
		expect(registry.insight.create.cleanupCommand,).toBe("dss insight delete <id> --if-exists",);
		expect(registry["data-quality"].rules.sideEffect,).toBe("read",);
		expect(registry["data-quality"].status.sideEffect,).toBe("read",);
		expect(registry["data-quality"]["create-rule"].sideEffect,).toBe("write",);
		expect(registry["data-quality"]["create-rule"].cleanupCommand,).toBe(
			"dss data-quality delete-rule <dataset> <rule-id> --if-exists",
		);
		expect(registry["data-quality"].compute.sideEffect,).toBe("write",);
		expect(registry.job.build.flags,).toContainEqual(
			expect.objectContaining({ name: "wait", kind: "boolean", },),
		);
		expect(registry.job.build.async,).toBe("job",);
		expect(registry.job.build.exitCodes.longRunningFailure,).toBe(4,);
		expect(registry.job.build.destructive,).toBe("reversible",);
		expect(registry["data-quality"]["project-status"].flags,).toContainEqual(
			expect.objectContaining({ name: "only-monitored", kind: "value", },),
		);
		expect(registry["data-quality"].compute.flags,).toContainEqual(
			expect.objectContaining({ name: "wait", kind: "boolean", },),
		);
		expect(registry.future.wait.sideEffect,).toBe("read",);
		expect(registry.future.wait.requiresProject,).toBe(false,);
		expect(registry.future.abort.sideEffect,).toBe("write",);
		expect(registry["data-quality"]["create-rule"].flags,).toContainEqual(
			expect.objectContaining({ name: "dry-run", kind: "boolean", },),
		);
		expect(registry["code-env"]["update-packages"].flags,).toContainEqual(
			expect.objectContaining({ name: "force-rebuild", kind: "boolean", },),
		);
		expect(registry.auth.login.sideEffect,).toBe("auth",);
		expect(registry.auth.login.requiresAuth,).toBe(false,);
		expect(registry.auth.login.requiresProject,).toBe(false,);
		expect(registry.doctor.run.sideEffect,).toBe("read",);
		expect(registry.doctor.run.requiresAuth,).toBe(true,);
		expect(registry.commands.run.sideEffect,).toBe("read",);
		expect(registry.commands.run.requiresAuth,).toBe(false,);
		expect(registry.agent.contract.sideEffect,).toBe("read",);
		expect(registry.agent.contract.requiresAuth,).toBe(false,);
		expect(registry.agent.contract.structuredExamples[0]?.argv,).toEqual([
			"agent",
			"contract",
			"--fields=protocol,agentContractVersion,cli,stdio,planning,compatibility",
		],);
		expect(registry["install-skill"].run.sideEffect,).toBe("write",);
		expect(registry["install-skill"].run.requiresAuth,).toBe(false,);
		expect(registry["install-skill"].run.flags,).toContainEqual(
			expect.objectContaining({ name: "dry-run", kind: "boolean", },),
		);
		expect(registry.recipe["set-payload"].inputContract.dataFlag,).toBeUndefined();
		expect(registry.recipe["set-payload"].destructive,).toBe("reversible",);
		expect(registry.recipe["set-payload"].flags,).toContainEqual(
			expect.objectContaining({ name: "backup-dir", kind: "value", },),
		);
		expect(registry.recipe["set-payload"].flags,).toContainEqual(
			expect.objectContaining({ name: "no-backup", kind: "boolean", },),
		);
		expect(registry.recipe["get-payload"].flags,).not.toContainEqual(
			expect.objectContaining({ name: "raw", },),
		);
		expect(registry.recipe["get-payload"].unsafeOutputs ?? [], "recipe get-payload unsafe outputs",)
			.not.toContainEqual(
				expect.objectContaining({ kind: "raw-stdout", },),
			);
		expect(registry.recipe.cat.outputShape,).toBe("string",);
		expect(registry.dataset.clone.cleanupCommand,).toBe("dss dataset delete <name> --if-exists",);
		expect(registry.recipe.clone.cleanupCommand,).toBe("dss recipe delete <name> --if-exists",);
		expect(registry.dataset.clone.flags,).toContainEqual(
			expect.objectContaining({ name: "allow-same-path", kind: "boolean", },),
		);
		expect(registry.job.summary.sideEffect,).toBe("read",);
		expect(registry.job.watch.async,).toBe("job",);
		expect(registry.recipe.run.flags,).toContainEqual(
			expect.objectContaining({ name: "max-log-lines", kind: "value", },),
		);
		expect(registry.recipe.run.flags,).toContainEqual(
			expect.objectContaining({ name: "dry-run", kind: "boolean", },),
		);
		expect(registry.recipe.run.async,).toBe("job",);
		expect(registry.sql.query.flags,).toContainEqual(
			expect.objectContaining({ name: "output-file", kind: "value", },),
		);
		expect(registry.sql.query.flags,).toContainEqual(
			expect.objectContaining({ name: "output", kind: "value", },),
		);
		expect(registry.sql.query.producesLocalFile,).toBe(true,);
		expect(registry.sql.query.flags,).toContainEqual(
			expect.objectContaining({ name: "preview", kind: "value", valueType: "N", },),
		);
		expect(registry.sql.query.optionalFlags,).toContain("preview",);
		expect(registry.connection.schemas.outputShape,).toBe("array",);
		expect(registry.connection.tables.flags,).toContainEqual(
			expect.objectContaining({ name: "schema", kind: "value", },),
		);
		expect(registry.dataset["refresh-schema"].sideEffect,).toBe("write",);
		expect(registry.dataset["refresh-schema"].flags,).toContainEqual(
			expect.objectContaining({ name: "data", kind: "value", },),
		);
		expect(registry.sql.query.requiredFlags,).toEqual([],);
		expect(registry.sql.query.requiredOneOf,).toEqual([{
			oneOf: [["connection",], ["dataset",],],
		},],);
		expect(registry.sql.query.optionalFlags,).not.toContain("connection",);
		expect(registry.sql.query.optionalFlags,).not.toContain("dataset",);
		expect(registry.code.run.async,).toBe("future",);
		expect(registry.code.run.exitCodes.longRunningFailure,).toBe(4,);
		expect(registry.code.run.mutatesDss,).toBe(true,);
		expect(registry.code.run.requiresProject,).toBe(true,);
		expect(registry.code.run.requiredOneOf,).toEqual([{ oneOf: [["file",], ["stdin",],], },],);
		expect(registry.code.run.flags,).toContainEqual(
			expect.objectContaining({ name: "env", kind: "value", },),
		);
		expect(registry.code.run.optionalFlags,).toContain("full-log",);
		expect(registry.code.run.flags,).toContainEqual(
			expect.objectContaining({ name: "max-log-bytes", kind: "value", valueType: "N", },),
		);
		expect(registry.insight.create.requiredOneOf,).toEqual([
			{ oneOf: [["data",], ["data-file",], ["stdin",], ["name", "type",],], },
		],);
		expect(registry.recipe.create.requiredOneOf,).toEqual([{
			oneOf: [["output",], ["output-folder",],],
		},],);
		expect(registry.recipe.clone.requiredOneOf,).toEqual([{ oneOf: [["name",], ["to",],], },],);
		expect(registry.dataset.create.requiredOneOf,).toBeUndefined();
		expect(registry.recipe.create.flags.find((flag,) => flag.name === "output")?.valueType,).toBe(
			"DS",
		);
		expect(registry.sql.query.flags.find((flag,) => flag.name === "output")?.valueType,).toBe(
			"PATH",
		);
		expect(registry.sql.query.flags.find((flag,) => flag.name === "connection")?.valueType,).toBe(
			"CONN",
		);
		expect(
			registry["code-env"]["set-jupyter"].flags.find((flag,) => flag.name === "active"),
		).toEqual({ name: "active", kind: "value", valueType: "enum", enumValues: ["true", "false",], },);
		expect(registry.dataset["refresh-schema"].idempotency,).toBe("convergent",);
		expect(registry.notebook["clear-jupyter-outputs"].idempotency,).toBe("convergent",);
		for (const action of ["create", "set-settings", "train",]) {
			expect(registry["ml-task"][action].optionalFlags, `ml-task ${action} dry-run`,).not.toContain(
				"dry-run",
			);
		}
		for (const action of ["deploy", "delete",]) {
			expect(registry["ml-task"][action].optionalFlags, `ml-task ${action} dry-run`,).toContain(
				"dry-run",
			);
		}
		for (const flagName of ["prediction-type", "backend-type", "guess-policy",]) {
			expect(
				registry["ml-task"].create.flags.find((flag,) => flag.name === flagName)?.enumValues,
				`ml-task create --${flagName} enum lock`,
			).toBeUndefined();
		}
	});

	it("does not advertise removed help or report-json flags", async () => {
		const registry = await exportedCommandRegistry();

		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const [action, meta,] of Object.entries(actions,)) {
				const flagNames = meta.flags.map((flag,) => flag.name);
				expect(flagNames, `${resource} ${action} flags`,).not.toContain("help",);
				expect(flagNames, `${resource} ${action} flags`,).not.toContain("report-json",);
				expect(flagNames, `${resource} ${action} flags`,).not.toContain("json",);
				expect(flagNames, `${resource} ${action} flags`,).not.toContain("raw",);
			}
		}
	});

	it("advertises every long flag referenced in each command usage", async () => {
		const registry = await exportedCommandRegistry();
		// Parser aliases are accepted but normalized to a canonical name in the registry.
		const aliasFlags = new Set([
			"project",
			"dryrun",
			"skip-tls-verify",
			"extra-ca-certs",
			"explain",
			"zone-name",
			"rows",
		],);
		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const [action, meta,] of Object.entries(actions,)) {
				const advertised = new Set(meta.flags.map((flag,) => flag.name),);
				for (const match of meta.usage.matchAll(/--([a-z0-9-]+)/g,)) {
					const flag = match[1]!;
					if (aliasFlags.has(flag,)) continue;
					expect(
						advertised.has(flag,),
						`${resource} ${action} usage flag --${flag} must be advertised`,
					).toBe(true,);
				}
			}
		}
	});

	it("classifies compound-name mutations and getters by side effect", async () => {
		const registry = await scopedCommandRegistry([
			"api-deployer",
			"continuous-activity",
			"flow-zone",
			"metrics",
			"project",
			"project-deployer",
		],);
		const writes = [
			["project", "permissions-set",],
			["continuous-activity", "start",],
			["metrics", "dataset-compute",],
			["flow-zone", "organize",],
		] as const;
		for (const [resource, action,] of writes) {
			expect(registry[resource]?.[action]?.sideEffect, `${resource} ${action} sideEffect`,).toBe(
				"write",
			);
			expect(registry[resource]?.[action]?.mutatesDss, `${resource} ${action} mutatesDss`,).toBe(
				true,
			);
		}
		const reads = [
			["project", "settings-get",],
			["api-deployer", "deployment-status",],
			["api-deployer", "deployment-settings",],
			["project-deployer", "deployment-status",],
		] as const;
		for (const [resource, action,] of reads) {
			expect(registry[resource]?.[action]?.sideEffect, `${resource} ${action} sideEffect`,).toBe(
				"read",
			);
			expect(registry[resource]?.[action]?.mutatesDss, `${resource} ${action} mutatesDss`,).toBe(
				false,
			);
		}
	});

	it("reports array output shape for list-style and timeline reads", async () => {
		const registry = await exportedCommandRegistry();
		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const [action, meta,] of Object.entries(actions,)) {
				if (/^list(-|$)/.test(action,)) {
					expect(meta.outputShape, `${resource} ${action} outputShape`,).toBe("array",);
				}
			}
		}
		expect(registry["data-quality"]?.["project-timeline"]?.outputShape,).toBe("array",);
		expect(registry["project-library"]?.get?.outputShape,).toBe("string",);
	});
});
