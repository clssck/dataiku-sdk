import { describe, expect, it, } from "bun:test";
import { execFile, } from "node:child_process";
import { dirname, join, resolve, } from "node:path";
import { fileURLToPath, } from "node:url";
import { promisify, } from "node:util";

const exec = promisify(execFile,);
const SDK_ROOT = resolve(dirname(fileURLToPath(import.meta.url,),), "..",);
const CLI_PATH = join(SDK_ROOT, "src/cli.ts",);
const BUN = process.execPath;

type CommandRegistryEntry = {
	resource: string;
	action: string;
	usage: string;
	description?: string;
	examples?: string[];
	flags: Array<{ name: string; kind: "boolean" | "value"; }>;
	positionals: string[];
	sideEffect: "read" | "write" | "auth";
	outputShape: "object" | "array" | "string" | "void";
	inputContract: { stdin?: boolean; dataFlag?: boolean; dataFileFlag?: boolean; };
	destructive: "none" | "reversible" | "destructive";
	producesLocalFile: boolean;
	mutatesDss: boolean;
	async: "none" | "job" | "future";
	idempotency: "safe" | "if-not-exists" | "if-exists" | "none";
	dryRun: boolean;
	requiredFlags: string[];
	optionalFlags: string[];
	payloadSchema?: {
		stdin?: boolean;
		dataFlag?: boolean;
		dataFileFlag?: boolean;
		jsonShape?: "object" | "array";
	};
	examplePayload?: unknown;
	cleanupCommand?: string;
	exitCodes: { ok: 0; usage: 1; error: 2; transient: 3; longRunningFailure?: 4; };
	cleanupHint?: string;
	requiresAuth: boolean;
	requiresProject: boolean;
};

type CommandRegistry = Record<string, Record<string, CommandRegistryEntry>>;

const EXPECTED_COMMANDS: Record<string, string[]> = {
	project: ["list", "get", "metadata", "flow", "map",],
	"flow-zone": ["list", "find", "get", "create", "update", "delete", "move", "organize", "graph",],
	dashboard: ["list", "get", "create", "update", "delete",],
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
	auth: ["login", "status", "logout",],
	doctor: ["run",],
	commands: ["run",],
	"install-skill": ["run",],
	cleanup: ["run",],
	fixtures: ["run",],
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
	},);
}

function expectedCleanupCommand(deleteUsage: string,): string {
	const base = deleteUsage.replace(/\[[^\]]*\]/g, " ",).replace(/\s+/g, " ",).trim();
	if (deleteUsage.includes("--if-exists",)) return `${base} --if-exists`;
	return base;
}

describe("CLI command surface", () => {
	it("exposes every supported resource/action in the machine-readable registry", async () => {
		const { stdout, } = await dss(["commands",],);
		const registry = JSON.parse(stdout,) as CommandRegistry;

		for (const [resource, actions,] of Object.entries(EXPECTED_COMMANDS,)) {
			expect(Object.keys(registry[resource] ?? {},).sort(),).toEqual(actions.slice().sort(),);
			for (const action of actions) {
				const meta = registry[resource]?.[action];
				const usageOmitsRunAction = action === "run"
					&& ["commands", "doctor", "install-skill", "cleanup", "fixtures",].includes(resource,);
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
					["safe", "if-not-exists", "if-exists", "none",],
					`${resource} ${action} idempotency`,
				).toContain(meta?.idempotency,);
				expect(typeof meta?.dryRun, `${resource} ${action} dryRun`,).toBe("boolean",);
				expect(Array.isArray(meta?.requiredFlags,), `${resource} ${action} requiredFlags`,).toBe(true,);
				expect(Array.isArray(meta?.optionalFlags,), `${resource} ${action} optionalFlags`,).toBe(true,);
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
				expect(meta?.exitCodes, `${resource} ${action} exitCodes`,).toEqual({
					ok: 0,
					usage: 1,
					error: 2,
					transient: 3,
					...(meta?.async !== "none" ? { longRunningFailure: 4, } : {}),
				},);
			}
		}

		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const [action, meta,] of Object.entries(actions,)) {
				if (!action.startsWith("create",)) continue;
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
		expect(registry.dataset.delete.flags,).toContainEqual({ name: "dry-run", kind: "boolean", },);
		expect(registry.dataset.delete.dryRun,).toBe(true,);
		expect(registry.dataset.create.flags,).toContainEqual({ name: "dry-run", kind: "boolean", },);
		expect(registry.dataset.create.flags,).toContainEqual({
			name: "if-not-exists",
			kind: "boolean",
		},);
		expect(registry.dataset.create.requiredFlags,).toEqual(["name", "connection", "type",],);
		expect(registry.dataset.create.optionalFlags,).toContain("dry-run",);
		expect(registry.dataset.create.cleanupCommand,).toBe("dss dataset delete <name> --if-exists",);
		expect(registry.dataset.create.idempotency,).toBe("if-not-exists",);
		expect(registry.dataset.update.payloadSchema,).toEqual({
			stdin: true,
			dataFlag: true,
			dataFileFlag: true,
			jsonShape: "object",
		},);
		expect(registry.dataset.update.examplePayload,).toEqual({ tags: ["production",], },);
		expect(registry.project.list.sideEffect,).toBe("read",);
		expect(registry.project.list.requiresProject,).toBe(false,);
		expect(registry.wiki.settings.sideEffect,).toBe("read",);
		expect(registry.wiki.settings.requiresProject,).toBe(true,);
		expect(registry.wiki.create.flags,).toContainEqual({ name: "dry-run", kind: "boolean", },);
		expect(registry.dashboard.create.flags,).toContainEqual({ name: "dry-run", kind: "boolean", },);
		expect(registry.dashboard.create.cleanupCommand,).toBe("dss dashboard delete <id> --if-exists",);
		expect(registry.insight.create.flags,).toContainEqual({ name: "dry-run", kind: "boolean", },);
		expect(registry.insight.create.cleanupCommand,).toBe("dss insight delete <id> --if-exists",);
		expect(registry["data-quality"].rules.sideEffect,).toBe("read",);
		expect(registry["data-quality"].status.sideEffect,).toBe("read",);
		expect(registry["data-quality"]["create-rule"].sideEffect,).toBe("write",);
		expect(registry["data-quality"]["create-rule"].cleanupCommand,).toBe(
			"dss data-quality delete-rule <dataset> <rule-id> --if-exists",
		);
		expect(registry["data-quality"].compute.sideEffect,).toBe("write",);
		expect(registry.job.build.flags,).toContainEqual({ name: "wait", kind: "boolean", },);
		expect(registry.job.build.async,).toBe("job",);
		expect(registry.job.build.exitCodes.longRunningFailure,).toBe(4,);
		expect(registry.job.build.destructive,).toBe("reversible",);
		expect(registry["data-quality"]["project-status"].flags,).toContainEqual({
			name: "only-monitored",
			kind: "value",
		},);
		expect(registry["data-quality"].compute.flags,).toContainEqual({
			name: "wait",
			kind: "boolean",
		},);
		expect(registry.future.wait.sideEffect,).toBe("read",);
		expect(registry.future.wait.requiresProject,).toBe(false,);
		expect(registry.future.abort.sideEffect,).toBe("write",);
		expect(registry["data-quality"]["create-rule"].flags,).toContainEqual({
			name: "dry-run",
			kind: "boolean",
		},);
		expect(registry["code-env"]["update-packages"].flags,).toContainEqual({
			name: "force-rebuild",
			kind: "boolean",
		},);
		expect(registry.auth.login.sideEffect,).toBe("auth",);
		expect(registry.auth.login.requiresAuth,).toBe(false,);
		expect(registry.doctor.run.sideEffect,).toBe("read",);
		expect(registry.doctor.run.requiresAuth,).toBe(true,);
		expect(registry.commands.run.sideEffect,).toBe("read",);
		expect(registry.commands.run.requiresAuth,).toBe(false,);
		expect(registry["install-skill"].run.sideEffect,).toBe("write",);
		expect(registry["install-skill"].run.requiresAuth,).toBe(false,);
		expect(registry["install-skill"].run.flags,).toContainEqual({
			name: "dry-run",
			kind: "boolean",
		},);
		expect(registry.recipe["set-payload"].inputContract.dataFlag,).toBeUndefined();
		expect(registry.recipe["set-payload"].destructive,).toBe("reversible",);
		expect(registry.recipe["set-payload"].flags,).toContainEqual({
			name: "backup-dir",
			kind: "value",
		},);
		expect(registry.recipe["set-payload"].flags,).toContainEqual({
			name: "no-backup",
			kind: "boolean",
		},);
		expect(registry.recipe["get-payload"].flags,).toContainEqual({
			name: "raw",
			kind: "boolean",
		},);
		expect(registry.recipe.cat.outputShape,).toBe("string",);
		expect(registry.dataset.clone.cleanupCommand,).toBe("dss dataset delete <name> --if-exists",);
		expect(registry.recipe.clone.cleanupCommand,).toBe("dss recipe delete <name> --if-exists",);
		expect(registry.dataset.clone.flags,).toContainEqual({
			name: "allow-same-path",
			kind: "boolean",
		},);
		expect(registry.job.summary.sideEffect,).toBe("read",);
		expect(registry.job.watch.async,).toBe("job",);
		expect(registry.recipe.run.flags,).toContainEqual({ name: "max-log-lines", kind: "value", },);
		expect(registry.recipe.run.flags,).toContainEqual({ name: "dry-run", kind: "boolean", },);
		expect(registry.recipe.run.async,).toBe("job",);
		expect(registry.sql.query.flags,).toContainEqual({ name: "output-file", kind: "value", },);
		expect(registry.sql.query.flags,).toContainEqual({ name: "output", kind: "value", },);
		expect(registry.sql.query.producesLocalFile,).toBe(true,);
		expect(registry.connection.schemas.outputShape,).toBe("array",);
		expect(registry.connection.tables.flags,).toContainEqual({ name: "schema", kind: "value", },);
		expect(registry.dataset["refresh-schema"].sideEffect,).toBe("write",);
		expect(registry.dataset["refresh-schema"].flags,).toContainEqual({
			name: "data",
			kind: "value",
		},);
	});

	it("prints action help for every registered command without resolving credentials", async () => {
		const { stdout, } = await dss(["commands",],);
		const registry = JSON.parse(stdout,) as CommandRegistry;

		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const action of Object.keys(actions,)) {
				const { stderr, } = await dss([resource, action, "--help",],);
				expect(stderr, `${resource} ${action} help`,).toContain("Usage:",);
			}
		}
	}, 90_000,);
});
