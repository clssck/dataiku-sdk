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
	cleanupCommand?: string;
	exitCodes: { ok: 0; usage: 1; error: 2; transient: 3; longRunningFailure?: 4; };
	cleanupHint?: string;
	requiresAuth: boolean;
	requiresProject: boolean;
};

type CommandRegistry = Record<string, Record<string, CommandRegistryEntry>>;

const EXPECTED_COMMANDS: Record<string, string[]> = {
	code: ["run",],
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
	version: ["run",],
	"install-skill": ["run",],
	cleanup: ["run",],
	fixtures: ["run",],
	batch: ["run",],
	app: [
		"list",
		"manifest",
		"instances",
		"create-instance",
		"instance-manifest",
		"save-instance-manifest",
		"delete-instance",
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
	"api-service": [
		"list",
		"create",
		"get-settings",
		"save-settings",
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
		const { stdout, } = await dss(["commands", "run",],);
		const registry = JSON.parse(stdout,) as CommandRegistry;
		expect(Object.keys(registry,).sort(),).toEqual(Object.keys(EXPECTED_COMMANDS,).sort(),);

		for (const [resource, actions,] of Object.entries(EXPECTED_COMMANDS,)) {
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
		expect(registry.recipe["get-payload"].flags,).toContainEqual(
			expect.objectContaining({ name: "raw", kind: "boolean", },),
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
	});

	it("does not advertise removed help or report-json flags", async () => {
		const { stdout, } = await dss(["commands", "run",],);
		const registry = JSON.parse(stdout,) as CommandRegistry;

		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const [action, meta,] of Object.entries(actions,)) {
				const flagNames = meta.flags.map((flag,) => flag.name);
				expect(flagNames, `${resource} ${action} flags`,).not.toContain("help",);
				expect(flagNames, `${resource} ${action} flags`,).not.toContain("report-json",);
			}
		}
	});
});
