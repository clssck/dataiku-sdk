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
	requiresAuth: boolean;
	requiresProject: boolean;
};

type CommandRegistry = Record<string, Record<string, CommandRegistryEntry>>;

const EXPECTED_COMMANDS: Record<string, string[]> = {
	project: ["list", "get", "metadata", "flow", "map",],
	"flow-zone": ["list", "get", "create", "update", "delete", "move", "graph",],
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
		"preview",
		"metadata",
		"download",
		"create",
		"delete",
		"update",
	],
	recipe: [
		"list",
		"get",
		"delete",
		"download",
		"download-code",
		"create",
		"diff",
		"update",
		"get-payload",
		"set-payload",
	],
	job: ["list", "get", "log", "build", "build-and-wait", "wait", "abort",],
	scenario: ["list", "get", "run", "run-and-wait", "status", "delete", "create", "update",],
	folder: ["list", "create", "get", "contents", "download", "upload", "delete-file",],
	variable: ["get", "set",],
	connection: ["list", "infer",],
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

describe("CLI command surface", () => {
	it("exposes every supported resource/action in the machine-readable registry", async () => {
		const { stdout, } = await dss(["commands",],);
		const registry = JSON.parse(stdout,) as CommandRegistry;

		for (const [resource, actions,] of Object.entries(EXPECTED_COMMANDS,)) {
			expect(Object.keys(registry[resource] ?? {},).sort(),).toEqual(actions.slice().sort(),);
			for (const action of actions) {
				const meta = registry[resource]?.[action];
				const usageOmitsRunAction = action === "run"
					&& ["commands", "doctor", "install-skill",].includes(resource,);
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
			}
		}

		expect(registry.dataset.delete.sideEffect,).toBe("write",);
		expect(registry.dataset.delete.requiresProject,).toBe(true,);
		expect(registry.dataset.delete.flags,).toContainEqual({ name: "dry-run", kind: "boolean", },);
		expect(registry.project.list.sideEffect,).toBe("read",);
		expect(registry.project.list.requiresProject,).toBe(false,);
		expect(registry.wiki.settings.sideEffect,).toBe("read",);
		expect(registry.wiki.settings.requiresProject,).toBe(true,);
		expect(registry.wiki.create.flags,).toContainEqual({ name: "dry-run", kind: "boolean", },);
		expect(registry.dashboard.create.flags,).toContainEqual({ name: "dry-run", kind: "boolean", },);
		expect(registry.insight.create.flags,).toContainEqual({ name: "dry-run", kind: "boolean", },);
		expect(registry["data-quality"].rules.sideEffect,).toBe("read",);
		expect(registry["data-quality"].status.sideEffect,).toBe("read",);
		expect(registry["data-quality"]["create-rule"].sideEffect,).toBe("write",);
		expect(registry["data-quality"].compute.sideEffect,).toBe("write",);
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
	}, 30_000,);
});
