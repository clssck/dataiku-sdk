import { describe, expect, it, } from "bun:test";
import { execFile, } from "node:child_process";
import { dirname, join, resolve, } from "node:path";
import { fileURLToPath, } from "node:url";
import { promisify, } from "node:util";

const exec = promisify(execFile,);
const SDK_ROOT = resolve(dirname(fileURLToPath(import.meta.url,),), "..",);
const CLI_PATH = join(SDK_ROOT, "src/cli.ts",);
const BUN = process.execPath;

type CommandRegistry = Record<
	string,
	Record<string, { usage: string; description?: string; examples?: string[]; }>
>;

const EXPECTED_COMMANDS: Record<string, string[]> = {
	project: ["list", "get", "metadata", "flow", "map",],
	"flow-zone": ["list", "get", "create", "update", "delete", "move", "graph",],
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
	"code-env": ["list", "get",],
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
	auth: ["login", "status", "logout",],
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
				expect(meta?.usage, `${resource} ${action} usage`,).toContain(`dss ${resource} ${action}`,);
				expect(meta?.description?.length ?? 0, `${resource} ${action} description`,).toBeGreaterThan(
					0,
				);
			}
		}
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
