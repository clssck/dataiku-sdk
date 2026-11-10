import { describe, expect, it, } from "bun:test";
import { stableJson, } from "../../src/utils/stable-hash.js";
import { cliEnv, dss, dssFailure, readBody, sendJson, withCliServer, } from "./_harness.js";

/**
 * `--plan` is a separate implementation of each mutating request, so it can
 * drift from what the command really sends. For commands whose wire body is
 * computable offline, the planned method/endpoint/payload must equal the
 * request the command then issues.
 */
const CASES: string[][] = [
	["scenario", "create", "my_scenario", "My Scenario",],
	["scenario", "create", "my_scenario", "My Scenario", "--data", '{"markedAsTest":true}',],
	["scenario", "run", "my_scenario", "--params", '{"region":"EU"}',],
	["flow-zone", "create", "--name", "Exports",],
	[
		"saved-model",
		"evaluate-version",
		"M",
		"V",
		"--dataset",
		"ds",
		"--skip-expensive-reports",
		"false",
	],
	["plugin", "install-from-git", "--repository", "https://example.com/p.git", "--checkout", "main",],
	["plugin", "update-from-git", "my-plugin", "--repository", "https://example.com/p.git",],
	[
		"plugin",
		"create-dev",
		"my-plugin",
		"--creation-mode",
		"GIT_CLONE",
		"--repository",
		"https://example.com/p.git",
	],
	["plugin", "rename", "my-plugin", "python/old.py", "new.py",],
	["plugin", "move", "my-plugin", "python/old.py", "python/lib",],
	["bundle", "export", "v1",],
	["wiki", "create", "--name", "Notes",],
	["dataset", "create", "--name", "orders", "--type", "Filesystem", "--connection", "fs",],
	[
		"dataset",
		"create",
		"--name",
		"sales",
		"--type",
		"Snowflake",
		"--connection",
		"sf",
		"--table",
		"SALES",
		"--schema",
		"PUBLIC",
		"--catalog",
		"DB",
	],
	[
		"dataset",
		"create",
		"--name",
		"open_orders",
		"--type",
		"PostgreSQL",
		"--connection",
		"pg",
		"--query",
		"SELECT id FROM orders",
	],
	["dataset", "create", "--name", "uploads", "--type", "UploadedFiles",],
	["folder", "create", "--name", "exports", "--type", "S3", "--connection", "s3",],
	["scenario", "active-set", "my_scenario", "false",],
	[
		"messaging-channel",
		"create",
		"--type",
		"shell",
		"--id",
		"ops",
		"--data",
		'{"type":"COMMAND","command":"echo"}',
	],
	["messaging-channel", "permissions-set", "ops", "--data", '[{"group":"g","canUse":true}]',],
	["messaging-channel", "delete", "ops",],
	["project-deployer", "run-test-scenarios", "dep1", "--automation-node-id", "n1",],
];

describe("--plan matches the request the command sends", () => {
	for (const argv of CASES) {
		it(argv.join(" ",), async () => {
			const sent: { method: string; path: string; body: unknown; }[] = [];
			await withCliServer(async (req, res,) => {
				const text = await readBody(req,);
				sent.push({
					method: req.method ?? "",
					path: req.url ?? "",
					body: text ? JSON.parse(text,) : undefined,
				},);
				sendJson(res, {},);
			}, async (url,) => {
				const env = cliEnv(url,);
				const plan = JSON.parse((await dss([...argv, "--plan",], { env, },)).stdout,) as {
					method: string;
					endpoint: string;
					payload?: unknown;
				};
				expect(sent,).toEqual([],);
				await dss(argv, { env, },).catch(() => undefined,);
				const request = sent.find((entry,) => entry.method === plan.method,);
				expect(request?.path,).toBe(plan.endpoint,);
				expect(stableJson(request?.body ?? null,),).toBe(stableJson(plan.payload ?? null,),);
			},);
		},);
	}
});

it("plugin create-dev --plan refuses a repository URL with embedded credentials", async () => {
	const failure = await dssFailure([
		"plugin",
		"create-dev",
		"p",
		"--creation-mode",
		"GIT_CLONE",
		"--repository",
		"https://user:s3cret-token@example.com/p.git",
		"--plan",
	], { env: { ...cliEnv("http://127.0.0.1:1",), }, },);
	expect(failure.stdout,).not.toContain("s3cret-token",);
	expect(JSON.parse(failure.stdout,),).toMatchObject({ exitCode: 1, },);
});

describe("--plan applies the registry's required-flag contract", () => {
	it("rejects a plan that omits a required flag, as apply would", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			sendJson(res, {},);
		}, async (url,) => {
			const env = cliEnv(url,);
			const failure = await dssFailure([
				"project",
				"create",
				"TEMP",
				"TempProject",
				"--plan",
			], { env, },);
			expect(failure.code,).toBe(1,);
			expect(JSON.parse(failure.stdout,),).toMatchObject({
				code: "missing_required_flag",
				exitCode: 1,
				error: expect.stringContaining("--owner is required",),
			},);

			const ok = await dss([
				"project",
				"create",
				"TEMP",
				"TempProject",
				"--owner",
				"alice",
				"--plan",
			], { env, },);
			expect(JSON.parse(ok.stdout,),).toMatchObject({ payload: { owner: "alice", }, },);
		},);
		expect(requests,).toEqual([],);
	});

	it("rejects a plan that satisfies none of a required choice", async () => {
		const failure = await dssFailure([
			"project",
			"settings-set",
			"--project-key",
			"P",
			"--plan",
		], { env: cliEnv("http://127.0.0.1:1",), },);
		expect(failure.code,).toBe(1,);
		expect(JSON.parse(failure.stdout,),).toMatchObject({ code: "missing_required_flag", },);
	});

	it("rejects an empty --name or --content-type before any request, in every mode", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			sendJson(res, { id: "A", name: "old", article: { id: "A", name: "old", }, payload: "x", },);
		}, async (url,) => {
			const env = cliEnv(url,);
			for (
				const argv of [
					["wiki", "update", "A", "--name", "",],
					["wiki", "update", "A", "--name", "", "--content", "# kept",],
					["dashboard", "update", "A", "--name", "",],
					["insight", "update", "A", "--name", "",],
					["insight", "update", "A", "--content-type", "",],
				]
			) {
				for (const mode of [[], ["--plan",], ["--dry-run",],]) {
					const failure = await dssFailure([...argv, ...mode,], { env, },);
					expect(JSON.parse(failure.stdout,),).toMatchObject({
						code: "invalid_flag_value",
						exitCode: 1,
					},);
				}
			}
		},);
		expect(requests,).toEqual([],);
	});

	it("requires an explicit content source instead of reading stdin by default", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			sendJson(res, {},);
		}, async (url,) => {
			const env = cliEnv(url,);
			for (
				const argv of [
					["project-library", "put", "python/a.py",],
					["project-library", "put", "python/a.py", "--plan",],
					["project-library", "put", "python/a.py", "--dry-run",],
					["project-library", "diff", "python/a.py",],
					["plugin", "contents-put", "my-plugin", "python/a.py",],
					["plugin", "contents-put", "my-plugin", "python/a.py", "--plan",],
					["plugin", "contents-put", "my-plugin", "python/a.py", "--dry-run",],
				]
			) {
				const failure = await dssFailure(argv, { env, },);
				expect(JSON.parse(failure.stdout,),).toMatchObject({
					code: "missing_required_flag",
					exitCode: 1,
				},);
			}
		},);
		expect(requests,).toEqual([],);
	});
});

describe("--plan reports whether the command waits", () => {
	it("run-and-wait always waits; run waits only with --wait", async () => {
		const waits: Record<string, unknown> = {};
		for (
			const [name, argv,] of [
				["run-and-wait", ["macro", "run-and-wait", "m",],],
				["run", ["macro", "run", "m",],],
				["run --wait", ["macro", "run", "m", "--wait",],],
			] as const
		) {
			const { stdout, } = await dss([...argv, "--plan", "--project-key", "P",], {
				env: cliEnv("http://127.0.0.1:1",),
			},);
			const plan = JSON.parse(stdout,) as { wait: unknown; };
			waits[name] = plan.wait;
		}
		expect(waits,).toEqual({ "run-and-wait": true, run: false, "run --wait": true, },);
	});
});
