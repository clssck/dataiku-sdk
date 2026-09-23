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
	["dataset", "create", "--name", "uploads", "--type", "UploadedFiles",],
	["folder", "create", "--name", "exports", "--type", "S3", "--connection", "s3",],
	["scenario", "active-set", "my_scenario", "false",],
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
				await dss(argv, { env, },).catch(() => undefined);
				const request = sent.find((entry,) => entry.method === plan.method);
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
