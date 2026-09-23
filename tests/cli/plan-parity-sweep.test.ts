import { expect, it, } from "bun:test";
import { mkdtempSync, rmSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { buildCommandRegistry, } from "../../src/cli/contract.js";
import { stableJson, } from "../../src/utils/stable-hash.js";
import { CLI_PATH, } from "./_harness.js";

/**
 * Every documented example of every write command: the `--plan` request must
 * equal the first request with the planned method that the command sends.
 * Examples whose command never reaches that request against an empty mock
 * (preflight GETs fail) are inconclusive and skipped, not counted as passes.
 * Plans marked `exact: false` declare that apply depends on live DSS state and
 * are skipped; that marker, not this file, is where such exceptions live.
 */
/** Deliberately summarized (redacted) payloads. */
const SUMMARIZED_BY_DESIGN =
	/<omitted|contentSource|configKeys|apiKeyProvided|"upload":"multipart"/;
const SKIP_RESOURCES: Record<string, true> = {
	auth: true,
	batch: true,
	cleanup: true,
	"install-skill": true,
};

type Sent = { method: string; path: string; body: unknown; };

async function runCli(
	args: string[],
	home: string,
): Promise<{ code: number | null; stdout: string; }> {
	const child = Bun.spawn([process.execPath, "--no-env-file", CLI_PATH, ...args,], {
		cwd: home,
		env: {
			PATH: process.env.PATH ?? "",
			HOME: home,
			XDG_CONFIG_HOME: home,
			DATAIKU_DISABLE_ENV: "1",
		},
		stdout: "pipe",
		stderr: "ignore",
	},);
	const timer = setTimeout(() => child.kill(), 5_000,);
	const stdout = await new Response(child.stdout,).text();
	await child.exited;
	clearTimeout(timer,);
	return { code: child.exitCode, stdout, };
}

function exampleArgv(example: string,): string[] | undefined {
	if (!example.startsWith("dss ",) || /[<>|$]/.test(example,) || example.includes("--dry-run",)) {
		return undefined;
	}
	return example.slice(4,).match(/'[^']*'|"[^"]*"|\S+/g,)?.map((token,) =>
		token.replace(/^(['"])(.*)\1$/, "$2",)
	);
}

async function probe(argv: string[], home: string,): Promise<string | undefined> {
	const sent: Sent[] = [];
	const server = Bun.serve({
		port: 0,
		async fetch(req,) {
			const url = new URL(req.url,);
			const text = await req.text();
			let body: unknown = text || null;
			try {
				body = text ? JSON.parse(text,) : null;
			} catch {
				// multipart or raw text bodies stay as text
			}
			sent.push({ method: req.method, path: url.pathname + url.search, body, },);
			return Response.json({},);
		},
	},);
	try {
		const base = [
			...argv,
			"--url",
			`http://127.0.0.1:${server.port}`,
			"--api-key",
			"k",
			...(argv.includes("--project-key",) ? [] : ["--project-key", "P",]),
		];
		const planned = await runCli([...base, "--plan",], home,);
		if (planned.code !== 0) return undefined;
		const plan = JSON.parse(planned.stdout,) as {
			exact?: boolean;
			method?: string;
			endpoint?: string;
			payload?: unknown;
		};
		if (plan.exact === false && sent.length === 0) return undefined;
		if (!plan.method || !plan.endpoint || sent.length > 0) {
			return sent.length > 0
				? "plan contacted DSS"
				: undefined;
		}
		await runCli(base, home,);
		const request = sent.find((entry,) => entry.method === plan.method);
		if (!request) return undefined;
		if (request.path !== plan.endpoint) return `endpoint ${plan.endpoint} != ${request.path}`;
		if (plan.payload === undefined || SUMMARIZED_BY_DESIGN.test(JSON.stringify(plan.payload,),)) {
			return undefined;
		}
		const planned_ = stableJson(plan.payload,);
		const actual = stableJson(request.body,);
		return planned_ === actual ? undefined : `payload ${planned_} != ${actual}`;
	} finally {
		server.stop(true,);
	}
}

it("--plan matches the sent request for every documented write example", async () => {
	const home = mkdtempSync(join(tmpdir(), "dss-plan-sweep-",),);
	try {
		const registry = buildCommandRegistry();
		const jobs: Array<{ key: string; argv: string[]; }> = [];
		for (const [resource, actions,] of Object.entries(registry,)) {
			if (SKIP_RESOURCES[resource] === true) continue;
			for (const [action, entry,] of Object.entries(actions,)) {
				const key = `${resource}.${action}`;
				if (entry.sideEffect !== "write") continue;
				for (const example of entry.examples ?? []) {
					const argv = exampleArgv(example,);
					if (argv) jobs.push({ key, argv, },);
				}
			}
		}
		const mismatches: string[] = [];
		let next = 0;
		await Promise.all(Array.from({ length: 8, }, async () => {
			while (next < jobs.length) {
				const job = jobs[next++]!;
				const problem = await probe(job.argv, home,);
				if (problem) mismatches.push(`${job.argv.join(" ",)}: ${problem}`,);
			}
		},),);
		expect(jobs.length,).toBeGreaterThan(100,);
		expect(mismatches.sort(),).toEqual([],);
	} finally {
		rmSync(home, { recursive: true, force: true, },);
	}
}, 180_000,);
