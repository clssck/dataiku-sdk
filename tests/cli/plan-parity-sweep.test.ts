import { expect, it, } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { dirname, extname, isAbsolute, join, } from "node:path";
import { buildCommandRegistry, } from "../../src/cli/contract.js";
import { stableJson, } from "../../src/utils/stable-hash.js";
import { CLI_PATH, } from "./_harness.js";

/**
 * Every documented example of every write command: the `--plan` request must
 * equal the first request with the planned method that the command sends.
 * Each example ends in exactly one outcome. `compared` examples proved the
 * plan against the wire; `skipped` ones are inconclusive and say why; a
 * mismatch, a timeout, or a plan that contacted DSS fails the test. Because
 * skips pass silently, the test also pins a floor for conclusive comparisons
 * and a ceiling per skip reason: turning comparisons into skips fails.
 * Plans marked `exact: false` declare that apply depends on live DSS state;
 * that marker, not this file, is where such exceptions live.
 */

/** Deliberately summarized (redacted) payloads. */
const SUMMARIZED_BY_DESIGN =
	/<omitted|\[redacted\]|contentSource|configKeys|apiKeyProvided|"upload":"multipart"|"contentType":"multipart\/form-data"/;
const SKIP_RESOURCES: Record<string, true> = {
	auth: true,
	batch: true,
	cleanup: true,
	"install-skill": true,
};

/** Flags whose value names a local input file the example expects to exist. */
const INPUT_FILE_FLAG = /^--(?:[a-z-]*-file|file)(?:=|$)/;
const KILL_AFTER_MS = 10_000;

type Sent = { method: string; path: string; body: unknown; };
type SkipReason = "plan-nonzero" | "plan-inexact" | "plan-no-request" | "no-matching-request";
type Outcome =
	| { kind: "compared"; payload: boolean; }
	| { kind: "skipped"; reason: SkipReason; }
	| { kind: "failed"; detail: string; };

async function runCli(
	args: string[],
	home: string,
): Promise<{ code: number | null; stdout: string; timedOut: boolean; }> {
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
	let timedOut = false;
	const timer = setTimeout(() => {
		timedOut = true;
		child.kill();
	}, KILL_AFTER_MS,);
	const stdout = await new Response(child.stdout,).text();
	await child.exited;
	clearTimeout(timer,);
	return { code: child.exitCode, stdout, timedOut, };
}

/** Shell-like split: quotes group, may sit inside a token (`--flag='{"a":1}'`), and are removed. */
function exampleArgv(example: string,): string[] | undefined {
	if (!example.startsWith("dss ",) || /[<>|$]/.test(example,) || example.includes("--dry-run",)) {
		return undefined;
	}
	return example.slice(4,).match(/(?:[^\s'"]|'[^']*'|"[^"]*")+/g,)?.map((token,) =>
		token.replace(
			/'([^']*)'|"([^"]*)"/g,
			(_match, single: string | undefined, double: string | undefined,) => single ?? double ?? "",
		),
	);
}

function fixtureContent(file: string,): string {
	switch (extname(file,)) {
		case ".json":
			return "{}";
		case ".py":
			return 'print("sweep")\n';
		case ".sql":
			return "SELECT 1\n";
		default:
			return "sweep\n";
	}
}

/** Create, under `home`, every relative input file the example names, so it is not inconclusive for want of a file. */
function provideInputFiles(argv: string[], home: string,): void {
	argv.forEach((token, index,) => {
		if (!INPUT_FILE_FLAG.test(token,)) return;
		const value = token.includes("=",) ? token.slice(token.indexOf("=",) + 1,) : argv[index + 1];
		if (value === undefined || isAbsolute(value,) || value.split("/",).includes("..",)) return;
		const target = join(home, value,);
		mkdirSync(dirname(target,), { recursive: true, },);
		writeFileSync(target, fixtureContent(value,),);
	},);
}

async function probe(argv: string[], home: string,): Promise<Outcome> {
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
		if (planned.timedOut) return { kind: "failed", detail: "plan timed out", };
		if (sent.length > 0) return { kind: "failed", detail: "plan contacted DSS", };
		if (planned.code !== 0) return { kind: "skipped", reason: "plan-nonzero", };
		const plan = JSON.parse(planned.stdout,) as {
			exact?: boolean;
			method?: string;
			endpoint?: string;
			payload?: unknown;
		};
		if (plan.exact === false) return { kind: "skipped", reason: "plan-inexact", };
		if (!plan.method || !plan.endpoint) return { kind: "skipped", reason: "plan-no-request", };
		const applied = await runCli(base, home,);
		const request = sent.find((entry,) => entry.method === plan.method,);
		if (!request) {
			return applied.timedOut
				? { kind: "failed", detail: "apply timed out before sending the planned request", }
				: { kind: "skipped", reason: "no-matching-request", };
		}
		if (request.path !== plan.endpoint) {
			return { kind: "failed", detail: `endpoint ${plan.endpoint} != ${request.path}`, };
		}
		if (plan.payload === undefined || SUMMARIZED_BY_DESIGN.test(JSON.stringify(plan.payload,),)) {
			return { kind: "compared", payload: false, };
		}
		const plannedPayload = stableJson(plan.payload,);
		const actual = stableJson(request.body,);
		return plannedPayload === actual
			? { kind: "compared", payload: true, }
			: { kind: "failed", detail: `payload ${plannedPayload} != ${actual}`, };
	} finally {
		await server.stop(true,);
	}
}

/**
 * Measured on the documented examples: jobs 317, compared 223 (with payload
 * 134), skipped 94 (plan-nonzero 1, plan-inexact 54, plan-no-request 0,
 * no-matching-request 39). Floors sit just under the measured counts and
 * ceilings just over, so an example moving from compared to skipped fails.
 */
const MIN_JOBS = 310;
const MIN_COMPARED = 218;
const MIN_COMPARED_WITH_PAYLOAD = 130;
const MAX_SKIPPED: Record<SkipReason, number> = {
	"plan-nonzero": 2,
	"plan-inexact": 56,
	"plan-no-request": 1,
	"no-matching-request": 41,
};

it("--plan matches the sent request for every documented write example", async () => {
	const home = mkdtempSync(join(tmpdir(), "dss-plan-sweep-",),);
	try {
		const registry = buildCommandRegistry();
		const jobs: Array<{ argv: string[]; }> = [];
		for (const [resource, actions,] of Object.entries(registry,)) {
			if (SKIP_RESOURCES[resource] === true) continue;
			for (const entry of Object.values(actions,)) {
				if (entry.sideEffect !== "write") continue;
				for (const example of entry.examples ?? []) {
					const argv = exampleArgv(example,);
					if (!argv) continue;
					provideInputFiles(argv, home,);
					jobs.push({ argv, },);
				}
			}
		}
		const failures: string[] = [];
		const skipped: Record<SkipReason, number> = {
			"plan-nonzero": 0,
			"plan-inexact": 0,
			"plan-no-request": 0,
			"no-matching-request": 0,
		};
		let compared = 0;
		let comparedWithPayload = 0;
		let next = 0;
		await Promise.all(Array.from({ length: 8, }, async () => {
			while (next < jobs.length) {
				const job = jobs[next++];
				const outcome = await probe(job.argv, home,);
				if (outcome.kind === "failed") {
					failures.push(`${job.argv.join(" ",)}: ${outcome.detail}`,);
				} else if (outcome.kind === "skipped") {
					skipped[outcome.reason]++;
				} else {
					compared++;
					if (outcome.payload) comparedWithPayload++;
				}
			}
		},),);
		const tally =
			`jobs ${jobs.length}, compared ${compared} (with payload ${comparedWithPayload}), skipped ${
				JSON.stringify(skipped,)
			}`;
		const floors: string[] = [];
		if (jobs.length < MIN_JOBS) floors.push(`jobs ${jobs.length} < ${MIN_JOBS}`,);
		if (compared < MIN_COMPARED) floors.push(`compared ${compared} < ${MIN_COMPARED}`,);
		if (comparedWithPayload < MIN_COMPARED_WITH_PAYLOAD) {
			floors.push(`compared with payload ${comparedWithPayload} < ${MIN_COMPARED_WITH_PAYLOAD}`,);
		}
		for (const [reason, count,] of Object.entries(skipped,) as Array<[SkipReason, number,]>) {
			if (count > MAX_SKIPPED[reason]) {
				floors.push(`skipped ${reason} ${count} > ${MAX_SKIPPED[reason]}`,);
			}
		}
		expect([...failures.sort(), ...floors,], tally,).toEqual([],);
	} finally {
		rmSync(home, { recursive: true, force: true, },);
	}
}, 180_000,);
