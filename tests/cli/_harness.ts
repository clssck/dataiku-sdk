import { execFile, spawn, } from "node:child_process";
import { mkdirSync, readFileSync, realpathSync, rmSync, statSync, writeFileSync, } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { tmpdir, } from "node:os";
import { dirname, join, resolve, } from "node:path";
import { fileURLToPath, } from "node:url";
import { promisify, } from "node:util";

export {
	createServer,
	dirname,
	execFile,
	fileURLToPath,
	join,
	mkdirSync,
	promisify,
	readFileSync,
	realpathSync,
	resolve,
	rmSync,
	spawn,
	statSync,
	tmpdir,
	writeFileSync,
};

export type { AddressInfo, IncomingMessage, ServerResponse, };

export const exec = promisify(execFile,);
export const SDK_ROOT = resolve(import.meta.dir, "..", "..",);
export const CLI_PATH = join(SDK_ROOT, "src/cli.ts",);
export const BUN = process.execPath;

export type CliExecOptions = { cwd?: string; env?: NodeJS.ProcessEnv; };
export type CliFailure = { code: number | null; stdout: string; stderr: string; };

// Hermetic default: never let the checkout's .env supply live credentials.
const DEFAULT_CLI_ENV: NodeJS.ProcessEnv = { ...process.env, DATAIKU_DISABLE_ENV: "1", };
/** A CLI child that outlives this is killed so a hang fails with its partial output, not a bare test timeout. */
const CLI_SPAWN_TIMEOUT_MS = 25_000;

async function runCli(
	args: string[],
	opts: CliExecOptions,
	stdin: Blob | "ignore",
): Promise<{ stdout: string; stderr: string; }> {
	const child = Bun.spawn([BUN, "--no-env-file", "run", CLI_PATH, ...args,], {
		cwd: opts.cwd ?? SDK_ROOT,
		env: opts.env ?? DEFAULT_CLI_ENV,
		stdin,
		stdout: "pipe",
		stderr: "pipe",
		timeout: CLI_SPAWN_TIMEOUT_MS,
	},);
	const [stdout, stderr, code,] = await Promise.all([
		new Response(child.stdout,).text(),
		new Response(child.stderr,).text(),
		child.exited,
	],);
	if (code === 0 && child.signalCode === null) return { stdout, stderr, };
	const timedOut = child.signalCode !== null && child.killed;
	throw Object.assign(
		new Error(
			timedOut
				? `CLI killed after ${CLI_SPAWN_TIMEOUT_MS}ms: dss ${args.join(" ",)}\nstdout: ${
					stdout.slice(-2000,)
				}\nstderr: ${stderr.slice(-2000,)}`
				: `CLI exited with code ${String(code,)}`,
		),
		{ code: child.signalCode ? null : code, stdout, stderr, timedOut, },
	);
}

export async function dss(
	args: string[],
	opts: CliExecOptions = {},
): Promise<{ stdout: string; stderr: string; }> {
	return runCli(args, opts, "ignore",);
}

export async function dssWithInput(
	args: string[],
	input: string,
	opts: CliExecOptions = {},
): Promise<{ stdout: string; stderr: string; }> {
	return runCli(args, opts, new Blob([input,],),);
}

export async function dssFailure(args: string[], opts: CliExecOptions = {},): Promise<CliFailure> {
	try {
		await dss(args, opts,);
		throw new Error("expected CLI command to fail",);
	} catch (error: unknown) {
		const failure = error as {
			code?: number | null;
			stdout?: string;
			stderr?: string;
			timedOut?: boolean;
		};
		// A hung CLI is a harness-level failure, not an expected command failure.
		if (failure.timedOut === true) throw error;
		return {
			code: failure.code ?? null,
			stdout: failure.stdout ?? "",
			stderr: failure.stderr ?? "",
		};
	}
}

export function readFileExists(path: string,): boolean {
	try {
		readFileSync(path,);
		return true;
	} catch {
		return false;
	}
}

export async function readBody(req: IncomingMessage,): Promise<string> {
	let body = "";
	for await (const chunk of req) {
		body += chunk.toString();
	}
	return body;
}

export function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

export function cliEnv(url: string,): NodeJS.ProcessEnv {
	return {
		...process.env,
		DATAIKU_URL: url,
		DATAIKU_API_KEY: "test-key",
		DATAIKU_PROJECT_KEY: "TEST",
	};
}

export function putItemRefs(body: Record<string, unknown> | undefined,): string[] {
	const recipe = (body?.recipe ?? {}) as Record<string, unknown>;
	const inputs = (recipe.inputs ?? {}) as Record<string, { items?: Array<{ ref?: string; }>; }>;
	return (inputs.main?.items ?? []).map((item,) => item.ref ?? "");
}

export function execProcessLogLine(msg: string,): string {
	return `[2026/06/01-11:30:11.928] [Exec-2] [INFO] [process]  - ${msg}`;
}

export function shortProcessLogLine(msg: string,): string {
	return `[t] [Exec] [INFO] [process]  - ${msg}`;
}

const FINISH_STREAMING_PATH = /\/finish-streaming\/\?readSessionId=[0-9a-f-]{36}$/;

/**
 * Starts a mock DSS server. Dataset finish-streaming checks (sent after a
 * dataset data stream is read to its natural end) answer 200 with an empty
 * body without reaching `handler`.
 */
export async function withCliServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server = createServer((req, res,) => {
		if (FINISH_STREAMING_PATH.test(req.url ?? "",)) {
			res.statusCode = 200;
			res.end();
			return;
		}
		void Promise.resolve(handler(req, res,),).catch((error: unknown,) => {
			res.statusCode = 500;
			res.end(error instanceof Error ? error.message : String(error,),);
		},);
	},);
	await new Promise<void>((resolvePromise, rejectPromise,) => {
		server.listen(0, "127.0.0.1", (error?: Error,) => {
			if (error) {
				rejectPromise(error,);
				return;
			}
			resolvePromise();
		},);
	},);

	const { port, } = server.address() as AddressInfo;
	const url = `http://127.0.0.1:${String(port,)}`;
	try {
		await run(url,);
	} finally {
		await new Promise<void>((resolvePromise, rejectPromise,) => {
			server.close((error,) => {
				if (error) {
					rejectPromise(error,);
					return;
				}
				resolvePromise();
			},);
		},);
	}
}
