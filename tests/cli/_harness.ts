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
export const SDK_ROOT = resolve(dirname(fileURLToPath(import.meta.url,),), "..", "..",);
export const CLI_PATH = join(SDK_ROOT, "src/cli.ts",);
export const BUN = process.execPath;

export type CliExecOptions = { cwd?: string; env?: NodeJS.ProcessEnv; };
export type CliFailure = { code: number | null; stdout: string; stderr: string; };

export async function dss(
	args: string[],
	opts: CliExecOptions = {},
): Promise<{ stdout: string; stderr: string; }> {
	return exec(BUN, ["--no-env-file", "run", CLI_PATH, ...args,], {
		cwd: opts.cwd ?? SDK_ROOT,
		env: opts.env ?? process.env,
	},);
}

export async function dssWithInput(
	args: string[],
	input: string,
	opts: CliExecOptions = {},
): Promise<{ stdout: string; stderr: string; }> {
	return new Promise((resolvePromise, rejectPromise,) => {
		const child = spawn(BUN, ["--no-env-file", "run", CLI_PATH, ...args,], {
			cwd: opts.cwd ?? SDK_ROOT,
			env: opts.env ?? process.env,
		},);
		let stdout = "";
		let stderr = "";

		child.stdout.setEncoding("utf8",);
		child.stderr.setEncoding("utf8",);
		child.stdout.on("data", (chunk: string,) => {
			stdout += chunk;
		},);
		child.stderr.on("data", (chunk: string,) => {
			stderr += chunk;
		},);
		child.stdin.on("error", () => {
			// Ignore EPIPE if the process exits before consuming all input.
		},);
		child.on("error", rejectPromise,);
		child.on("close", (code,) => {
			if (code === 0) {
				resolvePromise({ stdout, stderr, },);
				return;
			}
			rejectPromise(Object.assign(new Error(`CLI exited with code ${String(code,)}`,), {
				code,
				stdout,
				stderr,
			},),);
		},);
		child.stdin.end(input,);
	},);
}

export async function dssFailure(args: string[], opts: CliExecOptions = {},): Promise<CliFailure> {
	try {
		await dss(args, opts,);
		throw new Error("expected CLI command to fail",);
	} catch (error: unknown) {
		const failure = error as { code?: number | null; stdout?: string; stderr?: string; };
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

export async function withCliServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server = createServer((req, res,) => {
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
