import { describe, expect, it, } from "bun:test";
import { randomUUID, } from "node:crypto";
import { lstat, readFile, rm, symlink, writeFile, } from "node:fs/promises";
import { type HostDaemon, hostDirectoryScript, type OwnedHostDirectory, } from "./live-host.js";

const available = process.platform === "linux" && !!Bun.which("python3",) && !!Bun.which("git",);
const sseAvailable = process.platform === "linux" && !!Bun.which("python3",);
async function command(args: string[],) {
	const child = Bun.spawn(args, { stdout: "pipe", stderr: "pipe", },);
	const [stdout, stderr, code,] = await Promise.all([
		new Response(child.stdout,).text(),
		new Response(child.stderr,).text(),
		child.exited,
	],);
	return { stdout, stderr, code, };
}
async function host(
	entry: OwnedHostDirectory,
	op: Parameters<typeof hostDirectoryScript>[1],
	repositories: string[] = [],
	service?: "sse",
) {
	return command(["python3", "-c", hostDirectoryScript(entry, op, repositories, service,),],);
}
async function fixture(
	body: (entry: OwnedHostDirectory,) => Promise<void>,
	run: typeof host = host,
) {
	const nonce = randomUUID().replaceAll("-", "",);
	const entry: OwnedHostDirectory = {
		nonce,
		runId: "0123456789abcdef",
		path: `/tmp/sdk_live_0123456789abcdef_${nonce}`,
		projectKey: "LOCAL",
		state: "bound",
	};
	const created = await run(entry, "create",);
	if (created.code) throw new Error(created.stderr,);
	let bodyError: unknown;
	try {
		await body(entry,);
	} catch (error) {
		bodyError = error;
	}
	delete entry.daemon;
	const cleanup = await run(entry, "delete",);
	const cleanupError = cleanup.code ? new Error(cleanup.stderr,) : undefined;
	if (bodyError && cleanupError) {
		throw new AggregateError([bodyError, cleanupError,], "Fixture body and cleanup both failed",);
	}
	if (bodyError) throw bodyError;
	if (cleanupError) throw cleanupError;
}

describe.skipIf(!sseAvailable,)("owned host cleanup", () => {
	it("preserves recovery receipts when private contents prevent cleanup", async () => {
		const prefix = "import os\nif os.geteuid() == 0:\n os.setgid(65534)\n os.setuid(65534)\n";
		const run: typeof host = (entry, op,) =>
			command([
				"python3",
				"-c",
				prefix + hostDirectoryScript(entry, op,),
			],);
		await fixture(async entry => {
			const privatePath = `${entry.path}/private`;
			const setup = await command([
				"python3",
				"-c",
				prefix + [
					`os.mkdir(${JSON.stringify(privatePath,)})`,
					`open(${JSON.stringify(privatePath + "/retained",)}, "w").write("keep")`,
					`os.chmod(${JSON.stringify(privatePath,)}, 0)`,
				].join("\n",),
			],);
			if (setup.code) throw new Error(setup.stderr,);
			const failed = await run(entry, "delete",);
			const restored = await command([
				"python3",
				"-c",
				`${prefix}os.chmod(${JSON.stringify(privatePath,)}, 0o700)`,
			],);
			if (restored.code) throw new Error(restored.stderr,);
			expect(failed.code,).not.toBe(0,);
			expect(failed.stderr,).toContain("PermissionError",);
			const retried = await run(entry, "delete",);
			if (retried.code) throw new Error(retried.stderr,);
			expect(await lstat(entry.path,).catch(() => null),).toBeNull();
		}, run,);
	});
},);

describe.skipIf(!available,)("owned host fixtures", () => {
	it("recovers an unrecorded daemon and refuses a changed process identity", async () => {
		await fixture(async entry => {
			const repo = `${entry.path}/fixture.git`;
			const init = await command(["git", "init", "--bare", repo,],);
			expect(init.code,).toBe(0,);
			const started = await host(entry, "start", [repo,],);
			if (started.code) throw new Error(started.stderr,);
			const line = started.stdout.split("\n",).find(row => row.startsWith("HOST_RESULT=",))!;
			const daemon = JSON.parse(line.slice("HOST_RESULT=".length,),) as HostDaemon;
			entry.daemon = daemon;
			const url = `http://127.0.0.1:${daemon.port}/fixture.git`;
			expect((await command(["git", "ls-remote", url,],)).code,).toBe(0,);
			entry.daemon = { ...daemon, startTime: "0", };
			expect((await host(entry, "stop",)).code,).not.toBe(0,);
			expect((await command(["git", "ls-remote", url,],)).code,).toBe(0,);
			delete entry.daemon;
			const deleted = await host(entry, "delete",);
			if (deleted.code) throw new Error(deleted.stderr,);
			expect(await lstat(entry.path,).catch(() => null),).toBeNull();
			expect((await command(["git", "ls-remote", url,],)).code,).not.toBe(0,);
		},);
	}, 30000,);

	it("refuses changed markers and symlinks without deleting fixture contents", async () => {
		await fixture(async entry => {
			const owner = `${entry.path}/.owner`;
			const original = await readFile(owner, "utf8",);
			const content = `${entry.path}/retained`;
			await writeFile(content, "keep",);
			try {
				await writeFile(owner, JSON.stringify({ nonce: "wrong", runId: entry.runId, },),);
				expect((await host(entry, "delete",)).code,).not.toBe(0,);
				expect(await readFile(content, "utf8",),).toBe("keep",);
				await writeFile(`${entry.path}/target`, original,);
				await rm(owner,);
				await symlink(`${entry.path}/target`, owner,);
				expect((await host(entry, "delete",)).code,).not.toBe(0,);
				expect(await readFile(content, "utf8",),).toBe("keep",);
				expect(await readFile(`${entry.path}/target`, "utf8",),).toBe(original,);
			} finally {
				await rm(owner, { force: true, },);
				await writeFile(owner, original,);
			}
		},);
	});

	it("serves listed repositories over HTTP and refuses unlisted ones", async () => {
		await fixture(async entry => {
			const repo = `${entry.path}/fixture.git`;
			const foreign = `${entry.path}/foreign.git`;
			expect((await command(["git", "init", "--bare", repo,],)).code,).toBe(0,);
			expect((await command(["git", "init", "--bare", foreign,],)).code,).toBe(0,);
			expect((await command(["git", "-C", repo, "symbolic-ref", "HEAD", "refs/heads/main",],)).code,)
				.toBe(0,);
			const started = await host(entry, "start", [repo,],);
			if (started.code) throw new Error(started.stderr,);
			const line = started.stdout.split("\n",).find(row => row.startsWith("HOST_RESULT=",))!;
			entry.daemon = JSON.parse(line.slice("HOST_RESULT=".length,),) as HostDaemon;
			const url = `http://127.0.0.1:${entry.daemon.port}/fixture.git`;
			const work = `${entry.path}/work`;
			expect((await command(["git", "clone", url, work,],)).code,).toBe(0,);
			await writeFile(`${work}/seed.txt`, "seed",);
			expect((await command(["git", "-C", work, "add", "seed.txt",],)).code,).toBe(0,);
			expect(
				(await command([
					"git",
					"-C",
					work,
					"-c",
					"user.email=fixture@example.invalid",
					"-c",
					"user.name=fixture",
					"commit",
					"-m",
					"seed",
				],)).code,
			).toBe(0,);
			expect((await command(["git", "-C", work, "push", "origin", "HEAD:refs/heads/main",],)).code,)
				.toBe(0,);
			expect((await command(["git", "ls-remote", url,],)).stdout,).toContain("refs/heads/main",);
			const readback = `${entry.path}/readback`;
			expect((await command(["git", "clone", url, readback,],)).code,).toBe(0,);
			expect(await readFile(`${readback}/seed.txt`, "utf8",),).toBe("seed",);
			expect(
				(await command(["git", "ls-remote", `http://127.0.0.1:${entry.daemon.port}/foreign.git`,],))
					.code,
			).not.toBe(0,);
		},);
	}, 30000,);
},);

type SseRecord = { n: number; value: string; };
async function readSse(url: string, want: number,) {
	const response = await fetch(url, { signal: AbortSignal.timeout(15000,), },);
	expect(response.status,).toBe(200,);
	expect(response.headers.get("content-type",) ?? "",).toContain("text/event-stream",);
	const reader = response.body!.getReader();
	const decoder = new TextDecoder();
	let text = "";
	let consumed = 0;
	const records: SseRecord[] = [];
	let heartbeat = false;
	const until = Date.now() + 8000;
	while (Date.now() < until && (records.length < want || !heartbeat)) {
		const chunk = await reader.read();
		if (chunk.done) break;
		text += decoder.decode(chunk.value, { stream: true, },);
		let split: number;
		while ((split = text.indexOf("\n\n", consumed,)) >= 0) {
			const block = text.slice(consumed, split,);
			consumed = split + 2;
			if (block.split("\n",).some(line => line.startsWith(":",))) heartbeat = true;
			const data = block.split("\n",)
				.filter(line => line.startsWith("data: ",))
				.map(line => line.slice("data: ".length,))
				.join("\n",);
			if (data) records.push(JSON.parse(data,) as SseRecord,);
		}
	}
	await reader.cancel().catch(() => undefined);
	return { records, heartbeat, };
}

describe.skipIf(!sseAvailable,)("owned SSE host fixtures", () => {
	it(
		"streams deterministic events and verifies its identity until receipt-driven cleanup",
		async () => {
			await fixture(async entry => {
				const started = await host(entry, "start", [], "sse",);
				if (started.code) throw new Error(started.stderr,);
				const line = started.stdout.split("\n",).find(row => row.startsWith("HOST_RESULT=",))!;
				const daemon = JSON.parse(line.slice("HOST_RESULT=".length,),) as HostDaemon;
				expect(daemon.service,).toBe("sse",);
				expect(daemon.repositories,).toEqual([],);
				expect(daemon.pid,).toBeGreaterThan(1,);
				expect(daemon.port,).toBeGreaterThanOrEqual(1024,);
				expect(daemon.port,).toBeLessThanOrEqual(65535,);
				entry.daemon = daemon;
				const base = `http://127.0.0.1:${daemon.port}`;
				const { records, heartbeat, } = await readSse(`${base}/events`, 2,);
				expect(records.slice(0, 2,),).toEqual([{ n: 1, value: "one", }, { n: 2, value: "two", },],);
				expect(heartbeat,).toBe(true,);
				expect((await fetch(`${base}/private`, { signal: AbortSignal.timeout(5000,), },)).status,)
					.toBe(404,);
				expect((await host(entry, "verify",)).code,).toBe(0,);
				entry.daemon = { ...daemon, startTime: "0", };
				expect((await host(entry, "verify",)).code,).not.toBe(0,);
				entry.daemon = { ...daemon, service: undefined, };
				expect((await host(entry, "verify",)).code,).not.toBe(0,);
				entry.daemon = { ...daemon, repositories: [`${entry.path}/forged.git`,], };
				expect((await host(entry, "stop",)).code,).not.toBe(0,);
				expect((await fetch(`${base}/events`, { signal: AbortSignal.timeout(5000,), },)).status,)
					.toBe(200,);
				delete entry.daemon;
				const deleted = await host(entry, "delete",);
				if (deleted.code) throw new Error(deleted.stderr,);
				expect(await lstat(entry.path,).catch(() => null),).toBeNull();
				expect(await lstat(`/proc/${daemon.pid}`,).catch(() => null),).toBeNull();
				let refused = false;
				try {
					await fetch(`${base}/events`, { signal: AbortSignal.timeout(3000,), },);
				} catch {
					refused = true;
				}
				expect(refused,).toBe(true,);
			},);
		},
		60000,
	);

	it("fails closed on forged service modes and unknown receipt services", async () => {
		await fixture(async entry => {
			const repo = `${entry.path}/forged.git`;
			expect(() => hostDirectoryScript(entry, "start", [repo,], "sse",)).toThrow(/SSE/,);
			expect(() => hostDirectoryScript(entry, "delete", [repo,], "sse",)).toThrow(/SSE/,);
			const unknownService: string = "git";
			expect(() => hostDirectoryScript(entry, "start", [], unknownService as "sse",))
				.toThrow(/Unsupported/,);
			expect(() => hostDirectoryScript(entry, "start", [],)).toThrow(/requires repositories/,);
			const started = await host(entry, "start", [], "sse",);
			if (started.code) throw new Error(started.stderr,);
			const line = started.stdout.split("\n",).find(row => row.startsWith("HOST_RESULT=",))!;
			entry.daemon = JSON.parse(line.slice("HOST_RESULT=".length,),) as HostDaemon;
			const receipt = `${entry.path}/.daemon.json`;
			const original = await readFile(receipt, "utf8",);
			await writeFile(receipt, JSON.stringify({ ...JSON.parse(original,), service: "git", },),);
			expect((await host(entry, "stop",)).code,).not.toBe(0,);
			await writeFile(receipt, original,);
			expect((await host(entry, "verify",)).code,).toBe(0,);
		},);
	}, 60000,);
},);
