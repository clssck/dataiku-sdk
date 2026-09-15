import { describe, expect, it, } from "bun:test";
import { randomUUID, } from "node:crypto";
import { lstat, readFile, rm, symlink, writeFile, } from "node:fs/promises";
import { type HostDaemon, hostDirectoryScript, type OwnedHostDirectory, } from "./live-host.js";

const available = process.platform === "linux" && !!Bun.which("python3",) && !!Bun.which("git",);
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
) {
	return command(["python3", "-c", hostDirectoryScript(entry, op, repositories,),],);
}
async function fixture(body: (entry: OwnedHostDirectory,) => Promise<void>,) {
	const nonce = randomUUID().replaceAll("-", "",);
	const entry: OwnedHostDirectory = {
		nonce,
		runId: "0123456789abcdef",
		path: `/tmp/sdk_live_0123456789abcdef_${nonce}`,
		projectKey: "LOCAL",
		state: "bound",
	};
	const created = await host(entry, "create",);
	if (created.code) throw new Error(created.stderr,);
	let bodyError: unknown;
	try {
		await body(entry,);
	} catch (error) {
		bodyError = error;
	}
	delete entry.daemon;
	const cleanup = await host(entry, "delete",);
	const cleanupError = cleanup.code ? new Error(cleanup.stderr,) : undefined;
	if (bodyError && cleanupError) {
		throw new AggregateError([bodyError, cleanupError,], "Fixture body and cleanup both failed",);
	}
	if (bodyError) throw bodyError;
	if (cleanupError) throw cleanupError;
}

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
