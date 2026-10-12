import { expect, it, } from "bun:test";
import { chmod, mkdtemp, readdir, readFile, rm, stat, writeFile, } from "node:fs/promises";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { writeResponseToFile, } from "../src/utils/response-file.js";

it("uses umask for new exports and preserves existing permission bits", async () => {
	const dir = await mkdtemp(join(tmpdir(), "dss-permissions-",),);
	const path = join(dir, "result",);
	try {
		await writeResponseToFile(path, new Response("first",), "preserve",);
		if (process.platform !== "win32") {
			expect((await stat(path,)).mode & 0o777,).toBe(0o666 & ~process.umask(),);
		}
		await chmod(path, 0o660,);
		const originalMode = (await stat(path,)).mode & 0o777;
		await writeResponseToFile(path, new Response("second",), "preserve",);
		expect((await stat(path,)).mode & 0o777,).toBe(originalMode,);
		expect(await readFile(path, "utf8",),).toBe("second",);
	} finally {
		await rm(dir, { recursive: true, force: true, },);
	}
});

it("preserves contents and permissions when an export stream fails", async () => {
	const dir = await mkdtemp(join(tmpdir(), "dss-permissions-",),);
	const path = join(dir, "result",);
	try {
		await writeFile(path, "original",);
		await chmod(path, 0o640,);
		const originalMode = (await stat(path,)).mode & 0o777;
		const response = new Response(
			new ReadableStream({
				pull(controller,) {
					controller.error(new Error("stream failed",),);
				},
			},),
		);
		await expect(writeResponseToFile(path, response, "preserve",),).rejects.toThrow("stream failed",);
		expect(await readFile(path, "utf8",),).toBe("original",);
		expect((await stat(path,)).mode & 0o777,).toBe(originalMode,);
		expect(await readdir(dir,),).toEqual(["result",],);
	} finally {
		await rm(dir, { recursive: true, force: true, },);
	}
});

it("cancels the response body when the output cannot be opened", async () => {
	const dir = await mkdtemp(join(tmpdir(), "dss-cancel-",),);
	let cancelled = false;
	const response = new Response(
		new ReadableStream({
			pull(controller,) {
				controller.enqueue(new Uint8Array([1, 2, 3,],),);
			},
			cancel() {
				cancelled = true;
			},
		},),
	);
	try {
		// The parent path is a regular file, so the output directory cannot be created.
		await writeFile(join(dir, "blocker",), "x",);
		const error = await writeResponseToFile(join(dir, "blocker", "result",), response,).catch((
			caught: unknown,
		) => caught);
		expect(["ENOTDIR", "EEXIST",],).toContain((error as NodeJS.ErrnoException).code,);
		expect(cancelled,).toBe(true,);
		expect(await readdir(dir,),).toEqual(["blocker",],);
	} finally {
		await rm(dir, { recursive: true, force: true, },);
	}
});

it("creates missing parent directories for the output", async () => {
	const dir = await mkdtemp(join(tmpdir(), "dss-parents-",),);
	try {
		const path = join(dir, "a", "b", "result.bin",);
		expect(await writeResponseToFile(path, new Response("payload",),),).toBe(7,);
		expect(await readFile(path, "utf-8",),).toBe("payload",);
	} finally {
		await rm(dir, { recursive: true, force: true, },);
	}
});
