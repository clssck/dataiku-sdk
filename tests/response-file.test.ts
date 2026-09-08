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
		expect((await stat(path,)).mode & 0o777,).toBe(0o666 & ~process.umask(),);
		await chmod(path, 0o660,);
		await writeResponseToFile(path, new Response("second",), "preserve",);
		expect((await stat(path,)).mode & 0o777,).toBe(0o660,);
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
		const response = new Response(
			new ReadableStream({
				pull(controller,) {
					controller.error(new Error("stream failed",),);
				},
			},),
		);
		await expect(writeResponseToFile(path, response, "preserve",),).rejects.toThrow("stream failed",);
		expect(await readFile(path, "utf8",),).toBe("original",);
		expect((await stat(path,)).mode & 0o777,).toBe(0o640,);
		expect(await readdir(dir,),).toEqual(["result",],);
	} finally {
		await rm(dir, { recursive: true, force: true, },);
	}
});
