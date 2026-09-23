import { closeSync, openSync, } from "node:fs";
import { chmod, rename, rm, stat, } from "node:fs/promises";
import { basename, dirname, join, } from "node:path";

/** Stream with backpressure; publish only a complete download, preserving existing output on failure. */
export async function writeResponseToFile(
	path: string,
	response: Response,
	permissions: "private" | "preserve" = "private",
): Promise<number> {
	let mode: number | undefined;
	if (permissions === "preserve") {
		try {
			mode = (await stat(path,)).mode & 0o777;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
			mode = 0o666 & ~process.umask();
		}
	}
	const temporaryPath = join(dirname(path,), `.${basename(path,)}.tmp-${crypto.randomUUID()}`,);
	// Exclusive, private temp file; Bun.write streams the body into it natively.
	let fd: number;
	try {
		fd = openSync(temporaryPath, "wx", 0o600,);
	} catch (error) {
		// Nothing will read the body; release the connection before surfacing the error.
		await response.body?.cancel().catch(() => {},);
		throw error;
	}
	try {
		let bytes: number;
		try {
			bytes = await Bun.write(Bun.file(fd,), response,);
		} finally {
			closeSync(fd,);
		}
		// Keep partial contents private; restore publication permissions only after completion.
		if (mode !== undefined) await chmod(temporaryPath, mode,);
		await rename(temporaryPath, path,);
		return bytes;
	} finally {
		await rm(temporaryPath, { force: true, },);
	}
}
