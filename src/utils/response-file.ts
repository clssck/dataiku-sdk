import { chmod, type FileHandle, mkdir, open, rename, rm, stat, } from "node:fs/promises";
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
	// Exclusive, private temp file. The body is copied chunk by chunk with an
	// awaited write per chunk (backpressure). Not Bun.write(Bun.file(fd), response):
	// on Windows, Bun 1.4.3 let the process exit 0 mid-copy with nothing written.
	let handle: FileHandle;
	try {
		await mkdir(dirname(path,), { recursive: true, },);
		handle = await open(temporaryPath, "wx", 0o600,);
	} catch (error) {
		// Nothing will read the body; release the connection before surfacing the error.
		await response.body?.cancel().catch(() => {},);
		throw error;
	}
	try {
		let bytes = 0;
		try {
			if (response.body) {
				for await (const chunk of response.body) {
					await handle.write(chunk,);
					bytes += chunk.byteLength;
				}
			}
		} finally {
			await handle.close();
		}
		// Keep partial contents private; restore publication permissions only after completion.
		if (mode !== undefined) await chmod(temporaryPath, mode,);
		await rename(temporaryPath, path,);
		return bytes;
	} finally {
		await rm(temporaryPath, { force: true, },);
	}
}
