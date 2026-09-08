import { randomUUID, } from "node:crypto";
import { createWriteStream, } from "node:fs";
import { chmod, rename, rm, stat, } from "node:fs/promises";
import { basename, dirname, join, } from "node:path";
import { Readable, } from "node:stream";
import { pipeline, } from "node:stream/promises";

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
	const input = Readable.fromWeb(response.body as unknown as import("stream/web").ReadableStream,);
	const temporaryPath = join(dirname(path,), `.${basename(path,)}.tmp-${randomUUID()}`,);
	const output = createWriteStream(temporaryPath, { flags: "wx", mode: 0o600, },);
	try {
		await pipeline(input, output,);
		// Keep partial contents private; restore publication permissions only after completion.
		if (mode !== undefined) await chmod(temporaryPath, mode,);
		await rename(temporaryPath, path,);
		return output.bytesWritten;
	} finally {
		await rm(temporaryPath, { force: true, },);
	}
}
