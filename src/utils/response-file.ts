import { randomUUID, } from "node:crypto";
import { createWriteStream, } from "node:fs";
import { rename, rm, } from "node:fs/promises";
import { basename, dirname, join, } from "node:path";
import { Readable, } from "node:stream";
import { pipeline, } from "node:stream/promises";

/** Stream with backpressure; publish only a complete download, preserving existing output on failure. */
export async function writeResponseToFile(path: string, response: Response,): Promise<number> {
	const input = Readable.fromWeb(response.body as unknown as import("stream/web").ReadableStream,);
	const temporaryPath = join(dirname(path,), `.${basename(path,)}.tmp-${randomUUID()}`,);
	const output = createWriteStream(temporaryPath, { flags: "wx", mode: 0o600, },);
	try {
		await pipeline(input, output,);
		await rename(temporaryPath, path,);
		return output.bytesWritten;
	} finally {
		await rm(temporaryPath, { force: true, },);
	}
}
