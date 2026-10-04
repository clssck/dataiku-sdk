import type { DataikuClient, } from "../../client.js";
import type { DocumentationTemplate, } from "../../resources/documentation.js";
import type { DssTask, FutureWaitOptions, } from "../../resources/futures.js";
import type { FutureWaitResult, } from "../../schemas.js";
import { writeResponseToFile, } from "../../utils/response-file.js";
import { num, } from "../coerce.js";
import { UsageError, } from "../usage.js";

type Flags = Record<string, string | boolean>;

/** Documentation template from flags: --template-file, or --folder with --path, or the DSS default. */
export function documentationTemplateFromFlags(flags: Flags,): DocumentationTemplate {
	const file = flags["template-file"];
	const folder = flags["folder"];
	const path = flags["path"];
	if (typeof file === "string" && (folder !== undefined || path !== undefined)) {
		throw new UsageError(
			"Use either --template-file or --folder with --path, not both.",
			"conflicting_input_sources",
		);
	}
	if (typeof file === "string") return { kind: "file", filePath: file, };
	if (typeof folder === "string" || typeof path === "string") {
		if (typeof folder !== "string" || typeof path !== "string") {
			throw new UsageError(
				"A template in a managed folder needs both --folder FOLDER_ID and --path PATH.",
				"missing_required_flag",
			);
		}
		return { kind: "folder", folderId: folder, path, };
	}
	return { kind: "default", };
}

/** `--poll-interval` / `--timeout` for commands that wait on a DSS task. */
export function taskWaitOptions(flags: Flags,): FutureWaitOptions {
	return {
		pollIntervalMs: num(flags["poll-interval"], "--poll-interval",),
		timeoutMs: num(flags["timeout"], "--timeout",),
	};
}

/**
 * Shared `generate-documentation` flow: return the started task; with
 * --wait, its final state; with --output PATH, wait and download the docx
 * named by the result's `exportId`. A failed or timed-out generation carries
 * no exportId, so its state is returned instead.
 */
export async function runDocumentationCommand(
	client: DataikuClient,
	flags: Flags,
	task: DssTask,
	download: (exportId: string,) => Promise<Response>,
): Promise<DssTask | FutureWaitResult | { path: string; bytes: number; exportId: string; }> {
	const output = flags["output"];
	if (flags["wait"] !== true && typeof output !== "string") return task;
	const waited = await client.futures.waitTask(task, taskWaitOptions(flags,),);
	if (typeof output !== "string") return waited;
	const result = waited.result;
	const exportId = result && typeof result === "object" && "exportId" in result
		? result.exportId
		: undefined;
	if (typeof exportId !== "string") return waited;
	return {
		path: output,
		bytes: await writeResponseToFile(output, await download(exportId,),),
		exportId,
	};
}
