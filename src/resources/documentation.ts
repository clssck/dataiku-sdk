import type { DataikuClient, } from "../client.js";
import { type DssTask, dssTask, } from "./futures.js";

/** Documentation template: the DSS default, an uploaded docx, or a docx in a managed folder. */
export type DocumentationTemplate =
	| { kind: "default"; }
	| { kind: "file"; filePath: string; }
	| { kind: "folder"; folderId: string; path: string; };

/** The three generation endpoints of one documented object (flow, lab model, saved-model version). */
export interface DocumentationRoutes {
	default: string;
	file: string;
	folder: string;
}

/**
 * Start a DSS documentation export (docx): POST the default route, upload the
 * template as multipart `file`, or POST the folder route with `folderId` and
 * `path`. The finished task's result carries the `exportId` to download.
 */
export async function startDocumentation(
	client: DataikuClient,
	routes: DocumentationRoutes,
	template: DocumentationTemplate,
	operation: string,
): Promise<DssTask> {
	if (template.kind === "file") {
		return dssTask(await client.uploadJson<unknown>(routes.file, template.filePath,), operation,);
	}
	if (template.kind === "folder") {
		const query = new URLSearchParams({ folderId: template.folderId, path: template.path, },);
		return dssTask(await client.post<unknown>(`${routes.folder}?${query}`,), operation,);
	}
	return dssTask(await client.post<unknown>(routes.default,), operation,);
}
