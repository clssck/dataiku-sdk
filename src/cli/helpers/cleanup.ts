import { readFileSync, } from "node:fs";
import type { CleanupLedgerEntry, } from "../../utils/cleanup-ledger.js";
import { resultRecord, stringField, } from "../coerce.js";

export function projectArg(projectKey: string | undefined,): string[] {
	return projectKey ? ["--project-key", projectKey,] : [];
}

/** Snapshot of the create input for actions whose identifiers live in the JSON
 * payload rather than positional args (e.g. `workspace.create`). Only
 * `--data`/`--data-file` are inspected: restating `--stdin` would re-read a
 * stream the command already consumed, so a stdin-backed create cannot pin
 * identifiers here and the caller must skip it.
 */
function createInputFromFlags(
	flags: Record<string, string | boolean>,
): Record<string, unknown> | undefined {
	const text = typeof flags["data"] === "string"
		? flags["data"] as string
		: typeof flags["data-file"] === "string"
		? ((): string | undefined => {
			try {
				return readFileSync(flags["data-file"] as string, "utf-8",);
			} catch {
				return undefined;
			}
		})()
		: undefined;
	if (text === undefined) return undefined;
	try {
		const parsed: unknown = JSON.parse(text,);
		return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed,)
			? parsed as Record<string, unknown>
			: undefined;
	} catch {
		// An unparsable input cannot pin the resource: no entry, no guess.
		return undefined;
	}
}

/**
 * Creates whose DSS response carries no identifier: the cleanup ledger pins the
 * created object from this field of the `--data`/`--data-file` input instead.
 * `--stdin` cannot be replayed, so the CLI rejects it with `--record-cleanup`
 * before mutating.
 */
const CLEANUP_INPUT_IDENTIFIER_FIELDS: Readonly<Record<string, string>> = {
	"workspace.create": "workspaceKey",
	"connection.create": "name",
	"user.create": "login",
	"group.create": "name",
};

/** Input field that identifies the created object, when the result does not. */
export function cleanupInputIdentifierField(resource: string, action: string,): string | undefined {
	return CLEANUP_INPUT_IDENTIFIER_FIELDS[`${resource}.${action}`];
}

function inputIdentifier(
	resource: string,
	action: string,
	flags: Record<string, string | boolean>,
): { identifier: string; input: Record<string, unknown>; } | undefined {
	const field = cleanupInputIdentifierField(resource, action,);
	if (!field) return undefined;
	const input = resultRecord(createInputFromFlags(flags,),);
	const identifier = stringField(input, [field,],);
	return identifier ? { identifier, input, } : undefined;
}

export function cleanupLedgerEntry(
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
	result: unknown,
	projectKey: string | undefined,
): CleanupLedgerEntry | undefined {
	const record = resultRecord(result,);
	// A notebook save is ledger-eligible only when it created the notebook:
	// an update overwrote something that already existed, so there is nothing
	// for cleanup to remove, and a dry run never mutated anything.
	const isCreatedNotebookSave = (action === "save-jupyter" || action === "save-sql")
		&& record.created === true;
	if (
		!isCreatedNotebookSave
		&& !(action.startsWith("create",) || action === "clone" || action === "duplicate"
			|| action === "import" || action === "upload")
	) return undefined;
	if (record.skipped !== undefined) return undefined;
	// Only an explicit `cleanupEligible:false` suppresses the ledger: absent or
	// true (including indeterminate post-POST outcomes) must stay addressable.
	if (record.cleanupEligible === false) return undefined;
	const project = flags["project-key"] as string | undefined ?? projectKey;
	const withProject = projectArg(project,);
	const ts = new Date().toISOString();
	const base = { ts, action, resource, ...(project ? { projectKey: project, } : {}), };
	switch (`${resource}.${action}`) {
		case "dataset.create": {
			const name = stringField(record, ["created", "name",],) ?? flags["name"] as string | undefined;
			if (!name) return undefined;
			return {
				...base,
				name,
				cleanup: { argv: ["dataset", "delete", name, "--if-exists", ...withProject,], },
			};
		}
		case "dataset.clone": {
			const name = stringField(record, ["target", "created", "name",],) ?? args[1];
			if (!name) return undefined;
			return {
				...base,
				name,
				cleanup: { argv: ["dataset", "delete", name, "--if-exists", ...withProject,], },
			};
		}
		case "dataset.create-managed": {
			// The handler returns `created` as the dataset name and the project
			// DSS created it in, which wins over the requested project key.
			const name = stringField(record, ["created", "name",],) ?? flags["name"] as string | undefined;
			if (!name) return undefined;
			const landedProject = stringField(record, ["projectKey",],) ?? project;
			return {
				...base,
				...(landedProject ? { projectKey: landedProject, } : {}),
				name,
				cleanup: {
					argv: ["dataset", "delete", name, "--if-exists", ...projectArg(landedProject,),],
				},
			};
		}
		case "recipe.create": {
			const name = stringField(record, ["created", "recipeName", "name",],)
				?? flags["name"] as string | undefined;
			if (!name) return undefined;
			return {
				...base,
				name,
				cleanup: { argv: ["recipe", "delete", name, "--if-exists", ...withProject,], },
			};
		}
		case "recipe.clone": {
			const name = stringField(record, ["recipeName", "target", "created", "name",],)
				?? flags["name"] as string | undefined;
			if (!name) return undefined;
			return {
				...base,
				name,
				cleanup: { argv: ["recipe", "delete", name, "--if-exists", ...withProject,], },
			};
		}
		case "scenario.create": {
			const id = args[0];
			return {
				...base,
				id,
				name: args[1],
				cleanup: { argv: ["scenario", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "flow-zone.create": {
			const id = stringField(record, ["created", "id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				name: flags["name"] as string | undefined,
				cleanup: { argv: ["flow-zone", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "folder.create": {
			const id = stringField(record, ["created", "id",],) ?? flags["name"] as string | undefined;
			if (!id) return undefined;
			return {
				...base,
				id,
				name: flags["name"] as string | undefined,
				cleanup: { argv: ["folder", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "wiki.create": {
			const article =
				record.article && typeof record.article === "object" && !Array.isArray(record.article,)
					? record.article as Record<string, unknown>
					: {};
			const id = stringField(record, ["created",],) ?? stringField(article, ["id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				name: flags["name"] as string | undefined,
				cleanup: { argv: ["wiki", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "dashboard.create": {
			const id = stringField(record, ["created", "id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				name: flags["name"] as string | undefined,
				cleanup: { argv: ["dashboard", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "insight.create": {
			const id = stringField(record, ["created", "id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				name: flags["name"] as string | undefined,
				cleanup: { argv: ["insight", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "analysis.create": {
			const id = stringField(record, ["created", "id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				cleanup: { argv: ["analysis", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "model-evaluation-store.create": {
			const id = stringField(record, ["created", "id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				cleanup: {
					argv: ["model-evaluation-store", "delete", id, "--if-exists", ...withProject,],
				},
			};
		}
		case "ml-task.create": {
			const analysisId = args[0] || stringField(record, ["analysisId",],);
			const taskId = stringField(record, ["created", "mlTaskId", "id",],);
			if (!analysisId || !taskId) return undefined;
			return {
				...base,
				id: taskId,
				cleanup: { argv: ["ml-task", "delete", analysisId, taskId, ...withProject,], },
			};
		}
		case "ml-task.create-for-dataset": {
			// This create opens a new visual analysis around the task, so cleanup
			// removes the whole analysis rather than leaving an empty one behind.
			const analysisId = stringField(record, ["analysisId",],);
			if (!analysisId) return undefined;
			return {
				...base,
				id: analysisId,
				cleanup: { argv: ["analysis", "delete", analysisId, "--if-exists", ...withProject,], },
			};
		}
		case "saved-model.create-external": {
			const id = stringField(record, ["id",],);
			if (!id) return undefined;
			const landedProject = stringField(record, ["projectKey",],) ?? project;
			return {
				...base,
				...(landedProject ? { projectKey: landedProject, } : {}),
				id,
				name: args[0],
				cleanup: {
					argv: ["saved-model", "delete", id, "--if-exists", ...projectArg(landedProject,),],
				},
			};
		}
		case "project-folder.create-child": {
			// `created` falls back to the requested name when DSS omits the id,
			// and a name cannot address a folder delete: only `id` pins it.
			const id = stringField(record, ["id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				name: flags["name"] as string | undefined,
				cleanup: { argv: ["project-folder", "delete", id, "--if-exists",], },
			};
		}
		case "data-collection.create": {
			const id = stringField(record, ["id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				cleanup: { argv: ["data-collection", "delete", id,], },
			};
		}
		case "plugin.create-dev": {
			const id = stringField(record, ["created",],) ?? args[0];
			if (!id) return undefined;
			return {
				...base,
				id,
				cleanup: { argv: ["plugin", "delete", id,], },
			};
		}
		case "connection.create":
		case "user.create":
		case "group.create": {
			const pinned = inputIdentifier(resource, action, flags,);
			if (!pinned) return undefined;
			return {
				...base,
				name: pinned.identifier,
				cleanup: { argv: [resource, "delete", pinned.identifier, "--if-exists",], },
			};
		}
		case "data-quality.create-rule": {
			const ruleId = stringField(record, ["id", "created",],);
			if (!ruleId) return undefined;
			return {
				...base,
				id: ruleId,
				name: args[0],
				cleanup: {
					argv: ["data-quality", "delete-rule", args[0], ruleId, "--if-exists", ...withProject,],
				},
			};
		}
		case "code-env.create": {
			const lang = args[0];
			const name = args[1];
			return {
				...base,
				id: `${lang}:${name}`,
				name,
				cleanup: { argv: ["code-env", "delete", lang, name, "--if-exists",], },
			};
		}
		case "streaming-endpoint.create":
		case "streaming-endpoint.create-managed": {
			const id = stringField(record, ["created",],) ?? args[0];
			if (!id) return undefined;
			return {
				...base,
				id,
				cleanup: { argv: ["streaming-endpoint", "delete", id, ...withProject,], },
			};
		}
		case "meaning.create": {
			const id = args[0];
			return {
				...base,
				id,
				name: args[1],
				cleanup: { argv: ["meaning", "delete", id, "--if-exists",], },
			};
		}
		case "workspace.create": {
			const pinned = inputIdentifier(resource, action, flags,);
			if (!pinned) return undefined;
			return {
				...base,
				id: pinned.identifier,
				name: stringField(pinned.input, ["displayName",],),
				cleanup: { argv: ["workspace", "delete", pinned.identifier,], },
			};
		}
		case "project-library.create-file":
		case "project-library.create-folder": {
			const path = args[0];
			return {
				...base,
				path,
				cleanup: { argv: ["project-library", "delete", path, ...withProject,], },
			};
		}
		case "folder.upload":
			return {
				...base,
				name: args[0],
				path: args[1],
				cleanup: { argv: ["folder", "delete-file", args[0], args[1], ...withProject,], },
			};
		case "notebook.save-jupyter": {
			const name = args[0];
			if (!name) return undefined;
			return {
				...base,
				name,
				cleanup: {
					argv: ["notebook", "delete-jupyter", name, "--if-exists", ...withProject,],
				},
			};
		}
		case "notebook.save-sql": {
			// On create the persisted id is the server receipt (`saved`), not the
			// requested handle, which only becomes the display name.
			const id = stringField(record, ["saved", "id",],) ?? args[0];
			if (!id) return undefined;
			return {
				...base,
				id,
				cleanup: {
					argv: ["notebook", "delete-sql", id, "--if-exists", ...withProject,],
				},
			};
		}
		case "project.import": {
			const usedProjectKey = stringField(record, ["usedProjectKey",],);
			const recordedIncarnation = stringField(record, ["projectIncarnationHash",],);
			const boundIncarnation = recordedIncarnation
					&& /^[0-9a-f]{64}$/.test(recordedIncarnation,)
				? recordedIncarnation
				: undefined;
			// Only a verified success may produce a cleanup entry, and it must
			// bind the concrete landed project incarnation: replaying an
			// unbound delete could remove a replacement project that reused the
			// key. Never emit legacy/unbound project entries.
			if (!usedProjectKey || !boundIncarnation) return undefined;
			return {
				...base,
				projectKey: usedProjectKey,
				name: usedProjectKey,
				cleanup: {
					argv: [
						"project",
						"delete",
						usedProjectKey,
						"--if-exists",
						"--expect-project-incarnation",
						boundIncarnation,
					],
				},
			};
		}
		case "app.create-instance":
		case "app.create-successor-instance": {
			const targetKey = stringField(record, ["projectKey", "targetProjectKey",],);
			if (!targetKey) return undefined;
			const nestedInstance = resultRecord(record.instance,);
			const futureId = stringField(record, ["futureId", "jobId",],)
				?? stringField(nestedInstance, ["futureId", "jobId",],);
			const projectIncarnationHash = stringField(record, ["projectIncarnationHash",],);
			const boundIncarnation = projectIncarnationHash
					&& /^[0-9a-f]{64}$/.test(projectIncarnationHash,)
				? projectIncarnationHash
				: undefined;
			// Cleanup can delete only the concrete project incarnation created by
			// this lifecycle. A terminal future proves the requested key, but not
			// that the same project still occupies that key at replay time.
			const settled = record.futureTargetVerified === true;
			const lifecycleArgs = !boundIncarnation || (!settled && !futureId)
				? ["--unconfirmed-creation",]
				: futureId && !settled
				? [
					"--future-id",
					futureId,
					"--expect-project-incarnation",
					boundIncarnation,
				]
				: ["--expect-project-incarnation", boundIncarnation,];
			return {
				...base,
				projectKey: targetKey,
				name: targetKey,
				cleanup: {
					argv: [
						"app",
						"delete-instance",
						"--project-key",
						targetKey,
						...lifecycleArgs,
					],
				},
			};
		}
		default:
			return undefined;
	}
}
