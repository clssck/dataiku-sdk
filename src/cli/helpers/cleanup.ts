import type { CleanupLedgerEntry, } from "../../utils/cleanup-ledger.js";
import { resultRecord, stringField, } from "../coerce.js";

export function projectArg(projectKey: string | undefined,): string[] {
	return projectKey ? ["--project-key", projectKey,] : [];
}

export function cleanupLedgerEntry(
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
	result: unknown,
	projectKey: string | undefined,
): CleanupLedgerEntry | undefined {
	if (
		!(action.startsWith("create",) || action === "clone" || action === "duplicate"
			|| action === "upload")
	) return undefined;
	const record = resultRecord(result,);
	if (record.skipped !== undefined) return undefined;
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
		case "project.duplicate": {
			const targetKey = args[1];
			if (!targetKey) return undefined;
			return {
				ts,
				action,
				resource,
				name: targetKey,
				cleanup: { argv: ["project", "delete", targetKey, "--drop-data",], },
			};
		}
		case "folder.upload":
			return {
				...base,
				name: args[0],
				path: args[1],
				cleanup: { argv: ["folder", "delete-file", args[0], args[1], ...withProject,], },
			};
		default:
			return undefined;
	}
}
