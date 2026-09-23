import * as fs from "node:fs";
import * as path from "node:path";
import { APP_MANIFEST_CONCURRENCY_CONTROL, } from "../resources/applications.js";
import { buildDatasetCreateBody, } from "../resources/dataset-create.js";
import { validatePluginDestinationPath, validatePluginPath, } from "../resources/plugins.js";
import {
	encodeLibraryPath,
	PROJECT_LIBRARY_CONCURRENCY_CONTROL,
	validateLibraryDestinationPath,
	validateLibraryName,
	validateLibraryPath,
} from "../resources/project-library.js";
import { buildRecipeCreateRequest, } from "../resources/recipe-create.js";
import { encodeGitReferencePath, validateGitReferencePath, } from "../utils/git-reference.js";
import {
	jobBuildTargetTypeFromFlags,
	json,
	jsonInput,
	num,
	parseBooleanOption,
	parseJsonObject,
	requiredJsonInput,
	rewritePairsFromFlags,
	schemaColumnsInput,
	sha256Hex,
	stableHash,
	stringField,
	textInput,
} from "./coerce.js";
import { parseCodeRunIntegerFlag, resolveCodeInputWithSource, } from "./commands/code.js";
import { projectLibraryPutPayload, } from "./commands/project-library.js";
import { recipeCreateOptionsFromFlags, } from "./commands/recipe.js";
import { resolveSqlQueryInvocation, } from "./commands/sql.js";
import { buildRegistryEntry, type CommandRegistryEntry, splitPackageSpec, } from "./contract.js";
import { ambientProjectKey, } from "./env.js";
import { executionMode, } from "./flags.js";
import { flowZoneColor, flowZoneMoveItems, flowZoneName, } from "./helpers/flow-zone.js";
import { recipeBackupPath, recipeRunShouldWait, } from "./helpers/recipe.js";
import { encodedProjectEndpointForPlan, planResult, } from "./output.js";
import { commandUsage, } from "./syntax.js";
import type { CommandMeta, } from "./types.js";
import { requireArgs, UsageError, } from "./usage.js";

function codeEnvWait(flags: Record<string, string | boolean>,): boolean {
	return flags["no-wait"] !== true;
}

function codeEnvLang(value: string | undefined, usage: string,): "PYTHON" | "R" {
	if (value !== "PYTHON" && value !== "R") {
		throw new UsageError(
			`Invalid code environment language ${JSON.stringify(value,)}. Usage: ${usage}`,
		);
	}
	return value;
}

function codeEnvParams(flags: Record<string, string | boolean>,): Record<string, unknown> {
	const params = json(flags["params"],) ?? jsonInput(flags,) ?? {};
	if (typeof flags["python-interpreter"] === "string") {
		params.pythonInterpreter = flags["python-interpreter"];
	}
	return params;
}

/**
 * Resolve requested package specs from --file/--packages/--package. An
 * explicit source that resolves to zero specs is a legitimate clear
 * (set-packages replaces specPackageList wholesale); only a call with no
 * package source flag at all is a usage error.
 */
function codeEnvPackageList(flags: Record<string, string | boolean>,): string[] {
	const file = flags["file"];
	const packages = typeof file === "string"
		? splitPackageSpec(fs.readFileSync(file, "utf-8",),)
		: [];
	if (typeof flags["packages"] === "string") {
		packages.push(...splitPackageSpec(flags["packages"],),);
	}
	if (typeof flags["package"] === "string") {
		packages.push(...splitPackageSpec(flags["package"],),);
	}
	if (
		typeof file !== "string"
		&& typeof flags["packages"] !== "string"
		&& typeof flags["package"] !== "string"
	) {
		throw new UsageError(
			"--packages, --package, or --file is required. Use newline-separated package specs for version constraints; an explicitly empty value (or empty file) clears the requested package list.",
		);
	}
	return packages;
}

function exitCodesOnFailure(entry: CommandRegistryEntry,): Record<string, number> {
	return {
		usage: entry.exitCodes.usage,
		error: entry.exitCodes.error,
		transient: entry.exitCodes.transient,
		...(entry.exitCodes.longRunningFailure !== undefined
			? { longRunningFailure: entry.exitCodes.longRunningFailure, }
			: {}),
		...(entry.exitCodes.assertionFailure !== undefined
			? { assertionFailure: entry.exitCodes.assertionFailure, }
			: {}),
	};
}

/**
 * Plan-local project key resolution. `--plan` is a purely local preview, so it
 * resolves the target project from the explicit `--project-key` flag and the
 * documented `DATAIKU_PROJECT_KEY` environment variable only. The saved
 * credentials file is never opened and no API key is ever resolved, so planning
 * can neither depend on nor leak stored secrets.
 */
function planProjectKeyFromArgs(
	flags: Record<string, string | boolean>,
): string | undefined {
	const fromFlag = flags["project-key"];
	if (typeof fromFlag === "string" && fromFlag.trim().length > 0) return fromFlag.trim();
	const fromEnv = ambientProjectKey();
	return fromEnv !== undefined && fromEnv.trim().length > 0 ? fromEnv.trim() : undefined;
}

/**
 * Commands whose plan must pin the target with an explicit `--project-key`
 * instead of falling back to ambient `DATAIKU_PROJECT_KEY`, so a cleanup plan
 * can never silently retarget a project the caller did not name.
 */
const EXPLICIT_PLAN_PROJECT_KEY: Record<string, true> = { "app.delete-instance": true, };

/** Plan-local project key, or the canonical `--project-key` usage error. */
function requiredPlanProjectKey(
	flags: Record<string, string | boolean>,
	usage: string,
): string {
	return planProjectKeyFromArgs(flags,) ?? requiredPlanFlag(flags, "project-key", usage,);
}

function projectKeyForPlan(
	entry: CommandRegistryEntry,
	flags: Record<string, string | boolean>,
): string | undefined {
	if (!entry.requiresProject) return undefined;
	if (EXPLICIT_PLAN_PROJECT_KEY[`${entry.resource}.${entry.action}`] === true) {
		return requiredPlanFlag(flags, "project-key", entry.usage,);
	}
	const projectKey = planProjectKeyFromArgs(flags,);
	if (projectKey) return projectKey;
	throw new UsageError(
		`Missing project key. Pass --project-key or set DATAIKU_PROJECT_KEY before planning ${entry.resource} ${entry.action}.`,
	);
}

function requiredPlanFlag(
	flags: Record<string, string | boolean>,
	name: string,
	usage: string,
): string {
	const value = flags[name];
	// Return trimmed like the runtime's requiredStringFlag so plans advertise
	// exactly the identifier the command will act on.
	if (typeof value === "string" && value.trim().length > 0) return value.trim();
	throw new UsageError(`--${name} is required. Usage: ${usage}`,);
}

/**
 * Optional caller-chosen identifier. Absent selects the command's generated
 * mode; a present-but-empty value is an input error, never a silent omission.
 */
function optionalPlanFlag(
	flags: Record<string, string | boolean>,
	name: string,
	usage: string,
): string | undefined {
	const value = flags[name];
	if (value === undefined) return undefined;
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(
			`--${name} must not be empty when supplied; omit --${name} entirely to generate the successor key during apply. Usage: ${usage}`,
			"validation_failed",
		);
	}
	return value.trim();
}

function optionalJsonFlag(
	flags: Record<string, string | boolean>,
	name: string,
): Record<string, unknown> | undefined {
	const value = flags[name];
	return typeof value === "string" ? parseJsonObject(value, `--${name}`,) : undefined;
}

function requiredPlanJsonInput(
	flags: Record<string, string | boolean>,
	usage: string,
): Record<string, unknown> {
	return requiredJsonInput(flags, `--data, --data-file, or --stdin is required. Usage: ${usage}`,);
}

function dataQualityEndpoint(projectKey: string, datasetName: string, suffix: string,): string {
	return encodedProjectEndpointForPlan(
		projectKey,
		`/datasets/${encodeURIComponent(datasetName,)}/data-quality${suffix}`,
	);
}

function querySuffix(params: Record<string, string | number | boolean | undefined>,): string {
	const search = new URLSearchParams();
	for (const [key, value,] of Object.entries(params,)) {
		if (value !== undefined) search.set(key, String(value,),);
	}
	const raw = search.toString();
	return raw ? `?${raw}` : "";
}

function projectFolderEndpoint(folderId: string,): string {
	return `/public/api/project-folders/${encodeURIComponent(folderId,)}`;
}
/**
 * Git and future API mount. The official Python client mounts these routes on
 * `/dip/publicapi`, which is also what the project-git resource uses; do not
 * normalize them to the repo-wide `/public/api`.
 */
const PROJECT_GIT_API_ROOT = "/dip/publicapi";

function projectGitEndpoint(projectKey: string | undefined, suffix: string,): string {
	if (!projectKey) throw new UsageError("--project-key is required for project-git mutations.",);
	return `${PROJECT_GIT_API_ROOT}/projects/${encodeURIComponent(projectKey,)}/git${suffix}`;
}

function projectGitFutureEndpoint(jobId: string,): string {
	return `${PROJECT_GIT_API_ROOT}/futures/${encodeURIComponent(jobId,)}`;
}

/**
 * `--plan` bypasses the SDK boundary guard, so HTTP(S) URLs with embedded
 * userinfo are rejected here before any plan is printed. SSH/scp-style URLs
 * (`git@host:org/repo.git`, `ssh://...`) remain valid.
 */
function validatedPlanRepositoryUrl(url: string, flag: string, usage: string,): string {
	const candidate = url.trim();
	if (/[\u0000-\u001f\u007f]/u.test(candidate,)) {
		throw new UsageError(`--${flag} must not contain control characters. Usage: ${usage}`,);
	}
	if (/^https?:/i.test(candidate,)) {
		if (candidate.includes("\\",)) {
			throw new UsageError(`--${flag} must be a valid HTTP(S) URL. Usage: ${usage}`,);
		}
		if (!/^https?:\/\//i.test(candidate,)) {
			throw new UsageError(`--${flag} must be a valid HTTP(S) URL. Usage: ${usage}`,);
		}
		let parsed: URL;
		try {
			parsed = new URL(candidate,);
		} catch {
			throw new UsageError(`--${flag} must be a valid HTTP(S) URL. Usage: ${usage}`,);
		}
		if (parsed.username !== "" || parsed.password !== "") {
			throw new UsageError(
				`--${flag} must not contain embedded credentials (userinfo). Usage: ${usage}`,
			);
		}
	}
	return candidate;
}

function pluginRootEndpoint(pluginId: string, suffix: string,): string {
	return `/public/api/plugins/${encodeURIComponent(pluginId,)}${suffix}`;
}

function pluginActionEndpoint(pluginId: string, action: string,): string {
	return pluginRootEndpoint(pluginId, `/actions/${action}`,);
}

function pluginContentsEndpoint(pluginId: string, contentPath: string,): string {
	return `${pluginRootEndpoint(pluginId, "/contents/",)}${
		encodePluginSegmentsForPlan(contentPath,)
	}`;
}

function pluginFoldersEndpoint(pluginId: string, contentPath: string,): string {
	return `${pluginRootEndpoint(pluginId, "/folders/",)}${encodePluginSegmentsForPlan(contentPath,)}`;
}

/** Encodes a plugin content path per segment for plan-time endpoints. */
function encodePluginSegmentsForPlan(contentPath: string,): string {
	return contentPath.split("/",).map((segment,) => encodeURIComponent(segment,)).join("/",);
}

/**
 * Plan payload for plugin git install/update bodies. The repository URL is
 * validated (embedded userinfo rejected) and secrets are never echoed: the
 * plan carries the URL only, never embedded credentials.
 */
function pluginGitPlanPayload(
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	const repository = requiredPlanFlag(
		flags,
		"repository",
		"dss plugin install-from-git --repository URL",
	);
	// Mirrors PluginsResource install/update bodies: absent options are sent as null.
	return {
		gitRepositoryUrl: validatedPlanRepositoryUrl(repository, "repository", "--repository URL",),
		gitCheckout: typeof flags["checkout"] === "string" ? flags["checkout"] : null,
		gitSubpath: typeof flags["path-in-repository"] === "string" ? flags["path-in-repository"] : null,
	};
}

function optionalPlanProjectScope(
	flags: Record<string, string | boolean>,
): string {
	const projectKey = flags["project-key"];
	if (typeof projectKey !== "string" || projectKey.trim() === "") return "";
	return `?projectKey=${encodeURIComponent(projectKey.trim(),)}`;
}

function settingsConfigKeys(flags: Record<string, string | boolean>,): string[] {
	const content = flags["content"];
	if (typeof content !== "string") return [];
	try {
		const parsed: unknown = JSON.parse(content,);
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed,)) return [];
		return Object.keys(parsed as Record<string, unknown>,);
	} catch {
		return [];
	}
}

function contentSourceKind(flags: Record<string, string | boolean>,): string {
	if (flags["stdin"] === true) return "stdin";
	if (typeof flags["file"] === "string") return "file";
	if (typeof flags["content"] === "string") return "content";
	return "unknown";
}

/**
 * Plan-safe view of a user create/update body: password values are replaced by
 * a fixed marker so secrets never appear in --plan output.
 */
function redactedUserPayload(
	payload: Record<string, unknown>,
): Record<string, unknown> {
	if (payload["password"] === undefined) return payload;
	return { ...payload, password: "<omitted>", };
}

/**
 * Plan-safe view of a connection create/update body: the connection-type
 * specific `params` object may carry credentials (passwords, keys, tokens), so
 * the plan carries only the marker, never the raw params.
 */
function redactedConnectionPayload(
	payload: Record<string, unknown>,
): Record<string, unknown> {
	if (payload["params"] === undefined) return payload;
	return { ...payload, params: "<omitted; may include credentials>", };
}

/** Required CSV flag for plans (comma-separated logins list). */
function requiredPlanCsv(
	flags: Record<string, string | boolean>,
	name: string,
	usage: string,
): string[] {
	const raw = requiredPlanFlag(flags, name, usage,);
	return raw.split(",",).map((entry,) => entry.trim()).filter((entry,) => entry.length > 0);
}

function requiredPlanRepositoryUrl(
	flags: Record<string, string | boolean>,
	flag: string,
	usage: string,
): string {
	const url = requiredPlanFlag(flags, flag, usage,);
	return validatedPlanRepositoryUrl(url, flag, usage,);
}

/** Wait procedure advertised for the library calls that return a job id. */
function projectGitFutureWait(): Record<string, unknown> {
	return {
		when: "after-dispatch",
		endpoint: `${PROJECT_GIT_API_ROOT}/futures/{jobId}?peek=false`,
		description: "Poll the returned job id until the future reports a result; never abort it.",
	};
}

function jobBuildPayload(
	target: string,
	projectKey: string,
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	const targetType = jobBuildTargetTypeFromFlags(flags,);
	const partition = flags["partition"] as string | undefined;
	const output: Record<string, unknown> = { projectKey, id: target, type: targetType, };
	if (targetType === "DATASET") {
		if (partition !== undefined) output.partition = partition;
	} else {
		output.targetManagedFolderProjectKey = projectKey;
		output.targetManagedFolder = target;
		output.targetPartition = partition ?? "NP";
	}
	const payload: Record<string, unknown> = {
		outputs: [output,],
		type: (flags["build-mode"] as string | undefined) ?? "NON_RECURSIVE_FORCED_BUILD",
	};
	if (flags["force-rebuild"] === true && targetType === "DATASET") {
		payload.autoUpdateSchemaBeforeEachRecipeRun = true;
	}
	return payload;
}

function uploadPayload(filePath: string,): Record<string, unknown> {
	return {
		contentType: "multipart/form-data",
		fileField: "file",
		filePath,
		fileName: path.basename(filePath,),
	};
}

export function commandPlanShape(
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
	entry: CommandRegistryEntry,
	projectKey: string | undefined,
): {
	endpoint?: string;
	exact?: boolean;
	identifiers?: Record<string, unknown>;
	method?: string;
	payload?: unknown;
	localWrites?: unknown;
	reason?: string;
	wait?: unknown;
	requests?: unknown;
} {
	const projectEndpoint = (suffix: string,) => {
		if (!projectKey) throw new UsageError(`Missing project key for ${resource} ${action}.`,);
		return encodedProjectEndpointForPlan(projectKey, suffix,);
	};
	const id = args[0];
	const codeEnvEndpoint = (suffix = "",) =>
		`/public/api/admin/code-envs/${encodeURIComponent(codeEnvLang(args[0], entry.usage,),)}/${
			encodeURIComponent(args[1],)
		}${suffix}`;
	const statisticsWorksheetsEndpoint = (datasetName: string,) =>
		projectEndpoint(`/datasets/${encodeURIComponent(datasetName,)}/statistics/worksheets/`,);
	const statisticsWorksheetEndpoint = (datasetName: string, worksheetId: string,) =>
		`${statisticsWorksheetsEndpoint(datasetName,)}${encodeURIComponent(worksheetId,)}`;
	switch (`${resource}.${action}`) {
		case "sql.query": {
			const payload = resolveSqlQueryInvocation(args, flags, planProjectKeyFromArgs(flags,),);
			return {
				method: "POST",
				endpoint: "/public/api/sql/queries/",
				identifiers: {
					connection: payload.connection,
					dataset: payload.datasetFullName,
				},
				payload,
			};
		}
		case "ml-task.train":
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/models/lab/${encodeURIComponent(args[0],)}/${encodeURIComponent(args[1],)}/train`,
				),
				identifiers: { analysisId: args[0], mlTaskId: args[1], },
				payload: {
					sessionName: flags["session-name"] as string | undefined,
					runQueue: false,
				},
				wait: flags["wait"] === true,
			};
		case "wiki.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			const content = textInput(flags,);
			const create = {
				method: "POST",
				endpoint: projectEndpoint("/wiki/",),
				payload: { projectKey, name, parent: flags["parent"] as string | undefined ?? null, },
			};
			return {
				...create,
				identifiers: { name, },
				// Content is not accepted by the create endpoint; it lands in a follow-up update.
				...(content === undefined ? {} : {
					requests: [
						{ sequence: 1, ...create, },
						{ sequence: 2, method: "GET", endpoint: projectEndpoint("/wiki/{createdArticleId}",), },
						{
							sequence: 3,
							method: "PUT",
							endpoint: projectEndpoint("/wiki/{createdArticleId}",),
							payload: { payload: content, },
						},
					],
				}),
			};
		}
		case "wiki.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/wiki/${encodeURIComponent(id,)}`,),
				identifiers: { article: id, },
				payload: {
					...jsonInput(flags,),
					name: flags["name"] as string | undefined,
					content: textInput(flags,),
				},
			};
		case "wiki.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/wiki/${encodeURIComponent(id,)}`,),
				identifiers: { article: id, },
			};
		case "dashboard.create": {
			const data = jsonInput(flags,);
			const flagName = flags["name"] as string | undefined;
			const dataName = data?.["name"];
			const name = flagName ?? (typeof dataName === "string" ? dataName : undefined);
			if (!name) {
				throw new UsageError(
					`--name or dashboard settings containing a string name are required. Usage: ${
						commandUsage("dashboard", "create",)
					}`,
				);
			}
			const listed = parseBooleanOption(flags["listed"], "--listed",);
			const payload: Record<string, unknown> = { ...(data ?? { pages: [], }), name, };
			if (listed !== undefined) payload.listed = listed;
			return {
				method: "POST",
				endpoint: projectEndpoint("/dashboards/",),
				identifiers: { name, },
				payload,
			};
		}
		case "dashboard.update": {
			const payload: Record<string, unknown> = { ...jsonInput(flags,), };
			if (typeof flags["name"] === "string") payload.name = flags["name"];
			const listed = parseBooleanOption(flags["listed"], "--listed",);
			if (listed !== undefined) payload.listed = listed;
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/dashboards/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
				payload,
			};
		}
		case "dashboard.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/dashboards/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
			};
		case "insight.create": {
			const data = jsonInput(flags,);
			const name = flags["name"] as string | undefined;
			const type = flags["type"] as string | undefined;
			if (!data && (!name || !type)) {
				throw new UsageError(
					`--data or both --name and --type are required. Usage: ${commandUsage("insight", "create",)}`,
				);
			}
			const prototype: Record<string, unknown> = { ...data, };
			if (name !== undefined) prototype.name = name;
			if (type !== undefined) prototype.type = type;
			const listed = parseBooleanOption(flags["listed"], "--listed",);
			if (listed !== undefined) prototype.listed = listed;
			const params = optionalJsonFlag(flags, "params",);
			if (params !== undefined) prototype.params = params;
			return {
				method: "POST",
				endpoint: projectEndpoint("/insights/",),
				identifiers: { name, type, },
				payload: {
					insightPrototype: prototype,
					contentType: flags["content-type"] as string | undefined,
					payload: textInput(flags,),
				},
			};
		}
		case "insight.update":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/insights/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
				payload: {
					insight: {
						...jsonInput(flags,),
						name: flags["name"] as string | undefined,
						listed: parseBooleanOption(flags["listed"], "--listed",),
						params: optionalJsonFlag(flags, "params",),
					},
					contentType: flags["content-type"] as string | undefined,
					payload: textInput(flags,),
				},
			};
		case "insight.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/insights/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
			};
		case "data-quality.create-rule":
			return {
				method: "POST",
				endpoint: dataQualityEndpoint(projectKey!, args[0], "/rules",),
				identifiers: { dataset: args[0], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "data-quality.update-rule":
			return {
				method: "PUT",
				endpoint: dataQualityEndpoint(projectKey!, args[0], `/rules/${encodeURIComponent(args[1],)}`,),
				identifiers: { dataset: args[0], ruleId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "data-quality.delete-rule":
			return {
				method: "DELETE",
				endpoint: dataQualityEndpoint(
					projectKey!,
					args[0],
					`/rules/${encodeURIComponent(args[1],)}${querySuffix({ ruleId: args[1], },)}`,
				),
				identifiers: { dataset: args[0], ruleId: args[1], },
			};
		case "data-quality.compute":
			return {
				method: "POST",
				endpoint: dataQualityEndpoint(
					projectKey!,
					args[0],
					`/actions/compute-rules${
						querySuffix({
							partition: (flags["partition"] as string | undefined) ?? "NP",
							ruleId: flags["rule-id"] as string | undefined,
						},)
					}`,
				),
				identifiers: { dataset: args[0], ruleId: flags["rule-id"] as string | undefined, },
				wait: flags["wait"] === true,
			};
		case "future.abort":
			return {
				method: "DELETE",
				endpoint: `/public/api/futures/${encodeURIComponent(id,)}`,
				identifiers: { id, },
			};
		case "flow-zone.create": {
			const name = flowZoneName(flags["name"],);
			const payload = { name, color: flowZoneColor(flags["color"],) ?? "#2ab1ac", };
			return {
				method: "POST",
				endpoint: projectEndpoint("/flow/zones",),
				identifiers: { name, },
				payload,
			};
		}
		case "flow-zone.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/flow/zones/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
				payload: {
					name: typeof flags["name"] === "string" ? flowZoneName(flags["name"],) : undefined,
					color: flowZoneColor(flags["color"],),
					projectKey,
				},
			};
		case "flow-zone.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/flow/zones/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
			};
		case "flow-zone.move":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/flow/zones/${encodeURIComponent(id,)}/add-items`,),
				identifiers: { id, },
				payload: flowZoneMoveItems(flags,),
			};
		case "dataset.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			const connection = flags["connection"] as string | undefined;
			const dsType = requiredPlanFlag(flags, "type", entry.usage,);
			if (!connection && dsType.toLowerCase() !== "uploadedfiles") {
				throw new UsageError("--connection is required unless --type is UploadedFiles.",);
			}
			const endpoint = projectEndpoint("/datasets/",); // throws without a project key
			return {
				method: "POST",
				endpoint,
				identifiers: { name, },
				payload: buildDatasetCreateBody({
					projectKey: projectKey!,
					datasetName: name,
					connection,
					dsType,
				},),
			};
		}
		case "dataset.clone": {
			const source = args[0];
			const target = args[1];
			return {
				exact: false,
				reason:
					"Apply GETs the source dataset and copies its current connection, format, and schema into the new dataset body.",
				method: "POST",
				endpoint: projectEndpoint("/datasets/",),
				identifiers: { source, target, },
				payload: {
					sourceDataset: source,
					targetDataset: target,
					path: flags["path"] as string | undefined,
					table: flags["table"] as string | undefined,
					metastoreTableName: flags["metastore-table"] as string | undefined,
					allowSamePath: flags["allow-same-path"] === true,
					projectKey,
				},
			};
		}
		case "dataset.rename":
			return {
				method: "POST",
				endpoint: projectEndpoint("/actions/renameDataset",),
				identifiers: { oldName: args[0], newName: args[1], },
				payload: { oldName: args[0], newName: args[1], },
			};
		case "dataset.upload-file": {
			const fileName = requiredPlanFlag(flags, "file-name", entry.usage,);
			const datasetEndpoint = projectEndpoint(`/datasets/${encodeURIComponent(args[0],)}`,);
			const filesEndpoint = `${datasetEndpoint}/uploaded/files`;
			const upload = {
				method: "POST",
				endpoint: filesEndpoint,
				payload: { ...uploadPayload(args[1],), fileName, },
			};
			return {
				...upload,
				identifiers: {
					datasetName: args[0],
					localPath: args[1],
					fileName,
				},
				requests: [
					{ sequence: 1, method: "GET", endpoint: datasetEndpoint, },
					{ sequence: 2, method: "GET", endpoint: filesEndpoint, },
					{ sequence: 3, ...upload, },
					{
						sequence: 4,
						method: "GET",
						endpoint: filesEndpoint,
						assert: {
							filename: fileName,
							length: "local file byte length",
						},
					},
				],
			};
		}
		case "dataset.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/datasets/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
			};
		case "dataset.refresh-schema": {
			const columns = schemaColumnsInput(flags, entry.usage,);
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/datasets/${encodeURIComponent(id,)}/schema`,),
				identifiers: { name: id, },
				payload: { columns, },
			};
		}
		case "dataset.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/datasets/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
			};
		case "dataset.metadata-set":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/datasets/${encodeURIComponent(id,)}/metadata`,),
				identifiers: { name: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "dataset.create-managed": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			const connection = requiredPlanFlag(flags, "connection", entry.usage,);
			const creationSettings: Record<string, unknown> = { connectionId: connection, };
			const specificSettings: Record<string, unknown> = {};
			if (typeof flags["type-option-id"] === "string") {
				creationSettings.typeOptionId = flags["type-option-id"];
			}
			if (typeof flags["format-option-id"] === "string") {
				specificSettings.formatOptionId = flags["format-option-id"];
			}
			if (typeof flags["copy-partitioning-from"] === "string") {
				specificSettings.partitioningOptionId = `copy:${
					flags["partitioning-folder"] === true ? "folder" : "dataset"
				}:${flags["copy-partitioning-from"]}`;
			}
			if (Object.keys(specificSettings,).length > 0) {
				creationSettings.specificSettings = specificSettings;
			}
			return {
				method: "POST",
				endpoint: projectEndpoint("/datasets/managed",),
				identifiers: { name, },
				payload: { name, creationSettings, },
			};
		}
		case "recipe.add-input":
		case "recipe.remove-input": {
			const role = (flags["role"] as string | undefined) ?? "main";
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(args[0],)}`,),
				identifiers: { recipe: args[0], dataset: args[1], role, },
				payload: {
					operation: action === "add-input" ? "append" : "remove",
					dataset: args[1],
					role,
					projectKey,
				},
			};
		}
		case "recipe.clone": {
			const positionalSource = args[0];
			const fromFlag = typeof flags["from"] === "string" ? flags["from"].trim() : "";
			const source = positionalSource ?? fromFlag;
			if (!source) {
				throw new UsageError(
					`Source recipe is required. Usage: ${entry.usage}`,
					"missing_required_flag",
				);
			}
			if (positionalSource && fromFlag && positionalSource !== fromFlag) {
				throw new UsageError(
					"Positional source and --from must match when both are provided.",
					"invalid_enum",
				);
			}
			const toFlag = typeof flags["to"] === "string" ? flags["to"].trim() : "";
			const nameFlag = typeof flags["name"] === "string" ? flags["name"].trim() : "";
			const target = toFlag || nameFlag;
			if (!target) {
				throw new UsageError(
					`--name or --to is required. Usage: ${entry.usage}`,
					"missing_required_flag",
				);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint("/recipes/",),
				identifiers: { source, target, },
				payload: {
					sourceRecipe: source,
					targetRecipe: target,
					inputRewrites: rewritePairsFromFlags(flags, "replace-input",),
					outputRewrites: rewritePairsFromFlags(flags, "replace-output",),
					payloadTextRewrites: rewritePairsFromFlags(flags, "replace-payload-text",),
					outputDataset: flags["output"] as string | undefined,
					copyOutputSettings: flags["copy-output-settings"] === true,
					outputPath: flags["path"] as string | undefined,
					metastoreTableName: flags["metastore-table"] as string | undefined,
					projectKey,
				},
			};
		}
		case "recipe.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
			};
		case "recipe.create": {
			requiredPlanFlag(flags, "type", entry.usage,);
			const outputDataset = flags["output"] as string | undefined;
			const outputFolder = flags["output-folder"] as string | undefined;
			if (outputDataset && outputFolder) {
				throw new UsageError("--output and --output-folder are mutually exclusive.",);
			}
			if (!outputDataset && !outputFolder) {
				throw new UsageError("--output or --output-folder is required.",);
			}
			if (outputFolder && !flags["output-connection"]) {
				throw new UsageError("--output-connection is required when using --output-folder.",);
			}
			const endpoint = projectEndpoint("/recipes/",); // throws without a project key
			// Same flag mapping and body construction as the handler and RecipesResource.create.
			const { recipePrototype, creationSettings, } = buildRecipeCreateRequest(
				recipeCreateOptionsFromFlags(flags,),
				projectKey!,
			);
			return {
				method: "POST",
				endpoint,
				identifiers: { name: recipePrototype.name as string, },
				payload: { recipePrototype, creationSettings, },
			};
		}
		case "recipe.run":
			return {
				exact: false,
				reason:
					"The job endpoint is exact, but DSS reads the recipe and its outputs before constructing the job payload; use recipe run --dry-run for a resolved payload.",
				method: "POST",
				endpoint: projectEndpoint("/jobs/",),
				identifiers: { recipe: id, },
				wait: recipeRunShouldWait(flags,),
			};
		case "code.run": {
			const { script, source, } = resolveCodeInputWithSource(args, flags,);
			const sourceSha256 = sha256Hex(script,);
			const scenarioBase = projectEndpoint("/scenarios/{generatedScenarioId}",);
			const envName = flags["env"] as string | undefined;
			const keepScenario = flags["keep"] === true;
			const timeoutMs = parseCodeRunIntegerFlag(flags["timeout"], "--timeout",) ?? 120_000;
			const maxLogBytes = flags["full-log"] === true
				? 0
				: parseCodeRunIntegerFlag(flags["max-log-bytes"], "--max-log-bytes",) ?? 1_048_576;
			return {
				method: "POST",
				endpoint: projectEndpoint("/scenarios/",),
				identifiers: {
					source,
					sourceSha256,
					sourceBytes: Buffer.byteLength(script,),
				},
				requests: [
					{
						method: "POST",
						endpoint: projectEndpoint("/scenarios/",),
						payload: {
							id: "{generatedScenarioId}",
							name: "dss code run ({generatedScenarioId})",
							projectKey,
							type: "custom_python",
							params: {
								envSelection: envName
									? { envMode: "EXPLICIT_ENV", envName, }
									: { envMode: "INHERIT", },
							},
						},
					},
					{
						method: "PUT",
						endpoint: `${scenarioBase}/payload`,
						payload: { extension: "py", script: "<omitted>", },
						redactedFields: ["payload.script",],
					},
					{ method: "POST", endpoint: `${scenarioBase}/run/`, payload: {}, },
					{
						method: "GET",
						endpoint:
							`${scenarioBase}/get-run-for-trigger?triggerId={triggerId}&triggerRunId={triggerRunId}`,
						repeat: "until scenarioRun.result.outcome or timeout",
					},
					{
						method: "GET",
						endpoint: `${scenarioBase}/{runId}/log`,
						boundedBytes: maxLogBytes,
					},
					...keepScenario ? [] : [{ method: "DELETE", endpoint: scenarioBase, cleanup: true, },],
				],
				wait: {
					timeoutMs,
					pollEndpoint: `${scenarioBase}/get-run-for-trigger`,
				},
			};
		}
		case "recipe.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "recipe.set-payload": {
			const file = requiredPlanFlag(flags, "file", entry.usage,);
			const backupDir = flags["no-backup"] === true
				? undefined
				: (flags["backup-dir"] as string | undefined)
					?? path.join(process.cwd(), ".dss-backups", "recipes",);
			const backupPath = backupDir ? recipeBackupPath(id, backupDir,) : undefined;
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: {
					file,
					content: textInput(flags,),
					...(backupPath ? { backupPath, } : {}),
				},
				...(backupPath
					? { localWrites: [{ path: backupPath, source: "remote recipe backup", before: "PUT", },], }
					: {}),
			};
		}
		case "job.build":
		case "job.build-and-wait":
			return {
				method: "POST",
				endpoint: projectEndpoint("/jobs/",),
				identifiers: { target: id, },
				payload: jobBuildPayload(id, projectKey!, flags,),
				wait: action === "build-and-wait" || flags["wait"] === true,
			};
		case "scenario.abort":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/abort`,),
				identifiers: { id, },
			};
		case "scenario.payload-set":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/payload`,),
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "scenario.active-set": {
			const activeRaw = args[1];
			if (activeRaw !== "true" && activeRaw !== "false") {
				throw new UsageError(`Usage: ${entry.usage}`,);
			}
			const lightEndpoint = projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/light`,);
			const active = activeRaw === "true";
			// DSS parses the light PUT as a full Scenario, so the command echoes the
			// current light status with `active` overridden (see ScenariosResource.setActive).
			return {
				method: "PUT",
				endpoint: lightEndpoint,
				identifiers: { id, },
				payload: { active, },
				requests: [
					{ sequence: 1, method: "GET", endpoint: lightEndpoint, },
					{
						sequence: 2,
						method: "PUT",
						endpoint: lightEndpoint,
						payload: { "...current": true, active, },
					},
					{ sequence: 3, method: "GET", endpoint: lightEndpoint, },
				],
			};
		}
		case "folder.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			const type = flags["type"] as string | undefined;
			const connection = flags["connection"] as string | undefined;
			const pathFlag = flags["path"] as string | undefined;
			const body = {
				name,
				projectKey,
				type: type ?? null,
				params: { connection, path: pathFlag?.trim() || "/${projectKey}/${odbId}", },
			};
			// Mirrors FoldersResource.create.
			if (connection === undefined) {
				return {
					exact: false,
					reason:
						"Without --connection, apply reads DSS admin settings to pick the managed-folder connection (falling back to filesystem_folders); params.connection is set then.",
					method: "POST",
					endpoint: projectEndpoint("/managedfolders/",),
					identifiers: { name, },
					payload: body,
				};
			}
			return {
				method: "POST",
				endpoint: projectEndpoint("/managedfolders/",),
				identifiers: { name, },
				payload: body,
			};
		}
		case "job.abort":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/jobs/${encodeURIComponent(id,)}/abort/`,),
				identifiers: { id, },
			};
		case "scenario.run":
		case "scenario.run-and-wait":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/run/`,),
				identifiers: { id, },
				payload: {},
				wait: action === "run-and-wait" || flags["wait"] === true,
			};
		case "scenario.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
			};
		case "scenario.create": {
			const type = (flags["type"] as string | undefined) ?? "step_based";
			return {
				method: "POST",
				endpoint: projectEndpoint("/scenarios/",),
				identifiers: { id: args[0], name: args[1], },
				// Mirrors ScenariosResource.create's default params for step-based scenarios.
				payload: {
					id: args[0],
					name: args[1],
					projectKey,
					type,
					params: type === "step_based" ? { steps: [], triggers: [], reporters: [], } : {},
				},
			};
		}
		case "scenario.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "folder.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/managedfolders/${encodeURIComponent(id,)}`,),
				identifiers: { folder: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "folder.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/managedfolders/${encodeURIComponent(id,)}`,),
				identifiers: { folder: id, },
			};
		case "folder.upload":
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/managedfolders/${encodeURIComponent(args[0],)}/contents/${encodeURIComponent(args[1],)}`,
				),
				identifiers: { folder: args[0], path: args[1], localPath: args[2], },
			};
		case "folder.delete-file":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(
					`/managedfolders/${encodeURIComponent(args[0],)}/contents/${encodeURIComponent(args[1],)}`,
				),
				identifiers: { folder: args[0], path: args[1], },
			};
		case "variable.set":
			return {
				exact: false,
				reason:
					"The payload lists the requested changes; apply PUTs the full {standard, local} object (merged with the current variables unless --replace).",
				method: "PUT",
				endpoint: projectEndpoint("/variables/",),
				payload: {
					standard: optionalJsonFlag(flags, "standard",),
					local: optionalJsonFlag(flags, "local",),
					replace: flags["replace"] === true,
				},
			};
		case "project-deployer.create-project": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/project-deployer/projects",
				identifiers: { projectKey: payload.projectKey, id: payload.id, },
				payload,
			};
		}
		case "project-deployer.upload-bundle":
			return {
				method: "POST",
				endpoint: "/public/api/project-deployer/projects/bundles",
				identifiers: { filePath: id, },
				payload: uploadPayload(id,),
			};
		case "project-deployer.create-deployment": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/project-deployer/deployments",
				identifiers: { deploymentId: payload.deploymentId ?? payload.id, },
				payload,
			};
		}
		case "project-deployer.save-deployment-settings":
			return {
				method: "PUT",
				endpoint: `/public/api/project-deployer/deployments/${encodeURIComponent(id,)}/settings`,
				identifiers: { deploymentId: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "project-deployer.deploy":
			return {
				method: "POST",
				endpoint: `/public/api/project-deployer/deployments/${encodeURIComponent(id,)}/actions/update`,
				identifiers: { deploymentId: id, },
				payload: {},
			};
		case "project-deployer.delete-deployment":
			return {
				method: "DELETE",
				endpoint: `/public/api/project-deployer/deployments/${encodeURIComponent(id,)}`,
				identifiers: { deploymentId: id, },
			};
		case "project-deployer.create-infra": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/project-deployer/infras",
				identifiers: { infraId: payload.id, },
				payload,
			};
		}
		case "api-deployer.create-infra": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/api-deployer/infras",
				identifiers: { infraId: payload.id, },
				payload,
			};
		}
		case "api-deployer.delete-infra":
			return {
				method: "DELETE",
				endpoint: `/public/api/api-deployer/infras/${encodeURIComponent(id,)}`,
				identifiers: { infraId: id, },
			};
		case "api-deployer.create-service": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/api-deployer/services",
				identifiers: { serviceId: payload.id ?? payload.publishedServiceId, },
				payload,
			};
		}
		case "api-deployer.delete-service":
			return {
				method: "DELETE",
				endpoint: `/public/api/api-deployer/services/${encodeURIComponent(id,)}`,
				identifiers: { serviceId: id, },
			};
		case "api-deployer.publish-version":
			return {
				method: "POST",
				endpoint: `/public/api/api-deployer/services/${encodeURIComponent(args[0],)}/versions`,
				identifiers: { serviceId: args[0], filePath: args[1], },
				payload: uploadPayload(args[1],),
			};
		case "api-deployer.delete-version":
			return {
				method: "DELETE",
				endpoint: `/public/api/api-deployer/services/${encodeURIComponent(args[0],)}/versions/${
					encodeURIComponent(args[1],)
				}`,
				identifiers: { serviceId: args[0], version: args[1], },
			};
		case "api-deployer.create-deployment": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/api-deployer/deployments",
				identifiers: { deploymentId: payload.deploymentId ?? payload.id, },
				payload,
			};
		}
		case "api-deployer.save-deployment-settings":
			return {
				method: "PUT",
				endpoint: `/public/api/api-deployer/deployments/${encodeURIComponent(id,)}/settings`,
				identifiers: { deploymentId: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "api-deployer.deploy":
			return {
				method: "POST",
				endpoint: `/public/api/api-deployer/deployments/${encodeURIComponent(id,)}/actions/update`,
				identifiers: { deploymentId: id, },
				payload: {},
			};
		case "api-deployer.delete-deployment":
			return {
				method: "DELETE",
				endpoint: `/public/api/api-deployer/deployments/${encodeURIComponent(id,)}`,
				identifiers: { deploymentId: id, },
			};
		case "workspace.create": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/workspaces/",
				identifiers: { workspaceKey: payload.workspaceKey, },
				payload,
			};
		}
		case "workspace.update-settings":
			return {
				method: "PUT",
				endpoint: `/public/api/workspaces/${encodeURIComponent(id,)}`,
				identifiers: { workspaceKey: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "workspace.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/workspaces/${encodeURIComponent(id,)}`,
				identifiers: { workspaceKey: id, },
			};
		case "workspace.add-object":
			return {
				method: "POST",
				endpoint: `/public/api/workspaces/${encodeURIComponent(id,)}/objects`,
				identifiers: { workspaceKey: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "meaning.create": {
			const body = jsonInput(flags,) ?? {};
			const payload = {
				...body,
				id: args[0],
				label: args[1],
				type: args[2],
				description: body.description ?? null,
				entries: body.entries ?? null,
				mappings: body.mappings ?? null,
				pattern: body.pattern ?? null,
				normalizationMode: body.normalizationMode ?? null,
				detectable: body.detectable ?? false,
			};
			return {
				method: "POST",
				endpoint: "/public/api/meanings/",
				identifiers: { id: args[0], },
				payload,
			};
		}
		case "meaning.update":
			return {
				method: "PUT",
				endpoint: `/public/api/meanings/${encodeURIComponent(id,)}`,
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "meaning.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/meanings/${encodeURIComponent(id,)}`,
				identifiers: { id, },
			};
		case "llm.list": {
			const params = new URLSearchParams();
			const purpose = flags["purpose"];
			if (typeof purpose === "string" && purpose.trim() !== "") {
				params.set("purpose", purpose.trim(),);
			}
			const query = params.size > 0 ? `?${params.toString()}` : "";
			return {
				method: "GET",
				endpoint: projectEndpoint(`/llms/${query}`,),
			};
		}
		case "llm.completions":
		case "llm.embeddings": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			const llmId = stringField(payload, ["llmId",],);
			if (llmId === undefined || llmId.trim() === "") {
				throw new UsageError(
					`llmId is required and must be a non-empty string (e.g. "openai:openai1:gpt-4").\nUsage: ${entry.usage}`,
					"invalid_flag_value",
				);
			}
			const queries = payload["queries"];
			if (!Array.isArray(queries,) || queries.length === 0) {
				throw new UsageError(
					`queries is required and must be a non-empty array of query objects.\nUsage: ${entry.usage}`,
					"invalid_flag_value",
				);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(
					action === "completions" ? "/llms/completions" : "/llms/embeddings",
				),
				identifiers: { llmId, },
				payload,
			};
		}
		case "knowledge-bank.search": {
			if (typeof id !== "string" || id.trim() === "") {
				throw new UsageError(
					`knowledgeBankId must be a non-empty string.\nUsage: ${entry.usage}`,
					"invalid_flag_value",
				);
			}
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			const query = stringField(payload, ["query",],);
			if (query === undefined || query.trim() === "") {
				throw new UsageError(
					`query is required and must be a non-empty string.\nUsage: ${entry.usage}`,
					"invalid_flag_value",
				);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/knowledge-banks/${encodeURIComponent(id,)}/search`,
				),
				identifiers: { knowledgeBankId: id, },
				payload,
			};
		}
		case "knowledge-bank.clear":
			if (typeof id !== "string" || id.trim() === "") {
				throw new UsageError(
					`knowledgeBankId must be a non-empty string.\nUsage: ${entry.usage}`,
					"invalid_flag_value",
				);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/knowledge-banks/${encodeURIComponent(id,)}/clear`,
				),
				identifiers: { knowledgeBankId: id, },
			};
		case "data-collection.create":
			return {
				method: "POST",
				endpoint: "/public/api/data-collections/",
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "data-collection.settings-set":
			return {
				method: "PUT",
				endpoint: `/public/api/data-collections/${encodeURIComponent(id,)}`,
				identifiers: { dataCollectionId: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "data-collection.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/data-collections/${encodeURIComponent(id,)}`,
				identifiers: { dataCollectionId: id, },
			};
		case "data-collection.add-object": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			const cid = id
				?? (typeof payload["dataCollectionId"] === "string"
					? payload["dataCollectionId"] as string
					: undefined);
			if (!cid) throw new UsageError(`Usage: ${entry.usage}`,);
			const reference = { ...payload, };
			delete (reference as Record<string, unknown>)["dataCollectionId"];
			return {
				method: "POST",
				endpoint: `/public/api/data-collections/${encodeURIComponent(cid,)}/objects`,
				identifiers: { dataCollectionId: cid, },
				payload: reference,
			};
		}
		case "data-collection.remove-dataset": {
			const projectKeyArg = args[1];
			const datasetName = args[2];
			if (!projectKeyArg || !datasetName) {
				throw new UsageError(`Usage: ${entry.usage}`,);
			}
			return {
				method: "DELETE",
				endpoint: `/public/api/data-collections/${encodeURIComponent(id,)}/objects/dataset/${
					encodeURIComponent(projectKeyArg,)
				}/${encodeURIComponent(datasetName,)}`,
				identifiers: { dataCollectionId: id, projectKey: projectKeyArg, datasetName, },
			};
		}
		case "project.tags-set":
			return {
				method: "PUT",
				endpoint: encodedProjectEndpointForPlan(
					projectKey ?? requiredPlanProjectKey(flags, entry.usage,),
					"/tags",
				),
				identifiers: { projectKey: projectKey ?? requiredPlanProjectKey(flags, entry.usage,), },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "project.metadata-set":
			return {
				method: "PUT",
				endpoint: encodedProjectEndpointForPlan(
					projectKey ?? requiredPlanProjectKey(flags, entry.usage,),
					"/metadata",
				),
				identifiers: { projectKey: projectKey ?? requiredPlanProjectKey(flags, entry.usage,), },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "macro.run":
		case "macro.run-and-wait": {
			// The plan payload mirrors the live POST body exactly: params and
			// adminParams when provided via --data, {} otherwise. Validation is
			// identical to the live path (JSON object shape), zero network.
			const data = optionalJsonFlag(flags, "data",);
			const runPayload: Record<string, unknown> = {};
			if (data && typeof data === "object" && !Array.isArray(data,)) {
				if (data["params"] !== undefined) runPayload["params"] = data["params"];
				if (data["adminParams"] !== undefined) runPayload["adminParams"] = data["adminParams"];
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(`/runnables/${encodeURIComponent(id,)}?wait=false`,),
				identifiers: { id, },
				payload: runPayload,
				wait: action === "macro.run-and-wait" || flags["wait"] === true,
			};
		}
		case "macro.abort": {
			const runId = args[1];
			if (!runId) throw new UsageError(`Usage: ${entry.usage}`,);
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/runnables/${encodeURIComponent(id,)}/abort/${encodeURIComponent(runId,)}`,
				),
				identifiers: { id, runId, },
			};
		}
		case "recipe.metadata-set":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(id,)}/metadata`,),
				identifiers: { recipeName: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "user.create":
			return {
				method: "POST",
				endpoint: "/public/api/admin/users",
				payload: redactedUserPayload(requiredPlanJsonInput(flags, entry.usage,),),
			};
		case "group.create":
			return {
				method: "POST",
				endpoint: "/public/api/admin/groups",
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "saved-model.create-external": {
			const config = optionalJsonFlag(flags, "configuration",) ?? optionalJsonFlag(flags, "data",);
			return {
				method: "POST",
				endpoint: projectEndpoint("/savedmodels/",),
				identifiers: { name: id, },
				payload: {
					savedModelType: requiredPlanFlag(flags, "type", entry.usage,),
					name: id,
					...(typeof flags["prediction-type"] === "string"
						? { predictionType: flags["prediction-type"], }
						: {}),
					...(config ? { proxyModelConfiguration: config, } : {}),
				},
			};
		}
		case "saved-model.update-settings":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/savedmodels/${encodeURIComponent(id,)}`,),
				identifiers: { savedModelId: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "saved-model.delete-versions": {
			const versionsRaw = args[1];
			if (typeof versionsRaw !== "string" || versionsRaw.trim() === "") {
				throw new UsageError(`Usage: ${entry.usage}`,);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(`/savedmodels/${encodeURIComponent(id,)}/actions/delete-versions`,),
				identifiers: { savedModelId: id, },
				payload: {
					versions: versionsRaw.split(",",).map((v,) => v.trim()).filter((v,) => v.length > 0),
					removeIntermediate: parseBooleanOption(
						flags["remove-intermediate"],
						"--remove-intermediate",
					) ?? true,
				},
			};
		}
		case "saved-model.import-mlflow-version":
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/savedmodels/${encodeURIComponent(id,)}/versions/${encodeURIComponent(args[1] ?? "",)}`,
				),
				identifiers: { savedModelId: id, versionId: args[1], },
				payload: {
					source: { kind: "local-archive", archive: requiredPlanFlag(flags, "archive", entry.usage,), },
					...(typeof flags["code-env"] === "string"
						? { codeEnvName: flags["code-env"], }
						: {}),
					// Execute always sends the query parameter with the
					// external-caller default NONE; the plan mirrors it.
					containerExecConfigName: typeof flags["container-exec-config"] === "string"
						? flags["container-exec-config"]
						: "NONE",
					setActive: parseBooleanOption(flags["set-active"], "--set-active",) ?? true,
					...(flags["binary-classification-threshold"] !== undefined
						? {
							binaryClassificationThreshold: num(
								flags["binary-classification-threshold"],
								"--binary-classification-threshold",
							),
						}
						: {}),
				},
			};
		case "saved-model.import-mlflow-version-from-folder":
			return {
				exact: false,
				reason:
					"Apply sends these values as query parameters (with codeEnvName and binaryClassificationThreshold defaults) on an empty multipart body; the payload shows them structured.",
				method: "POST",
				endpoint: projectEndpoint(
					`/savedmodels/${encodeURIComponent(id,)}/versions/${encodeURIComponent(args[1] ?? "",)}`,
				),
				identifiers: { savedModelId: id, versionId: args[1], },
				payload: {
					source: {
						kind: "managed-folder",
						folderRef: requiredPlanFlag(flags, "folder", entry.usage,),
						path: flags["path"] as string | undefined,
					},
					...(typeof flags["code-env"] === "string" ? { codeEnvName: flags["code-env"], } : {}),
					// Execute always sends the query parameter with the
					// external-caller default NONE; the plan mirrors it.
					containerExecConfigName: typeof flags["container-exec-config"] === "string"
						? flags["container-exec-config"]
						: "NONE",
					setActive: parseBooleanOption(flags["set-active"], "--set-active",) ?? true,
				},
			};
		case "saved-model.external-metadata-put": {
			// DSS requires containerExecConfigName on this endpoint; for an
			// external API caller it resolves as LOCAL-CONFIG -> NONE, so the
			// plan mirrors the NONE default unless --container-exec-config
			// overrides it. Same contract as execute.
			const containerExecConfigName = typeof flags["container-exec-config"] === "string"
				? flags["container-exec-config"]
				: "NONE";
			return {
				method: "PUT",
				endpoint: projectEndpoint(
					`/savedmodels/${encodeURIComponent(id,)}/versions/${
						encodeURIComponent(args[1] ?? "",)
					}/external-ml/metadata?containerExecConfigName=${
						encodeURIComponent(containerExecConfigName,)
					}`,
				),
				identifiers: { savedModelId: id, versionId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		}
		case "saved-model.evaluate-version": {
			const payload: Record<string, unknown> = {
				datasetRef: requiredPlanFlag(flags, "dataset", entry.usage,),
				// Execute always sends an explicit containerExecConfigName
				// (official external-caller semantics: LOCAL-CONFIG -> NONE);
				// the plan mirrors that default and any explicit override.
				containerExecConfigName: typeof flags["container-exec-config"] === "string"
					? flags["container-exec-config"]
					: "NONE",
			};
			// --sampling is a JSON flag value; the plan carries the parsed
			// object under the same key as the execute body (samplingParam).
			if (flags["sampling"] !== undefined) {
				payload.samplingParam = json(flags["sampling"], "--sampling",);
			}
			return {
				method: "POST",
				endpoint: `${
					projectEndpoint(
						`/savedmodels/${encodeURIComponent(id,)}/versions/${
							encodeURIComponent(args[1] ?? "",)
						}/external-ml/actions/evaluate`,
					)
				}?useOptimalThreshold=${
					parseBooleanOption(flags["use-optimal-threshold"], "--use-optimal-threshold",) ?? true
				}&skipExpensiveReports=${
					parseBooleanOption(flags["skip-expensive-reports"], "--skip-expensive-reports",) ?? true
				}`,
				identifiers: { savedModelId: id, versionId: args[1], },
				payload,
			};
		}
		case "saved-model.set-user-meta":
			return {
				method: "PUT",
				endpoint: projectEndpoint(
					`/savedmodels/${encodeURIComponent(id,)}/versions/${
						encodeURIComponent(args[1] ?? "",)
					}/user-meta`,
				),
				identifiers: { savedModelId: id, versionId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "user.update":
			return {
				method: "PUT",
				endpoint: `/public/api/admin/users/${encodeURIComponent(id,)}`,
				identifiers: { login: id, },
				payload: redactedUserPayload(requiredPlanJsonInput(flags, entry.usage,),),
			};
		case "user.delete":
		case "group.delete":
			return {
				method: "DELETE",
				endpoint: resource === "user"
					? `/public/api/admin/users/${encodeURIComponent(id,)}`
					: `/public/api/admin/groups/${encodeURIComponent(id,)}`,
				identifiers: resource === "user" ? { login: id, } : { name: id, },
			};
		case "user.resync":
			return {
				method: "POST",
				endpoint: `/public/api/admin/users/${encodeURIComponent(id,)}/actions/resync`,
				identifiers: { login: id, },
			};
		case "user.resync-multi": {
			const logins = requiredPlanCsv(flags, "logins", entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/admin/users/actions/resync-multi",
				identifiers: { count: logins.length, },
				payload: logins,
			};
		}
		case "user.external-users":
			return {
				method: "GET",
				endpoint: "/public/api/admin/users/actions/external-users",
			};
		case "user.external-groups":
			return {
				method: "GET",
				endpoint: "/public/api/admin/users/actions/external-groups",
			};
		case "connection.test":
			return {
				method: "GET",
				endpoint: `/public/api/connections/${encodeURIComponent(id,)}/test`,
				identifiers: { connectionName: id, },
			};
		case "user.provision":
			return {
				method: "POST",
				endpoint: "/public/api/admin/users/actions/provision",
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "group.update":
			return {
				method: "PUT",
				endpoint: `/public/api/admin/groups/${encodeURIComponent(id,)}`,
				identifiers: { name: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "connection.create":
			return {
				method: "POST",
				endpoint: "/public/api/admin/connections",
				payload: redactedConnectionPayload(requiredPlanJsonInput(flags, entry.usage,),),
			};
		case "connection.update":
			return {
				method: "PUT",
				endpoint: `/public/api/admin/connections/${encodeURIComponent(id,)}`,
				identifiers: { connectionName: id, },
				payload: redactedConnectionPayload(requiredPlanJsonInput(flags, entry.usage,),),
			};
		case "connection.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/admin/connections/${encodeURIComponent(id,)}`,
				identifiers: { connectionName: id, },
			};
		case "connection.prepare-import": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			if (!projectKey) throw new UsageError(`Missing project key for ${resource} ${action}.`,);
			return {
				method: "POST",
				endpoint: encodedProjectEndpointForPlan(
					projectKey,
					"/datasets/tables-import/actions/prepare-from-keys",
				),
				identifiers: { projectKey, },
				payload,
			};
		}
		case "connection.execute-import": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			if (!projectKey) throw new UsageError(`Missing project key for ${resource} ${action}.`,);
			return {
				method: "POST",
				endpoint: encodedProjectEndpointForPlan(
					projectKey,
					"/datasets/tables-import/actions/execute-from-candidates",
				),
				identifiers: { projectKey, },
				payload,
			};
		}
		case "plugin.install-from-zip":
			return {
				method: "POST",
				endpoint: "/public/api/plugins/actions/installFromZip",
				payload: { file: requiredPlanFlag(flags, "file", entry.usage,), upload: "multipart", },
			};
		case "plugin.install-from-store":
			return {
				method: "POST",
				endpoint: "/public/api/plugins/actions/installFromStore",
				identifiers: { pluginId: id, },
				payload: { pluginId: id, },
			};
		case "plugin.install-from-git": {
			const payload = pluginGitPlanPayload(flags,);
			return {
				method: "POST",
				endpoint: "/public/api/plugins/actions/installFromGit",
				payload,
			};
		}
		case "plugin.update-from-zip":
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, "updateFromZip",),
				identifiers: { pluginId: id, },
				payload: { file: requiredPlanFlag(flags, "file", entry.usage,), upload: "multipart", },
			};
		case "plugin.update-from-store":
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, "updateFromStore",),
				identifiers: { pluginId: id, },
			};
		case "plugin.update-from-git":
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, "updateFromGit",),
				identifiers: { pluginId: id, },
				payload: pluginGitPlanPayload(flags,),
			};
		case "plugin.settings-set": {
			const scope = optionalPlanProjectScope(flags,);
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/settings",)}${scope}`,
				identifiers: { pluginId: id, },
				payload: { configKeys: settingsConfigKeys(flags,), },
			};
		}
		case "plugin.code-env-create": {
			const conda = parseBooleanOption(flags["conda"], "--conda",) ?? false;
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/code-env/actions/create",)}`,
				identifiers: { pluginId: id, },
				payload: {
					deploymentMode: "PLUGIN_MANAGED",
					conda,
					pythonInterpreter: flags["python-interpreter"] ?? null,
				},
				wait: flags["wait"] === true,
			};
		}
		case "plugin.code-env-update":
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/code-env/actions/update",)}`,
				identifiers: { pluginId: id, },
				wait: flags["wait"] === true,
			};
		case "plugin.move-to-dev":
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, "moveToDev",),
				identifiers: { pluginId: id, },
			};
		case "plugin.delete":
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, "delete",),
				identifiers: { pluginId: id, },
				payload: { force: parseBooleanOption(flags["force"], "--force",) ?? false, },
			};
		case "plugin.create-dev": {
			const creationMode = requiredPlanFlag(flags, "creation-mode", entry.usage,);
			const needsGit = creationMode !== "EMPTY";
			// Mirrors PluginsResource.createDev: git fields are always present, null when unused.
			const payload: Record<string, unknown> = {
				pluginId: id,
				creationMode,
				gitRepository: needsGit && typeof flags["repository"] === "string"
					? validatedPlanRepositoryUrl(flags["repository"], "repository", "--repository URL",)
					: null,
				gitCheckout: needsGit && typeof flags["checkout"] === "string" ? flags["checkout"] : null,
				gitSubpath: creationMode === "GIT_EXPORT" && typeof flags["path-in-repository"] === "string"
					? flags["path-in-repository"]
					: null,
			};
			return {
				method: "POST",
				endpoint: "/public/api/plugins/actions/createDev",
				identifiers: { pluginId: id, creationMode, },
				payload,
			};
		}
		case "plugin.set-git-remote": {
			const repository = requiredPlanFlag(flags, "repository", entry.usage,);
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/gitRemote",)}`,
				identifiers: { pluginId: id, },
				payload: { repositoryUrl: validatedPlanRepositoryUrl(repository, "repository", entry.usage,), },
			};
		}
		case "plugin.delete-git-remote":
			return {
				method: "DELETE",
				endpoint: pluginRootEndpoint(id, "/gitRemote",),
				identifiers: { pluginId: id, },
			};
		case "plugin.push":
		case "plugin.pull":
		case "plugin.fetch":
		case "plugin.reset-local":
		case "plugin.reset-remote": {
			const actionMap: Record<string, string> = {
				"plugin.push": "push",
				"plugin.pull": "pullRebase",
				"plugin.fetch": "fetch",
				"plugin.reset-local": "resetToLocalHeadState",
				"plugin.reset-remote": "resetToRemoteHeadState",
			};
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, actionMap[`${resource}.${action}`]!,),
				identifiers: { pluginId: id, },
			};
		}
		case "plugin.contents-put":
			return {
				method: "POST",
				endpoint: pluginContentsEndpoint(id, args[1] ?? "",),
				identifiers: { pluginId: id, path: args[1], },
				payload: { contentSource: contentSourceKind(flags,), },
			};
		case "plugin.contents-delete":
			return {
				method: "DELETE",
				endpoint: pluginContentsEndpoint(id, args[1] ?? "",),
				identifiers: { pluginId: id, path: args[1], },
			};
		case "plugin.folder-add":
			return {
				method: "POST",
				endpoint: pluginFoldersEndpoint(id, args[1] ?? "",),
				identifiers: { pluginId: id, path: args[1], },
			};
		case "plugin.rename": {
			if (!args[2]) throw new UsageError(`Usage: ${entry.usage}`,);
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/contents-actions/rename",)}`,
				identifiers: { pluginId: id, path: args[1], },
				payload: { oldPath: `/${validatePluginPath(args[1] ?? "",)}`, newName: args[2], },
			};
		}
		case "plugin.move": {
			const destination = args[2];
			if (!destination) throw new UsageError(`Usage: ${entry.usage}`,);
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/contents-actions/move",)}`,
				identifiers: { pluginId: id, path: args[1], },
				payload: {
					oldPath: `/${validatePluginPath(args[1] ?? "",)}`,
					newPath: validatePluginDestinationPath(destination,),
				},
			};
		}
		case "business-app.save-settings":
			return {
				method: "PUT",
				endpoint: `/public/api/business-apps/${encodeURIComponent(id,)}/settings`,
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "business-app.create-instance":
			return {
				method: "POST",
				endpoint: `/public/api/business-apps/${encodeURIComponent(id,)}/instances`,
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "business-app.upgrade-instance":
			return {
				method: "POST",
				endpoint: `/public/api/business-apps/${encodeURIComponent(args[0],)}/instances/${
					encodeURIComponent(args[1],)
				}/upgrade`,
				identifiers: { id: args[0], projectKey: args[1], },
				payload: {},
			};
		case "business-app.install-from-archive":
			return {
				method: "POST",
				endpoint: "/public/api/business-apps/install-from-archive",
				identifiers: { filePath: id, },
				payload: uploadPayload(id,),
			};
		case "app.save-instance-manifest": {
			const targetProjectKey = requiredPlanProjectKey(flags, entry.usage,);
			return {
				method: "PUT",
				endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/app-manifest`,
				identifiers: { projectKey: targetProjectKey, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		}
		case "app.create-instance": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			if (payload["targetProjectKey"] === undefined) {
				// Generated-at-apply mode: --plan never allocates or reserves a
				// random key. The payload stays exactly as supplied, the runtime
				// generates the key (and defaults the display name to it) during
				// apply, and no absence probe runs for a generated key, so the
				// plan carries no preflight requests and no fake concrete GET
				// path: `{targetProjectKey}` resolves at apply time.
				return {
					exact: false,
					reason:
						"targetProjectKey (and targetProjectName when omitted) are generated during apply; the sent body adds them.",
					method: "POST",
					endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances`,
					identifiers: {
						appId: id,
						targetProjectKeyGeneratedDuringApply: true,
						...(payload["targetProjectName"] === undefined
							? { targetProjectNameGeneratedDuringApply: true, }
							: {}),
						preflightExecuted: false,
						preflightWillRunDuringApply: false,
						preflightRequests: [],
						incarnationControl: "client-side-non-atomic-future-target-and-creation-tag-join",
						incarnationObservationRequests: [
							{
								method: "GET",
								endpointTemplate: "/public/api/projects/{targetProjectKey}/",
								when: flags["wait"] === true
									? "after-terminal-future-target"
									: "conditional-inline-hasResult-target",
								intent:
									"After DSS reports inline or terminal creation success for the generated key, observe creationTag for later cleanup binding.",
							},
						],
						note:
							"Generated-at-apply: a random APP_ targetProjectKey is generated client-side during apply (never at plan time); targetProjectName defaults to it when omitted. No absent-project preflight runs for a generated key.",
					},
					payload,
					wait: flags["wait"] === true,
				};
			}
			const rawTargetProjectKey = stringField(payload, ["targetProjectKey",],);
			if (!rawTargetProjectKey || rawTargetProjectKey.trim() === "") {
				throw new UsageError(
					"Instance creation payload must include a non-empty targetProjectKey.",
					"validation_failed",
					`Usage: ${entry.usage}`,
				);
			}
			// The runtime trims and rewrites body.targetProjectKey; the plan
			// advertises the normalized identifier and payload.
			const targetProjectKey = rawTargetProjectKey.trim();
			const normalizedPayload = { ...payload, targetProjectKey, };
			return {
				method: "POST",
				endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances`,
				identifiers: {
					appId: id,
					targetProjectKey,
					preflightExecuted: false,
					preflightWillRunDuringApply: true,
					incarnationControl: "client-side-non-atomic-future-target-and-creation-tag-join",
					incarnationObservationRequests: [
						{
							method: "GET",
							endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
							when: flags["wait"] === true
								? "after-terminal-future-target"
								: "conditional-inline-hasResult-target",
							intent:
								"After DSS reports inline or terminal creation success for the requested key, observe creationTag for later cleanup binding.",
						},
					],
					preflightRequests: [
						{
							method: "GET",
							endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
							when: "before-create",
							intent: "Require the target project to be absent before creating the instance.",
						},
						{
							method: "GET",
							endpoint: "/public/api/projects/",
							when: "conditional",
							intent:
								"Fallback list probe issued only when the direct project GET is forbidden (403). Presence proves a collision; absence cannot prove availability.",
						},
						{
							method: "GET",
							endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances/`,
							when: "conditional",
							intent:
								"Fallback app-instance list probe issued when the direct project GET is forbidden (403). Presence proves a collision; absence still rejects creation as unverifiable.",
						},
					],
					note:
						"Strict preflight: the instance POST runs only after the payload targetProjectKey is confirmed absent. Creation never writes into an existing or unprovable project.",
				},
				payload: normalizedPayload,
				wait: flags["wait"] === true,
			};
		}
		case "app.set-manifest-version": {
			const targetProjectKey = requiredPlanProjectKey(flags, entry.usage,);
			const payloadPatch: Record<string, unknown> = {};
			const version = flags["manifest-version"] as string | undefined;
			if (version !== undefined) {
				if (version.trim() === "") {
					throw new UsageError(
						"App manifest version must be a non-empty string.",
						"validation_failed",
						`Usage: ${entry.usage}`,
					);
				}
				payloadPatch.version = version;
			}
			const versionNotes = flags["version-notes"] as string | undefined;
			if (versionNotes !== undefined) payloadPatch.versionNotes = versionNotes;
			if (version === undefined && versionNotes === undefined) {
				throw new UsageError(
					"At least one of --manifest-version or --version-notes is required.",
					"usage_error",
					`Usage: ${entry.usage}`,
				);
			}
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError(
					"Expected manifest hash must be a 64-character SHA-256 hex digest.",
					"validation_failed",
					`Usage: ${entry.usage}`,
				);
			}
			return {
				method: "PUT",
				endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/app-manifest`,
				identifiers: {
					projectKey: targetProjectKey,
					payloadPatch,
					...(expectHash !== undefined ? { expectHash: expectHash.toLowerCase(), } : {}),
					concurrencyControl: APP_MANIFEST_CONCURRENCY_CONTROL,
					staleReadCheck: expectHash === undefined
						? "none"
						: "client-side-expect-hash-compare-before-put",
					note: expectHash === undefined
						? "Unconditional PUT: no stale-read check is armed because --expect-hash was not supplied."
						: "The hash is compared client-side against a fresh read; the PUT itself stays unconditional, so this command can overwrite a writer that commits between that read and this PUT without detecting the lost update.",
				},
			};
		}
		case "app.create-successor-instance": {
			const sourceProjectKey = requiredPlanFlag(flags, "from", entry.usage,);
			const targetProjectKey = optionalPlanFlag(flags, "to", entry.usage,);
			if (targetProjectKey !== undefined && sourceProjectKey === targetProjectKey) {
				throw new UsageError(
					"--from and --to must be different project keys.",
					"validation_failed",
					`Usage: ${entry.usage}`,
				);
			}
			const targetProjectName = flags["name"] as string | undefined;
			const copyPermissions = parseBooleanOption(flags["copy-permissions"], "--copy-permissions",)
				?? false;
			if (targetProjectKey === undefined) {
				// Generated-at-apply mode: --plan never allocates a key and no
				// target exists yet, so the plan makes no target-absence claim
				// and advertises no target probe. The key is generated once
				// during apply, immediately before the single instance POST;
				// `{targetProjectKey}` resolves only after the terminal future
				// names it. Source and template gates still run before the POST.
				return {
					method: "POST",
					endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances`,
					identifiers: {
						appId: id,
						sourceProjectKey,
						targetProjectKeyGeneratedDuringApply: true,
						targetPreflight: "not-applicable-generated-key",
						preflightExecuted: false,
						preflightWillRunDuringApply: true,
						...(targetProjectName !== undefined
							? { targetProjectName, }
							: { targetProjectNameGeneratedDuringApply: true, }),
						copyPermissions,
						incarnationControl: "client-side-non-atomic-future-target-and-creation-tag-join",
						incarnationObservationRequests: [
							{
								method: "GET",
								endpointTemplate: "/public/api/projects/{targetProjectKey}/",
								when: "after-terminal-future-target",
								intent:
									"After the terminal future names the generated successor key, observe creationTag and bind later target checks and cleanup to that hash.",
							},
						],
						preflightRequests: [
							{
								method: "GET",
								endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances/`,
								when: "before-create",
								intent: "Verify the --from project is a registered instance of the app.",
							},
							{
								method: "GET",
								endpoint: `/public/api/projects/${encodeURIComponent(sourceProjectKey,)}/app-manifest`,
								when: "before-create",
								intent: "Verify the --from project is an APP_INSTANCE project.",
							},
						],
						...(copyPermissions
							? {
								permissionConcurrencyControl: "client-side-non-atomic-stale-identity-and-hash-checks",
								permissionRequests: [
									{
										method: "GET",
										endpoint: `/public/api/projects/${encodeURIComponent(sourceProjectKey,)}/permissions`,
										intent: "Snapshot the predecessor instance ACL before creation.",
									},
									{
										method: "GET",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/permissions",
										intent: "Read the generated successor ACL to decide whether the copy is a no-op.",
									},
									{
										method: "GET",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/",
										intent:
											"Recheck generated successor creationTag after reading its ACL; stop if the project key was reused.",
									},
									{
										method: "GET",
										endpoint: `/public/api/projects/${encodeURIComponent(sourceProjectKey,)}/permissions`,
										intent:
											"Recheck the predecessor ACL immediately before the write and stop if its hash drifted.",
									},
									{
										method: "GET",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/",
										intent:
											"Recheck generated successor creationTag immediately before the unconditional permission PUT.",
									},
									{
										method: "PUT",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/permissions",
										intent: "Apply the predecessor ACL snapshot to the generated successor instance.",
									},
									{
										method: "GET",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/permissions",
										intent: "Verify the generated successor ACL hash equals the predecessor snapshot hash.",
									},
									{
										method: "GET",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/",
										intent:
											"Detect generated successor project-key reuse across the permission write and verification read.",
									},
								],
							}
							: {}),
						note:
							"Generated-at-apply: the successor key is generated once during apply (never at plan time) and no absent-project preflight runs for a generated key, so the plan claims no target absence. Source and template gates run before the single instance POST, then the terminal future's own result names the successor key. The predecessor is never modified or deleted; cleanup targets the generated successor key only.",
					},
					payload: targetProjectName !== undefined ? { targetProjectName, } : {},
					wait: true,
				};
			}
			return {
				method: "POST",
				endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances`,
				identifiers: {
					appId: id,
					sourceProjectKey,
					targetProjectKey,
					preflightExecuted: false,
					preflightWillRunDuringApply: true,
					...(targetProjectName !== undefined ? { targetProjectName, } : {}),
					copyPermissions,
					incarnationControl: "client-side-non-atomic-future-target-and-creation-tag-join",
					postFutureRequests: [
						{
							method: "GET",
							endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
							intent:
								"After the terminal future names the successor key, observe creationTag and bind later target checks and cleanup to that hash.",
						},
					],
					...(copyPermissions
						? {
							permissionConcurrencyControl: "client-side-non-atomic-stale-identity-and-hash-checks",
						}
						: {}),
					preflightRequests: [
						{
							method: "GET",
							endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances/`,
							when: "before-create",
							intent: "Verify the --from project is a registered instance of the app.",
						},
						{
							method: "GET",
							endpoint: `/public/api/projects/${encodeURIComponent(sourceProjectKey,)}/app-manifest`,
							when: "before-create",
							intent: "Verify the --from project is an APP_INSTANCE project.",
						},
						{
							method: "GET",
							endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
							when: "before-create",
							intent: "Require the --to target project to be absent before creating the successor.",
						},
						{
							method: "GET",
							endpoint: "/public/api/projects/",
							when: "conditional",
							intent:
								"Fallback list probe issued only when the direct project GET is forbidden (403). Presence proves a collision; absence cannot prove availability and rejects creation.",
						},
					],
					...(copyPermissions
						? {
							permissionRequests: [
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(sourceProjectKey,)}/permissions`,
									intent: "Snapshot the predecessor instance ACL before creation.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/permissions`,
									intent: "Read the successor ACL to decide whether the copy is a no-op.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
									intent:
										"Recheck successor creationTag after reading its ACL; stop if the project key was reused.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(sourceProjectKey,)}/permissions`,
									intent:
										"Recheck the predecessor ACL immediately before the write and stop if its hash drifted.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
									intent:
										"Recheck successor creationTag immediately before the unconditional permission PUT.",
								},
								{
									method: "PUT",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/permissions`,
									intent: "Apply the predecessor ACL snapshot to the successor instance.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/permissions`,
									intent: "Verify the successor ACL hash equals the predecessor snapshot hash.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
									intent:
										"Detect successor project-key reuse across the permission write and verification read.",
								},
							],
						}
						: {}),
					note:
						"Additive, non-transactional: strict preflight verifies the predecessor instance and the --to target absence before the single instance POST, then waits on the DSS future. The predecessor is never modified or deleted; cleanup targets only the successor key. DSS exposes no immutable future target ID, conditional DELETE, or conditional permission PUT: later creationTag and ACL checks narrow and detect races but cannot atomically join creation provenance or serialize writes.",
				},
				payload: {
					targetProjectKey,
					targetProjectName: targetProjectName ?? targetProjectKey,
				},
				wait: true,
			};
		}
		case "app.delete-instance": {
			const targetProjectKey = requiredPlanFlag(flags, "project-key", entry.usage,);
			const futureId = flags["future-id"] === undefined
				? undefined
				: requiredPlanFlag(flags, "future-id", entry.usage,);
			const expectedProjectIncarnation = flags["expect-project-incarnation"] === undefined
				? undefined
				: requiredPlanFlag(flags, "expect-project-incarnation", entry.usage,);
			if (
				expectedProjectIncarnation !== undefined
				&& !/^[0-9a-f]{64}$/.test(expectedProjectIncarnation,)
			) {
				throw new UsageError(
					"--expect-project-incarnation must be a 64-character lowercase SHA-256 hash.",
					"validation_failed",
				);
			}
			const unconfirmedCreation = parseBooleanOption(
				flags["unconfirmed-creation"],
				"--unconfirmed-creation",
			) ?? false;
			const manifestProbe = `/public/api/projects/${
				encodeURIComponent(targetProjectKey,)
			}/app-manifest`;
			const projectDetailsProbe = `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`;
			const typeValidationRequests = [
				{
					method: "GET",
					endpoint: manifestProbe,
					when: "before-delete",
					intent: "Verify the target is an APP_INSTANCE project before deleting.",
				},
				{
					method: "GET",
					endpoint: projectDetailsProbe,
					when: expectedProjectIncarnation === undefined
						? "conditional-type-check-before-delete"
						: "incarnation-and-conditional-type-check-before-delete",
					intent: expectedProjectIncarnation === undefined
						? "Fallback probe issued only when the app-manifest response omits projectAppType (live DSS does)."
						: "Recompute the current project-incarnation hash from creationTag after the manifest probe; the same response supplies projectAppType when the manifest omits it.",
				},
			];
			const preflightRequests = typeValidationRequests;
			if (unconfirmedCreation) {
				return {
					identifiers: {
						projectKey: targetProjectKey,
						...(futureId !== undefined ? { futureId, } : {}),
						unconfirmedCreation: true,
						note:
							"Indeterminate creation without a DSS future ID: no DSS request is issued. The command reports an unresolved cleanup failure (exit 4, cleanupResolved false) without deleting, because creation may still be running.",
					},
				};
			}
			if (futureId === undefined) {
				return {
					method: "DELETE",
					endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}`,
					identifiers: {
						projectKey: targetProjectKey,
						...(expectedProjectIncarnation === undefined
							? {}
							: {
								projectIncarnationGate: {
									required: false,
									provided: true,
									expectedHash: expectedProjectIncarnation,
								},
								incarnationControl: "client-side-non-atomic-stale-identity-check",
							}),
						preflightRequests,
						note: expectedProjectIncarnation === undefined
							? "Convergent direct delete: the manifest preflight rejects non-instance targets, an absent target (404) is an already-absent success issued without any DELETE, and only a verified instance target receives the project DELETE. No project-incarnation binding was requested."
							: "Incarnation-bound direct delete: after verifying APP_INSTANCE type, a project GET must match the expected creationTag hash before an unconditional DELETE. DSS exposes no immutable project ID or conditional DELETE, so this client-side check narrows but cannot serialize against project-key reuse after the GET.",
					},
				};
			}
			return {
				method: "DELETE",
				endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}`,
				identifiers: {
					projectKey: targetProjectKey,
					futureId,
					projectIncarnationGate: {
						required: true,
						provided: expectedProjectIncarnation !== undefined,
						...(expectedProjectIncarnation === undefined
							? {}
							: { expectedHash: expectedProjectIncarnation, }),
					},
					incarnationControl: "client-side-non-atomic-future-target-and-creation-tag-join",
					preflightRequests: [
						{
							method: "GET",
							endpoint: manifestProbe,
							when: "before-future-wait",
							intent:
								"Verify the target is an APP_INSTANCE project before the supplied future is touched, so an invalid target cannot affect it.",
						},
						{
							method: "GET",
							endpoint: projectDetailsProbe,
							when: "conditional-before-wait",
							intent:
								"Fallback probe issued only when the pre-wait app-manifest response omits projectAppType (live DSS does).",
						},
					],
					futureGate: [
						{
							method: "GET",
							endpoint: `/public/api/futures/${encodeURIComponent(futureId,)}?peek=false`,
							intent:
								"Wait for the supplied creation future to settle or time out; never abort it, and repeat this read-only GET while it is live.",
						},
					],
					postFutureValidationRequests: expectedProjectIncarnation === undefined
						? []
						: typeValidationRequests,
					note: expectedProjectIncarnation === undefined
						? "The target type is verified before the future is touched. Waiting never aborts the future, but a terminal target match still cannot authorize deletion without --expect-project-incarnation; the command then exits with validation_failed and issues no DELETE."
						: "The target type is verified before the future is touched. The DELETE runs only after the terminal future reports the requested target, then a later project GET still matches the recorded creationTag hash and the target is re-verified as APP_INSTANCE. DSS exposes no immutable future target ID or conditional DELETE: the future target and creationTag are independent, non-atomic observations that narrow but cannot eliminate a project-key-reuse race.",
				},
			};
		}
		case "app.permissions-restore": {
			const file = requiredPlanFlag(flags, "file", entry.usage,);
			const targetProjectKey = requiredPlanProjectKey(flags, entry.usage,);
			const permissionsEndpoint = `/public/api/projects/${
				encodeURIComponent(targetProjectKey,)
			}/permissions`;
			const projectDetailsEndpoint = `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`;
			return {
				method: "PUT",
				endpoint: permissionsEndpoint,
				identifiers: {
					file,
					projectKey: targetProjectKey,
					localPreflight: [
						"Read and hash-verify the owner-only snapshot file.",
						"Require its project key and canonical DSS URL to match this invocation.",
					],
					preflightRequests: [
						{
							method: "GET",
							endpoint: projectDetailsEndpoint,
							intent: "Require the snapshot project-incarnation hash to match creationTag.",
						},
						{
							method: "GET",
							endpoint: permissionsEndpoint,
							intent: "Read current permissions; an equal hash makes the PUT unnecessary.",
						},
						{
							method: "GET",
							endpoint: projectDetailsEndpoint,
							intent:
								"Recheck the project incarnation after the permission read and immediately before any PUT.",
						},
					],
					conditionalWrite: {
						method: "PUT",
						endpoint: permissionsEndpoint,
						when: "permissions-differ-and-dry-run-is-false",
					},
					verificationRequests: [
						{
							method: "GET",
							endpoint: permissionsEndpoint,
							intent: "Verify DSS persisted the desired permission hash.",
						},
						{
							method: "GET",
							endpoint: projectDetailsEndpoint,
							intent: "Detect project-key reuse across the permission write.",
						},
					],
					incarnationControl: "client-side-non-atomic-stale-identity-check",
					note:
						"DSS exposes no conditional permission PUT or immutable project ID. The repeated creationTag checks narrow and detect key-reuse races but cannot serialize the check with the PUT.",
				},
			};
		}
		case "statistics.create-worksheet":
			return {
				method: "POST",
				endpoint: statisticsWorksheetsEndpoint(args[0],),
				identifiers: { dataset: args[0], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "statistics.update-worksheet":
			return {
				method: "PUT",
				endpoint: statisticsWorksheetEndpoint(args[0], args[1],),
				identifiers: { dataset: args[0], worksheetId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "statistics.delete-worksheet":
			return {
				method: "DELETE",
				endpoint: statisticsWorksheetEndpoint(args[0], args[1],),
				identifiers: { dataset: args[0], worksheetId: args[1], },
			};
		case "statistics.run-worksheet":
			return {
				method: "POST",
				endpoint: `${statisticsWorksheetEndpoint(args[0], args[1],)}/actions/run-card`,
				identifiers: { dataset: args[0], worksheetId: args[1], },
			};
		case "statistics.run-card":
			return {
				method: "POST",
				endpoint: `${statisticsWorksheetEndpoint(args[0], args[1],)}/actions/run-card`,
				identifiers: { dataset: args[0], worksheetId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "statistics.run-computation":
			return {
				method: "POST",
				endpoint: `${statisticsWorksheetEndpoint(args[0], args[1],)}/actions/run-computation`,
				identifiers: { dataset: args[0], worksheetId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "webapp.create":
			return {
				method: "POST",
				endpoint: projectEndpoint("/webapps/",),
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "webapp.update-settings": {
			const patch = requiredPlanJsonInput(flags, entry.usage,);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError("Expected webapp hash must be a 64-character SHA-256 hex digest.",);
			}
			const endpoint = projectEndpoint(`/webapps/${encodeURIComponent(id,)}`,);
			return {
				exact: false,
				reason: "Apply GETs the current settings, deep-merges the patch, then PUTs the full object.",
				method: "PUT",
				endpoint,
				identifiers: {
					webappId: id,
					patchHash: stableHash(patch,),
					...(expectHash ? { expectHash: expectHash.toLowerCase(), } : {}),
				},
				payload: { patch: "<omitted>", patchHash: stableHash(patch,), },
				requests: [
					{ method: "GET", endpoint, purpose: "read full settings and verify expectHash", },
					{
						method: "PUT",
						endpoint,
						condition: "settings read and expected hash matched",
						payload: "<merged full settings>",
					},
				],
			};
		}
		case "webapp.stop-backend":
			return {
				method: "PUT",
				endpoint: projectEndpoint(
					`/webapps/${encodeURIComponent(id,)}/backend/actions/stop`,
				),
				identifiers: { webappId: id, },
				payload: {},
			};
		case "webapp.restart-backend": {
			const endpoint = projectEndpoint(
				`/webapps/${encodeURIComponent(id,)}/backend/actions/restart`,
			);
			return {
				method: "PUT",
				endpoint,
				identifiers: { webappId: id, },
				payload: {},
				wait: {
					requested: flags["wait"] === true,
					timeoutMs: num(flags["timeout"], "--timeout",),
					pollIntervalMs: num(flags["poll-interval"], "--poll-interval",),
				},
			};
		}
		case "api-service.create":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/apiservices/${encodeURIComponent(id,)}`,),
				identifiers: { serviceId: id, },
				payload: {},
			};
		case "api-service.save-settings": {
			const settings = requiredPlanJsonInput(flags, entry.usage,);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError(
					"Expected API service settings hash must be a 64-character SHA-256 hex digest.",
				);
			}
			const endpoint = projectEndpoint(
				`/apiservices/${encodeURIComponent(id,)}/settings`,
			);
			const payload = {
				settings: "<omitted>",
				settingsHash: stableHash(settings,),
				...(expectHash ? { expectHash: expectHash.toLowerCase(), } : {}),
			};
			return {
				method: "PUT",
				endpoint,
				identifiers: { serviceId: id, },
				payload,
				requests: [
					...(expectHash
						? [{ method: "GET", endpoint, purpose: "verify expectHash", },]
						: []),
					{
						method: "PUT",
						endpoint,
						condition: expectHash ? "hash matched" : "unconditional",
						payload,
					},
				],
			};
		}
		case "api-service.add-prediction-endpoint": {
			const endpoint = projectEndpoint(
				`/apiservices/${encodeURIComponent(id,)}/settings`,
			);
			return {
				exact: false,
				reason: "Apply GETs the full settings, appends one endpoint, then PUTs the full object.",
				method: "PUT",
				endpoint,
				identifiers: { serviceId: id, endpointId: args[1], savedModelId: args[2], },
				payload: { id: args[1], type: "STD_PREDICTION", modelRef: args[2], },
				requests: [
					{ method: "GET", endpoint, purpose: "read full settings", },
					{ method: "PUT", endpoint, payload: "<merged full settings>", },
				],
			};
		}
		case "api-service.create-package":
		case "api-service.delete-package":
		case "api-service.publish-package": {
			const serviceId = args[0];
			const packageId = args[1];
			const packageEndpoint = projectEndpoint(
				`/apiservices/${encodeURIComponent(serviceId,)}/packages/${encodeURIComponent(packageId,)}`,
			);
			if (action === "delete-package") {
				return {
					method: "DELETE",
					endpoint: packageEndpoint,
					identifiers: { serviceId, packageId, },
				};
			}
			if (action === "publish-package") {
				return {
					method: "POST",
					endpoint: `${packageEndpoint}/publish${
						querySuffix({
							publishedServiceId: flags["published-service-id"] as string | undefined,
						},)
					}`,
					identifiers: { serviceId, packageId, },
				};
			}
			return {
				method: "POST",
				endpoint: packageEndpoint + querySuffix({
					releaseNotes: flags["release-notes"] as string | undefined,
				},),
				identifiers: { serviceId, packageId, },
			};
		}
		case "bundle.export": {
			const evaluateProjectStandardsChecks = parseBooleanOption(
				flags["evaluate-standards-checks"],
				"--evaluate-standards-checks",
			) ?? true;
			return {
				method: "PUT",
				endpoint: projectEndpoint("/bundles/exported/" + encodeURIComponent(id,),) + querySuffix({
					releaseNotes: flags["release-notes"] as string | undefined,
					evaluateProjectStandardsChecks,
				},),
				identifiers: { bundleId: id, },
			};
		}
		case "bundle.publish":
			return {
				method: "POST",
				endpoint: projectEndpoint("/bundles/" + encodeURIComponent(id,) + "/publish",) + querySuffix({
					publishedProjectKey: flags["published-project-key"] as string | undefined,
				},),
				identifiers: { bundleId: id, },
				payload: {},
			};
		case "bundle.activate": {
			const rawScenarios = flags["scenarios"];
			let scenariosToEnable: Record<string, boolean> | undefined;
			if (rawScenarios !== undefined && rawScenarios !== false) {
				const parsed = json(rawScenarios,);
				if (
					typeof parsed !== "object"
					|| parsed === null
					|| Array.isArray(parsed,)
					|| !Object.values(parsed,).every((value,) => typeof value === "boolean")
				) {
					throw new UsageError("--scenarios must be a JSON object mapping scenario IDs to true|false.",);
				}
				scenariosToEnable = parsed as Record<string, boolean>;
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/bundles/imported/${encodeURIComponent(id,)}/actions/activate`,
				),
				identifiers: { bundleId: id, },
				payload: scenariosToEnable === undefined
					? {}
					: { scenariosActiveOnActivation: scenariosToEnable, },
			};
		}
		case "bundle.preload":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/bundles/imported/${encodeURIComponent(id,)}/actions/preload`,),
				identifiers: { bundleId: id, },
				payload: {},
			};
		case "project-library.create-file":
		case "project-library.create-folder": {
			const libraryPath = validateLibraryPath(id,);
			const kind = action === "create-file" ? "contents" : "folders";
			return {
				exact: false,
				reason:
					"Apply performs a live absence check before POST; --if-not-exists may stop after that read, and create never overwrites an existing item.",
				method: "POST",
				endpoint: projectEndpoint(`/libraries/${kind}/${encodeLibraryPath(libraryPath,)}`,),
				identifiers: { path: libraryPath, },
			};
		}
		case "project-library.put": {
			const libraryPath = validateLibraryPath(id,);
			const endpoint = projectEndpoint(`/libraries/contents/${encodeLibraryPath(libraryPath,)}`,);
			const payload = projectLibraryPutPayload(flags,);
			return {
				method: "POST",
				endpoint,
				identifiers: {
					path: libraryPath,
					concurrencyControl: payload.expectSha256
						? PROJECT_LIBRARY_CONCURRENCY_CONTROL
						: "none",
				},
				payload,
				...(payload.expectSha256
					? {
						requests: [
							{
								method: "GET",
								endpoint: `${endpoint}?dataEncoding=base64`,
								purpose: "verify expectSha256",
							},
							{ method: "POST", endpoint, condition: "hash matched", payload, },
						],
					}
					: {}),
			};
		}
		case "project-library.delete": {
			const libraryPath = validateLibraryPath(id,);
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/libraries/contents/${encodeLibraryPath(libraryPath,)}`,),
				identifiers: { path: libraryPath, },
			};
		}
		case "project-library.rename": {
			const libraryPath = validateLibraryPath(id,);
			const newName = validateLibraryName(args[1],);
			return {
				method: "POST",
				endpoint: projectEndpoint("/libraries/contents-actions/rename/",),
				identifiers: { path: libraryPath, newName, },
				payload: { oldPath: `/${libraryPath}`, newName, },
			};
		}
		case "project-library.move": {
			const libraryPath = validateLibraryPath(id,);
			const destinationFolder = validateLibraryDestinationPath(args[1],);
			return {
				method: "POST",
				endpoint: projectEndpoint("/libraries/contents-actions/move",),
				identifiers: { path: libraryPath, destinationFolder, },
				payload: { oldPath: `/${libraryPath}`, newPath: destinationFolder, },
			};
		}
		case "code-env.create": {
			const wait = codeEnvWait(flags,);
			return {
				method: "POST",
				endpoint: `${codeEnvEndpoint()}?wait=${wait}`,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				payload: {
					...codeEnvParams(flags,),
					deploymentMode: requiredPlanFlag(flags, "deployment-mode", entry.usage,),
				},
				wait,
			};
		}
		case "code-env.set-definition": {
			const definition = requiredPlanJsonInput(flags, entry.usage,);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError("--expect-hash must be a 64-character SHA-256 hex digest.",);
			}
			const endpoint = codeEnvEndpoint();
			const definitionHash = stableHash(definition,);
			const normalizedExpectHash = expectHash?.toLowerCase();
			return {
				exact: false,
				reason:
					"Apply PUTs the supplied definition object unchanged; this plan omits that body and reports only its hash.",
				method: "PUT",
				endpoint,
				identifiers: {
					lang: codeEnvLang(args[0], entry.usage,),
					name: args[1],
					definitionHash,
					...(normalizedExpectHash ? { expectHash: normalizedExpectHash, } : {}),
				},
				payload: "<omitted>",
				...(normalizedExpectHash
					? {
						requests: [
							{ method: "GET", endpoint, purpose: "verify expectHash", },
							{
								method: "PUT",
								endpoint,
								condition: "hash matched",
								payload: "<omitted>",
								redactedFields: ["payload",],
							},
						],
					}
					: {}),
			};
		}
		case "code-env.set-packages": {
			const installCorePackages = parseBooleanOption(
				flags["install-core-packages"],
				"--install-core-packages",
			);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError("--expect-hash must be a 64-character SHA-256 hex digest.",);
			}
			const endpoint = codeEnvEndpoint();
			return {
				exact: false,
				reason:
					"Apply GETs the full definition, merges only package fields, then PUTs the full result.",
				method: "PUT",
				endpoint,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				payload: {
					packages: codeEnvPackageList(flags,),
					...(installCorePackages !== undefined ? { installCorePackages, } : {}),
					...(expectHash ? { expectHash: expectHash.toLowerCase(), } : {}),
				},
				requests: [
					{ method: "GET", endpoint, purpose: "read full definition and verify expectHash", },
					{
						method: "PUT",
						endpoint,
						condition: "definition read and hash matched",
						payload: "<merged full definition>",
					},
				],
			};
		}
		case "code-env.update-packages": {
			const versionToUpdate = typeof flags["env-version"] === "string"
				? flags["env-version"]
				: typeof flags["version"] === "string"
				? flags["version"]
				: undefined;
			const wait = codeEnvWait(flags,);
			return {
				method: "POST",
				endpoint: `${codeEnvEndpoint("/packages",)}${
					querySuffix({
						forceRebuildEnv: flags["force-rebuild"] === true,
						versionToUpdate,
						wait,
					},)
				}`,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				wait,
			};
		}
		case "code-env.update-images": {
			const wait = codeEnvWait(flags,);
			return {
				method: "POST",
				endpoint: `${codeEnvEndpoint("/images",)}${
					querySuffix({ envVersion: flags["env-version"] as string | undefined, wait, },)
				}`,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				wait,
			};
		}
		case "code-env.set-jupyter": {
			const active = parseBooleanOption(flags["active"], "--active",);
			if (active === undefined) {
				throw new UsageError(
					`--active is required. Usage: ${commandUsage("code-env", "set-jupyter",)}`,
				);
			}
			const wait = codeEnvWait(flags,);
			return {
				method: "POST",
				endpoint: `${codeEnvEndpoint("/jupyter",)}${querySuffix({ active, wait, },)}`,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				wait,
			};
		}
		case "code-env.delete": {
			const wait = codeEnvWait(flags,);
			return {
				method: "DELETE",
				endpoint: `${codeEnvEndpoint()}?wait=${wait}`,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				wait,
			};
		}
		case "notebook.save-jupyter": {
			const content = requiredPlanJsonInput(flags, entry.usage,);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError("Expected notebook hash must be a 64-character SHA-256 hex digest.",);
			}
			const endpoint = projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}`,);
			return {
				exact: false,
				reason:
					"A fresh GET selects POST when absent or PUT when present; a confirming GET supplies the persisted hash.",
				endpoint,
				identifiers: {
					name: id,
					contentHash: stableHash(content,),
					...(expectHash ? { expectHash: expectHash.toLowerCase(), } : {}),
				},
				requests: [
					{ method: "GET", endpoint, purpose: "resolve create/update and check --expect-hash", },
					{
						method: "POST|PUT",
						endpoint,
						condition: "POST after not_found; otherwise PUT",
						payload: "<omitted>",
						redactedFields: ["payload",],
					},
					{ method: "GET", endpoint, purpose: "confirm persisted hash", },
				],
			};
		}
		case "notebook.delete-jupyter":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
			};
		case "notebook.clear-jupyter-outputs":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}/outputs`,),
				identifiers: { name: id, },
			};
		case "notebook.unload-jupyter":
			if (flags["all"] === true) {
				if (args.length > 0) throw new UsageError(entry.usage,);
				return {
					exact: false,
					reason:
						"DSS has no unload-all endpoint; apply lists active notebooks and sessions, then issues one verified session DELETE per result.",
					identifiers: { all: true, },
					requests: [
						{ method: "GET", endpoint: projectEndpoint("/jupyter-notebooks/?active=true",), },
						{
							method: "GET",
							endpoint: projectEndpoint("/jupyter-notebooks/{name}/sessions",),
							forEach: "active notebook",
						},
						{
							method: "DELETE",
							endpoint: projectEndpoint("/jupyter-notebooks/{name}/sessions/{sessionId}",),
							forEach: "listed session",
						},
					],
				};
			}
			requireArgs(args, 2, entry.usage,);
			return {
				method: "DELETE",
				endpoint: projectEndpoint(
					`/jupyter-notebooks/${encodeURIComponent(args[0],)}/sessions/${encodeURIComponent(args[1],)}`,
				),
				identifiers: { name: args[0], sessionId: args[1], },
			};
		case "notebook.save-sql": {
			const content = requiredPlanJsonInput(flags, entry.usage,);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError("Expected notebook hash must be a 64-character SHA-256 hex digest.",);
			}
			const endpoint = projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}`,);
			return {
				exact: false,
				reason:
					"A fresh GET selects collection POST when absent or entity PUT when present; a confirming GET supplies the persisted hash.",
				endpoint,
				identifiers: {
					id,
					contentHash: stableHash(content,),
					...(expectHash ? { expectHash: expectHash.toLowerCase(), } : {}),
				},
				requests: [
					{ method: "GET", endpoint, purpose: "resolve create/update and check --expect-hash", },
					{
						method: "POST",
						endpoint: projectEndpoint("/sql-notebooks/",),
						condition: "after not_found",
						payload: "<omitted; includes id and projectKey>",
						redactedFields: ["payload",],
					},
					{
						method: "PUT",
						endpoint,
						condition: "when present",
						payload: "<omitted>",
						redactedFields: ["payload",],
					},
					{ method: "GET", endpoint, purpose: "confirm persisted hash", },
				],
			};
		}
		case "notebook.delete-sql":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
			};
		case "notebook.clear-sql-history":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}/history/clear`,),
				identifiers: { id, },
				payload: {
					cellId: flags["cell-id"] as string | undefined,
					numRunsToRetain: num(flags["retain"], "--retain",),
				},
			};
		case "project.create": {
			const settings = jsonInput(flags,) ?? null;
			return {
				method: "POST",
				endpoint: "/public/api/projects/",
				identifiers: { projectKey: args[0], name: args[1], },
				payload: {
					projectKey: args[0],
					name: args[1],
					owner: (flags["owner"] as string | undefined) ?? null,
					settings,
					description: null,
					permissions: [],
					tags: [],
				},
			};
		}
		case "project.delete": {
			const expectedProjectIncarnation = flags["expect-project-incarnation"] === undefined
				? undefined
				: requiredPlanFlag(flags, "expect-project-incarnation", entry.usage,);
			if (
				expectedProjectIncarnation !== undefined
				&& !/^[0-9a-f]{64}$/.test(expectedProjectIncarnation,)
			) {
				throw new UsageError(
					"--expect-project-incarnation must be a 64-character lowercase SHA-256 hash.",
					"validation_failed",
				);
			}
			const endpoint = `/public/api/projects/${encodeURIComponent(id,)}${
				querySuffix({
					clearManagedDatasets: flags["drop-data"] === true,
					clearOutputManagedFolders: false,
					clearJobAndScenarioLogs: true,
					wait: true,
				},)
			}`;
			const guarded = flags["if-exists"] === true
				|| executionMode(flags,).dryRun
				|| expectedProjectIncarnation !== undefined;
			if (!guarded) {
				return {
					method: "DELETE",
					endpoint,
					identifiers: { projectKey: id, },
				};
			}
			const projectProbe = `/public/api/projects/${encodeURIComponent(id,)}/`;
			const incarnationGate = expectedProjectIncarnation === undefined
				? { required: false, provided: false, }
				: {
					required: true,
					provided: true,
					expectedHash: expectedProjectIncarnation,
				};
			if (executionMode(flags,).dryRun) {
				return {
					method: "GET",
					endpoint: projectProbe,
					identifiers: {
						projectKey: id,
						dryRun: true,
						ifExists: flags["if-exists"] === true,
						projectIncarnationGate: incarnationGate,
						note:
							"Read-only guarded-delete preflight. No DELETE is issued; a supplied creationTag hash must match.",
					},
				};
			}
			return {
				method: "DELETE",
				endpoint,
				identifiers: {
					projectKey: id,
					ifExists: flags["if-exists"] === true,
					projectIncarnationGate: incarnationGate,
					incarnationControl: expectedProjectIncarnation === undefined
						? "none"
						: "client-side-non-atomic-stale-identity-check",
					preflightRequests: [{
						method: "GET",
						endpoint: projectProbe,
						intent: expectedProjectIncarnation === undefined
							? "Confirm existence before a convergent delete."
							: "Recompute the current project-incarnation hash from creationTag before DELETE.",
					},],
					note: expectedProjectIncarnation === undefined
						? "Convergent delete: a 404 preflight is an already-absent success only with --if-exists."
						: "Incarnation-bound delete: the GET must match the expected creationTag hash before an unconditional DELETE. DSS exposes no conditional project DELETE, so the client-side gate cannot serialize against key reuse after the GET.",
				},
			};
		}
		case "project.duplicate": {
			const options = jsonInput(flags,);
			return {
				method: "POST",
				endpoint: `/public/api/projects/${encodeURIComponent(args[0],)}/duplicate/`,
				identifiers: { sourceKey: args[0], targetKey: args[1], targetName: args[2], },
				payload: {
					targetProjectName: args[2],
					targetProjectKey: args[1],
					duplicationMode: (options?.duplicationMode as string | undefined) ?? "MINIMAL",
					exportAnalysisModels: (options?.exportAnalysisModels as boolean | undefined) ?? true,
					exportSavedModels: (options?.exportSavedModels as boolean | undefined) ?? true,
					exportGitRepository: options?.exportGitRepository ?? null,
					exportInsightsData: (options?.exportInsightsData as boolean | undefined) ?? true,
					remapping: options?.remapping ?? {},
					...(options?.targetProjectFolderId !== undefined
						? { targetProjectFolderId: options.targetProjectFolderId, }
						: {}),
				},
			};
		}
		case "project.import": {
			const settings = jsonInput(flags,) ?? {};
			const rawTarget = flags["target-project-key"] as string | undefined;
			const targetProjectKey = rawTarget?.trim();
			if (rawTarget !== undefined && targetProjectKey === "") {
				throw new UsageError(
					`--target-project-key must not be empty. Usage: ${entry.usage}`,
				);
			}
			const settingsTarget = settings.targetProjectKey;
			if (
				settingsTarget !== undefined
				&& (typeof settingsTarget !== "string" || settingsTarget.trim() === "")
			) {
				throw new UsageError(
					`targetProjectKey in import settings must be a non-empty string. Usage: ${entry.usage}`,
				);
			}
			if (
				targetProjectKey !== undefined
				&& settingsTarget !== undefined
				&& targetProjectKey !== settingsTarget.trim()
			) {
				throw new UsageError(
					`--target-project-key conflicts with targetProjectKey in import settings. Usage: ${entry.usage}`,
				);
			}
			const processPayload = targetProjectKey === undefined
				? settings
				: { ...settings, targetProjectKey, };
			const finalizedPayload = Object.keys(processPayload,).length === 0
				? { _: "_", }
				: processPayload;
			const upload = {
				method: "POST",
				endpoint: "/public/api/projects/import/upload",
				payload: {
					contentType: "multipart/form-data",
					fileField: "file",
					filePath: id,
					fileName: "tmp-import.zip",
				},
			};
			return {
				...upload,
				identifiers: {
					filePath: id,
					...(targetProjectKey ? { targetProjectKey, } : {}),
					archivePreflight: {
						local: true,
						required: true,
						checks: [
							"zip-integrity",
							"safe-unique-members",
							"manifest",
							"flow-references",
							"orphan-members",
						],
					},
					successVerification: {
						usedProjectKeyRequired: true,
						projectReadRequired: true,
						projectIncarnationRequired: true,
						remappingReported: true,
					},
					...(flags["record-cleanup"] === undefined
						? {}
						: {
							cleanupBinding: "actual used project key plus verified creationTag incarnation hash",
						}),
				},
				requests: [
					{ sequence: 1, ...upload, },
					{
						sequence: 2,
						method: "POST",
						endpoint: "/public/api/projects/import/{importId}/process",
						pathBindings: { importId: "requests[0].response.id", },
						payload: finalizedPayload,
					},
				],
			};
		}
		case "project.settings-set":
			return {
				method: "PUT",
				endpoint: projectEndpoint("/settings",),
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "project.permissions-set":
			return {
				method: "PUT",
				endpoint: projectEndpoint("/permissions",),
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "continuous-activity.start":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/continuous-activities/${encodeURIComponent(id,)}/start`,),
				identifiers: { recipeId: id, },
				payload: jsonInput(flags,) ?? {},
			};
		case "continuous-activity.stop":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/continuous-activities/${encodeURIComponent(id,)}/stop`,),
				identifiers: { recipeId: id, },
			};
		case "metrics.dataset-compute":
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/datasets/${encodeURIComponent(id,)}/actions/computeMetrics?partition=`,
				),
				identifiers: { dataset: id, },
			};
		case "flow-zone.organize":
			return {
				method: "POST",
				endpoint: projectEndpoint("/flow/zones",),
				payload: jsonInput(flags,),
			};
		/* ---- project-git: mutations only; reads are never planned ---- */
		case "project-git.set-remote": {
			const remote = (flags["name"] as string | undefined) ?? "origin";
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/remotes/${encodeURIComponent(remote,)}`,
				),
				identifiers: { remote, },
				payload: { url: requiredPlanRepositoryUrl(flags, "repository", entry.usage,), },
			};
		}
		case "project-git.remove-remote": {
			const remote = (flags["name"] as string | undefined) ?? "origin";
			return {
				method: "DELETE",
				endpoint: projectGitEndpoint(
					projectKey,
					`/remotes/${encodeURIComponent(remote,)}`,
				),
				identifiers: { remote, },
			};
		}
		case "project-git.create-branch":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/branches/",),
				identifiers: { name: id, },
				payload: {
					name: id,
					commit: (flags["commit"] as string | undefined) ?? null,
					duplicateProject: parseBooleanOption(
						flags["duplicate-project"],
						"--duplicate-project",
					) ?? false,
					targetProjectKey: (flags["target-project-key"] as string | undefined) ?? null,
					targetProjectFolderId: (flags["target-project-folder-id"] as string | undefined) ?? null,
				},
			};
		case "project-git.delete-branch": {
			const remote = parseBooleanOption(flags["remote"], "--remote",) ?? false;
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/deleteBranch",),
				identifiers: { name: id, remote, },
				payload: {
					name: id,
					remote,
					deleteRemotely: parseBooleanOption(
						flags["delete-remotely"],
						"--delete-remotely",
					) ?? false,
					forceDelete: parseBooleanOption(flags["force-delete"], "--force-delete",) ?? false,
				},
			};
		}
		case "project-git.switch":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/switchBranch${querySuffix({ branchName: id, },)}`,
				),
				identifiers: { branch: id, },
			};
		case "project-git.create-tag":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/tags/",),
				identifiers: { name: id, },
				payload: {
					name: id,
					reference: (flags["reference"] as string | undefined) ?? "HEAD",
					message: (flags["message"] as string | undefined) ?? "",
				},
			};
		case "project-git.delete-tag":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/deleteTag",),
				identifiers: { name: id, },
				payload: { name: id, },
			};
		case "project-git.fetch":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/fetch",),
			};
		case "project-git.pull":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/pullRebase${
						querySuffix({
							branchName: flags["branch"] as string | undefined,
						},)
					}`,
				),
				...((flags["branch"] as string | undefined) !== undefined
					? { identifiers: { branch: flags["branch"], }, }
					: {}),
			};
		case "project-git.push":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/push${
						querySuffix({
							branchName: flags["branch"] as string | undefined,
						},)
					}`,
				),
				...((flags["branch"] as string | undefined) !== undefined
					? { identifiers: { branch: flags["branch"], }, }
					: {}),
			};
		case "project-git.commit": {
			const message = requiredPlanFlag(flags, "message", entry.usage,);
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/commit",),
				identifiers: { message, },
				payload: { message, },
			};
		}
		case "project-git.revert-to-revision":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/revertToRevision${querySuffix({ commit: id, },)}`,
				),
				identifiers: { commit: id, },
			};
		case "project-git.revert-commit":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/revertCommit${querySuffix({ commit: id, },)}`,
				),
				identifiers: { commit: id, },
			};
		case "project-git.reset-to-head":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/resetToLocalHeadState",),
			};
		case "project-git.reset-to-upstream":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/resetToRemoteHeadState",),
			};
		case "project-git.drop-and-rebuild": {
			if (
				parseBooleanOption(
					flags["i-know-what-i-am-doing"],
					"--i-know-what-i-am-doing",
				) !== true
			) {
				throw new UsageError(
					`--i-know-what-i-am-doing is required to acknowledge the irreversible Git history loss. Usage: ${entry.usage}`,
				);
			}
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/dropAndRebuild${querySuffix({ iKnowWhatIAmDoing: true, },)}`,
				),
			};
		}
		case "project-git.add-library": {
			const targetPath = id;
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/lib-git-refs/",),
				identifiers: { localTargetPath: targetPath, },
				payload: {
					repository: requiredPlanRepositoryUrl(flags, "repository", entry.usage,),
					login: (flags["login"] as string | undefined) ?? null,
					password: flags["password-env"] !== undefined ? "***" : null,
					pathInGitRepository: (flags["path-in-repository"] as string | undefined) ?? "",
					localTargetPath: targetPath,
					checkout: requiredPlanFlag(flags, "checkout", entry.usage,),
					addToPythonPath: parseBooleanOption(
						flags["no-add-to-python-path"],
						"--no-add-to-python-path",
					) !== true,
				},
				wait: projectGitFutureWait(),
			};
		}
		case "project-git.set-library": {
			const targetPath = validateGitReferencePath(id,);
			return {
				method: "PUT",
				endpoint: projectGitEndpoint(
					projectKey,
					`/lib-git-refs/${encodeGitReferencePath(targetPath,)}`,
				),
				identifiers: { library: targetPath, },
				payload: {
					repository: requiredPlanRepositoryUrl(flags, "repository", entry.usage,),
					login: (flags["login"] as string | undefined) ?? null,
					password: flags["password-env"] !== undefined ? "***" : null,
					pathInGitRepository: (flags["path-in-repository"] as string | undefined) ?? "",
					checkout: requiredPlanFlag(flags, "checkout", entry.usage,),
				},
			};
		}
		case "project-git.remove-library": {
			const targetPath = validateGitReferencePath(id,);
			return {
				method: "DELETE",
				endpoint: projectGitEndpoint(
					projectKey,
					`/lib-git-refs/${encodeGitReferencePath(targetPath,)}${
						querySuffix({
							deleteDirectory: parseBooleanOption(
								flags["delete-directory"],
								"--delete-directory",
							) ?? false,
						},)
					}`,
				),
				identifiers: { library: targetPath, },
			};
		}
		case "project-git.reset-library": {
			const targetPath = validateGitReferencePath(id,);
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/lib-git-refs/action/reset",),
				identifiers: { library: targetPath, },
				payload: { gitRef: targetPath, },
				wait: projectGitFutureWait(),
			};
		}
		case "project-git.push-library": {
			const targetPath = validateGitReferencePath(id,);
			const message = requiredPlanFlag(flags, "message", entry.usage,);
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/lib-git-refs/action/push",),
				identifiers: { library: targetPath, message, },
				payload: { gitRef: targetPath, commitMessage: message, },
				wait: projectGitFutureWait(),
			};
		}
		case "project-git.push-all-libraries": {
			const message = requiredPlanFlag(flags, "message", entry.usage,);
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/git-refs/push-all",),
				identifiers: { message, },
				payload: { commitMessage: message, },
				wait: projectGitFutureWait(),
			};
		}
		case "project-git.reset-all-libraries":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/git-refs/reset-all",),
				wait: projectGitFutureWait(),
			};
		case "project-git.future-abort":
			return {
				method: "DELETE",
				endpoint: projectGitFutureEndpoint(id,),
				identifiers: { jobId: id, },
			};
		case "project-folder.settings-set": {
			const payload = requiredPlanJsonInput(
				flags,
				`--data, --data-file, or --stdin is required. Usage: ${entry.usage}`,
			);
			return {
				method: "PUT",
				endpoint: `${projectFolderEndpoint(id,)}/settings`,
				identifiers: { folderId: id, },
				payload,
			};
		}
		case "project-folder.move": {
			const destination = args[1];
			if (!destination) throw new UsageError(`Usage: ${entry.usage}`,);
			return {
				method: "POST",
				endpoint: `${projectFolderEndpoint(id,)}/move${querySuffix({ destination, },)}`,
				identifiers: { folderId: id, destination, },
			};
		}
		case "project-folder.delete":
			return {
				method: "DELETE",
				endpoint: projectFolderEndpoint(id,),
				identifiers: { folderId: id, },
			};
		case "project-folder.create-child": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			return {
				method: "POST",
				endpoint: `${projectFolderEndpoint(id,)}/children${querySuffix({ name, },)}`,
				identifiers: { parentFolderId: id, name, },
			};
		}
		case "project-folder.move-project": {
			const projectKeyArg = args[1];
			const destination = args[2];
			if (!projectKeyArg || !destination) {
				throw new UsageError(`Usage: ${entry.usage}`,);
			}
			return {
				method: "POST",
				endpoint: `${projectFolderEndpoint(id,)}/projects/${encodeURIComponent(projectKeyArg,)}/move${
					querySuffix({ destination, },)
				}`,
				identifiers: { folderId: id, projectKey: projectKeyArg, destination, },
			};
		}
		default:
			return {
				exact: false,
				reason:
					`No exact offline request shape is defined for ${resource}.${action}; no endpoint was guessed.`,
				identifiers: id ? { id, } : undefined,
			};
	}
}

export function buildMutationPlan(
	resource: string,
	action: string,
	meta: CommandMeta,
	args: string[],
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	const entry = buildRegistryEntry(resource, action, meta,);
	if (!entry.mutatesDss && entry.sideEffect !== "write") {
		throw new UsageError(`--plan is only supported for mutating commands. Usage: ${meta.usage}`,);
	}
	const requiredPositionals = entry.positionalArguments.filter((positional,) => positional.required);
	requireArgs(args, requiredPositionals.length, meta.usage,);
	const projectKey = projectKeyForPlan(entry, flags,);
	const shape = commandPlanShape(resource, action, args, flags, entry, projectKey,);
	return planResult(resource, action, {
		...shape,
		asyncKind: entry.async,
		exitCodesOnFailure: exitCodesOnFailure(entry,),
		idempotency: entry.idempotency,
		plannedAndDryRun: executionMode(flags,).dryRun,
	},);
}
