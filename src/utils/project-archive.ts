import StreamZip from "node-stream-zip";
import { stat, } from "node:fs/promises";
import type { Readable, } from "node:stream";
import { crc32, } from "node:zlib";
import { ClientValidationError, } from "../errors.js";

const MANIFEST_MEMBER = "export-manifest.json";
const DATASETS_ROOT = "project_config/datasets";
const RECIPES_ROOT = "project_config/recipes";
const BUNDLED_DATA_ROOT = "any_datasets_data";
const UPLOADS_ROOT = "uploads";
/**
 * Definition roots for the non-dataset flow objects a recipe channel can
 * reference. Their names live in the same flat namespace as dataset names, so
 * an untyped reference resolving here is not a dangling dataset reference.
 */
const NON_DATASET_OBJECT_ROOTS = [
	"project_config/managed_folders",
	"project_config/saved_models",
	"project_config/streaming_endpoints",
	"project_config/model_evaluation_stores",
];
/**
 * Reference `type` values that designate a non-dataset flow object. Refs
 * carrying one of these are validated against their own definition
 * namespace, never against the dataset definitions.
 */
const NON_DATASET_REFERENCE_TYPES: Record<string, true> = {
	MANAGED_FOLDER: true,
	FOLDER: true,
	SAVED_MODEL: true,
	MODEL: true,
	STREAMING_ENDPOINT: true,
	MODEL_EVALUATION_STORE: true,
};
/** Upper bound for parsed JSON members (manifest, recipes). */
const MAX_JSON_MEMBER_BYTES = 16 * 1024 * 1024;
/** Upper bound for residue text scanning of individual members. */
const MAX_RESIDUE_SCAN_BYTES = 1024 * 1024;

export type ProjectArchiveIssueSeverity = "error" | "warning";

export interface ProjectArchiveIssue {
	severity: ProjectArchiveIssueSeverity;
	code: string;
	message: string;
	member?: string;
}

export interface ProjectArchiveInspection {
	filePath: string;
	sizeBytes: number;
	memberCount: number;
	sourceProjectKey?: string;
	valid: boolean;
	issues: ProjectArchiveIssue[];
}

interface OpenArchive {
	zip: StreamZip;
	records: StreamZip.ZipEntry[];
}

interface StreamedMember {
	bytes: number;
	crc: number;
	content: Buffer[] | undefined;
	/** True when accumulation stopped because the byte cap was reached. */
	truncated: boolean;
}

function plainRecord(value: unknown,): Record<string, unknown> | undefined {
	return typeof value === "object" && value !== null && !Array.isArray(value,)
		? value as Record<string, unknown>
		: undefined;
}

function stringField(record: Record<string, unknown>, key: string,): string | undefined {
	const value = record[key];
	return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

/**
 * Open a ZIP archive and enumerate every central-directory record via the
 * `entry` event, so duplicate member names stay visible (the `entries()` map
 * collapses them). Name validation is skipped here and performed separately,
 * so unsafe names are reported as issues instead of aborting the inspection.
 */
function openArchive(filePath: string,): Promise<OpenArchive> {
	const { promise, resolve, reject, } = Promise.withResolvers<OpenArchive>();
	let settled = false;
	const zip = new StreamZip({
		file: filePath,
		storeEntries: true,
		skipEntryNameValidation: true,
	},);
	const records: StreamZip.ZipEntry[] = [];
	zip.on("entry", (entry,) => {
		records.push(entry,);
	},);
	zip.on("error", (err,) => {
		if (!settled) {
			settled = true;
			try {
				zip.close();
			} catch {
				// Best effort: the fd is already held by this invalid handle.
			}
			reject(err,);
		}
	},);
	zip.on("ready", () => {
		if (!settled) {
			settled = true;
			resolve({ zip, records, },);
		}
	},);
	return promise;
}

function closeArchive(zip: StreamZip,): Promise<void> {
	const { promise, resolve, } = Promise.withResolvers<void>();
	zip.close(() => {
		resolve();
	},);
	return promise;
}

/** Independent member-path safety check (never trusts the library's own). */
function isUnsafeMemberPath(name: string,): boolean {
	if (name.length === 0 || name.includes("\0",)) return true;
	if (name.startsWith("/",) || name.startsWith("\\",)) return true;
	if (/^[A-Za-z]:/.test(name,)) return true;
	for (const segment of name.replace(/\\/g, "/",).split("/",)) {
		if (segment === "..") return true;
	}
	return false;
}

/**
 * Stream one member through a bounded-memory CRC-32/size check. The library's
 * own verification is bypassed for data-descriptor records (bit 3 set), so the
 * manual `node:zlib.crc32` computation covers every record uniformly.
 *
 * When `accumulate` is set, buffered content never exceeds `maxBytes`: the
 * buffer is dropped as soon as the cap is reached while the member is still
 * streamed in full, so CRC/size verification stays complete and the size
 * mismatch (if the declared size lied) is still reported.
 */
function streamMember(
	zip: StreamZip,
	entry: StreamZip.ZipEntry,
	accumulate: boolean,
	maxBytes = Number.POSITIVE_INFINITY,
): Promise<StreamedMember> {
	const { promise, resolve, reject, } = Promise.withResolvers<StreamedMember>();
	zip.stream(entry, (err, stm,) => {
		if (err) {
			return reject(err,);
		}
		if (!stm) {
			return reject(new Error("archive produced no member stream",),);
		}
		const stream = stm as Readable;
		let bytes = 0;
		let crc = 0;
		let truncated = false;
		const content: Buffer[] | undefined = accumulate ? [] : undefined;
		stream.on("data", (chunk: Buffer | string,) => {
			const buffer = typeof chunk === "string" ? Buffer.from(chunk,) : chunk;
			bytes += buffer.length;
			crc = crc32(buffer, crc,) >>> 0;
			if (!content || truncated) return;
			if (bytes > maxBytes) {
				truncated = true;
				content.length = 0;
				return;
			}
			content.push(buffer,);
		},);
		stream.on("end", () => {
			resolve({
				bytes,
				crc,
				content: truncated ? undefined : content,
				truncated,
			},);
		},);
		stream.on("error", (streamErr,) => {
			reject(streamErr,);
		},);
	},);
	return promise;
}

function definitionNames(records: StreamZip.ZipEntry[], root: string,): Set<string> {
	const names = new Set<string>();
	const prefix = `${root}/`;
	for (const entry of records) {
		const name = entry.name;
		if (!entry.isDirectory && name.startsWith(prefix,) && name.endsWith(".json",)) {
			names.add(name.slice(prefix.length, -5,),);
		}
	}
	return names;
}

function datasetDefinitionNames(records: StreamZip.ZipEntry[],): Set<string> {
	return definitionNames(records, DATASETS_ROOT,);
}

function recipeDefinitionNames(records: StreamZip.ZipEntry[],): Set<string> {
	return definitionNames(records, RECIPES_ROOT,);
}

/**
 * Check dangling dataset references in recipe definitions: every `ref` under
 * `inputs`/`outputs` channel items that provably names a local dataset must
 * name a defined dataset. Refs to managed folders, saved models, streaming
 * endpoints or model evaluation stores, and project-qualified (`project.key`)
 * refs are not provable against the local dataset definitions and are never
 * treated as dangling datasets.
 */
function checkRecipeReferences(
	recipes: Map<string, unknown>,
	datasetNames: Set<string>,
	nonDatasetNames: Set<string>,
	issues: ProjectArchiveIssue[],
): void {
	for (const recipeName of [...recipes.keys(),].sort()) {
		const recipe = plainRecord(recipes.get(recipeName,),);
		if (!recipe) continue;
		for (const direction of ["inputs", "outputs",]) {
			const channels = plainRecord(recipe[direction],);
			if (!channels) continue;
			for (const [channelName, channelValue,] of Object.entries(channels,)) {
				const channel = plainRecord(channelValue,);
				if (!channel) continue;
				const items = channel["items"];
				if (!Array.isArray(items,)) continue;
				const channelType = stringField(channel, "type",);
				for (const itemValue of items) {
					const item = plainRecord(itemValue,) ?? {};
					const ref = stringField(item, "ref",);
					if (!ref) continue;
					// Project-qualified refs point at the source project's own
					// objects and cannot be proven or disproven locally.
					if (ref.includes(".",)) continue;
					const declaredType = stringField(item, "type",)
						?? stringField(item, "objectType",)
						?? stringField(item, "kind",)
						?? channelType;
					if (declaredType !== undefined) {
						const normalized = declaredType.trim().toUpperCase();
						if (NON_DATASET_REFERENCE_TYPES[normalized] === true) continue;
					}
					if (datasetNames.has(ref,) || nonDatasetNames.has(ref,)) continue;
					issues.push({
						severity: "error",
						code: "dataset_reference_unresolved",
						message:
							`recipe ${recipeName} has dangling dataset reference ${direction}.${channelName}.${ref}`,
						member: `${RECIPES_ROOT}/${recipeName}.json`,
					},);
				}
			}
		}
	}
}

/**
 * Check orphaned bundled-data members and recipe payload members: every
 * `any_datasets_data/`/`uploads/` root must correspond to a dataset
 * definition or a manifest entry, and every non-`.json` recipe member must
 * belong to a defined recipe.
 */
function checkOrphanMembers(
	records: StreamZip.ZipEntry[],
	recipeNames: Set<string>,
	anchoredDatasetNames: Set<string>,
	issues: ProjectArchiveIssue[],
): void {
	for (const root of [BUNDLED_DATA_ROOT, UPLOADS_ROOT,]) {
		const prefix = `${root}/`;
		const orphans = new Set<string>();
		for (const entry of records) {
			const name = entry.name;
			if (!name.startsWith(prefix,)) continue;
			const datasetName = name.slice(prefix.length,).split("/", 1,)[0];
			if (datasetName.length > 0 && !anchoredDatasetNames.has(datasetName,)) {
				orphans.add(`${root}/${datasetName}`,);
			}
		}
		for (const path of [...orphans,].sort()) {
			issues.push({
				severity: "error",
				code: "orphan_member",
				message: `archive member ${path} has no dataset definition or manifest entry`,
				member: path,
			},);
		}
	}

	const orphanPayloads = new Set<string>();
	const prefix = `${RECIPES_ROOT}/`;
	for (const entry of records) {
		const name = entry.name;
		if (entry.isDirectory || !name.startsWith(prefix,) || name.endsWith(".json",)) continue;
		const relative = name.slice(prefix.length,);
		const dotIndex = relative.lastIndexOf(".",);
		if (dotIndex <= 0) continue;
		const recipeName = relative.slice(0, dotIndex,);
		if (!recipeNames.has(recipeName,)) orphanPayloads.add(relative,);
	}
	for (const relative of [...orphanPayloads,].sort()) {
		issues.push({
			severity: "error",
			code: "orphan_member",
			message: `archive member ${prefix}${relative} has no corresponding recipe definition`,
			member: `${prefix}${relative}`,
		},);
	}
}

const CREDENTIAL_VALUE_RE =
	/\b(?:bearer\s+[A-Za-z0-9._~+/=_-]{8,})\b|(?:authorization|api[-_]?key|apikey|x-api-key)["'\s:=]+[A-Za-z0-9._~+/=_-]{16,}/gi;
const IDENTITY_EMAIL_RE =
	/(?<![:/?#&=])[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}(?![/])/g;
const POSIX_HOME_PATH_RE = /\/(?:Users|home)\/[^/\s"'<>,;)]+/g;
const WINDOWS_HOME_PATH_RE = /[A-Za-z]:\\Users\\[^\\\s"'<>,;)]+/g;
const IDENTITY_METADATA_KEY_RE =
	/\b(?:createdBy|lastModifiedBy|modifiedBy|author|creator|owner|username|ownerEmail|userEmail|email|login)\b\s*:/gi;

interface ResidueCounts {
	credential: number;
	email: number;
	homePath: number;
	metadataField: number;
}

function countResidue(text: string,): ResidueCounts {
	return {
		credential: (text.match(CREDENTIAL_VALUE_RE,) ?? []).length,
		email: (text.match(IDENTITY_EMAIL_RE,) ?? []).length,
		homePath: (text.match(POSIX_HOME_PATH_RE,) ?? []).length
			+ (text.match(WINDOWS_HOME_PATH_RE,) ?? []).length,
		metadataField: (text.match(IDENTITY_METADATA_KEY_RE,) ?? []).length,
	};
}

function residueIssue(member: string, counts: ResidueCounts,): ProjectArchiveIssue {
	const total = counts.credential + counts.email + counts.homePath + counts.metadataField;
	return {
		severity: "warning",
		code: "identity_residue",
		message: `member contains ${total} identity/history/machine-specific residue pattern(s)`
			+ ` (${counts.credential} credential-like, ${counts.email} email-like,`
			+ ` ${counts.homePath} local-path-like, ${counts.metadataField} identity metadata field-like)`,
		member,
	};
}

/**
 * Inspect a DSS project export ZIP archive without extracting anything to
 * disk: enumerate every central-directory record, validate member paths
 * independently, stream every file member through a size/CRC-32 check with
 * bounded memory (manual `node:zlib.crc32`, covering data-descriptor
 * records), parse `export-manifest.json` and the source project key, and run
 * the generic dangling-reference/orphan-member graph checks. Identity,
 * history and machine-specific residue is reported only as warnings carrying
 * member paths and pattern counts, never matched values.
 */
export async function inspectProjectArchive(
	filePath: string,
): Promise<ProjectArchiveInspection> {
	let sizeBytes = 0;
	let statError: ProjectArchiveIssue | undefined;
	try {
		const info = await stat(filePath,);
		if (!info.isFile()) {
			statError = {
				severity: "error",
				code: "archive_not_regular_file",
				message: "archive path is not a regular file",
			};
		} else if (info.size === 0) {
			statError = {
				severity: "error",
				code: "archive_empty",
				message: "archive file is empty",
			};
		}
		sizeBytes = info.size;
	} catch {
		statError = {
			severity: "error",
			code: "archive_not_regular_file",
			message: "archive path does not exist or cannot be read",
		};
	}

	const issues: ProjectArchiveIssue[] = [];
	if (statError) {
		issues.push(statError,);
		return {
			filePath,
			sizeBytes,
			memberCount: 0,
			valid: false,
			issues,
		};
	}

	let zip: StreamZip | undefined;
	let records: StreamZip.ZipEntry[] = [];
	try {
		const opened = await openArchive(filePath,);
		zip = opened.zip;
		records = opened.records;
	} catch {
		issues.push({
			severity: "error",
			code: "archive_invalid_zip",
			message: "archive is not a valid ZIP archive",
		},);
		return {
			filePath,
			sizeBytes,
			memberCount: 0,
			valid: false,
			issues,
		};
	}

	try {
		const memberCounts = new Map<string, number>();
		for (const entry of records) {
			memberCounts.set(entry.name, (memberCounts.get(entry.name,) ?? 0) + 1,);
			if (isUnsafeMemberPath(entry.name,)) {
				issues.push({
					severity: "error",
					code: "unsafe_member_path",
					message: "member path is not a safe relative path",
					member: entry.name,
				},);
			}
		}
		for (const name of [...memberCounts.keys(),].sort()) {
			const count = memberCounts.get(name,) ?? 0;
			if (count > 1) {
				issues.push({
					severity: "error",
					code: "duplicate_member_name",
					message: `archive member name appears ${count} times in the central directory`,
					member: name,
				},);
			}
		}

		const manifestJson = new Map<string, unknown>();
		const recipes = new Map<string, unknown>();
		for (const entry of records) {
			if (!entry.isFile) continue;
			const name = entry.name;
			const isManifest = name === MANIFEST_MEMBER;
			const isRecipeJson = name.startsWith(`${RECIPES_ROOT}/`,) && name.endsWith(".json",);
			if (isManifest && entry.size > MAX_JSON_MEMBER_BYTES) {
				issues.push({
					severity: "error",
					code: "manifest_too_large",
					message: "export-manifest.json member is too large to inspect",
					member: name,
				},);
			}
			if (isRecipeJson && entry.size > MAX_JSON_MEMBER_BYTES) {
				issues.push({
					severity: "warning",
					code: "member_json_too_large",
					message: "recipe definition member is too large to inspect for references",
					member: name,
				},);
			}
			const parseCandidate = (isManifest || isRecipeJson) && entry.size <= MAX_JSON_MEMBER_BYTES;
			const scanCandidate = (isManifest
				|| name.startsWith("project_config/",)
				|| name.startsWith(`${BUNDLED_DATA_ROOT}/`,)
				|| name.startsWith(`${UPLOADS_ROOT}/`,))
				&& entry.size <= MAX_RESIDUE_SCAN_BYTES;
			let streamed: StreamedMember | undefined;
			try {
				// The buffering cap applies to actual streamed bytes, not the
				// declared size: a member can declare a small size and still
				// inflate arbitrarily, which must not be buffered unbounded.
				streamed = await streamMember(
					zip,
					entry,
					parseCandidate || scanCandidate,
					parseCandidate
						? MAX_JSON_MEMBER_BYTES
						: scanCandidate
						? MAX_RESIDUE_SCAN_BYTES
						: Number.POSITIVE_INFINITY,
				);
			} catch (streamErr) {
				streamed = undefined;
				const prefix = `member ${name}: `;
				if (streamErr instanceof Error && /CRC/i.test(streamErr.message,)) {
					issues.push({
						severity: "error",
						code: "member_crc_mismatch",
						message: `${prefix}data CRC-32 does not match its central-directory record`,
						member: name,
					},);
				} else if (streamErr instanceof Error && /Invalid size/i.test(streamErr.message,)) {
					issues.push({
						severity: "error",
						code: "member_size_mismatch",
						message: `${prefix}data length does not match its central-directory record`,
						member: name,
					},);
				} else {
					issues.push({
						severity: "error",
						code: "member_unreadable",
						message: `${prefix}data cannot be read from archive`,
						member: name,
					},);
				}
			}
			if (!streamed) continue;
			if (streamed.bytes !== entry.size) {
				issues.push({
					severity: "error",
					code: "member_size_mismatch",
					message: `member ${name}: data length does not match its central-directory record`,
					member: name,
				},);
			}
			if (streamed.crc !== entry.crc) {
				issues.push({
					severity: "error",
					code: "member_crc_mismatch",
					message: `member ${name}: data CRC-32 does not match its central-directory record`,
					member: name,
				},);
			}
			if (streamed.truncated) {
				issues.push({
					severity: isManifest ? "error" : "warning",
					code: "member_size_limit_exceeded",
					message:
						`member ${name}: data exceeds the inspection byte limit and was not buffered for parsing`,
					member: name,
				},);
			}
			const content = streamed.content;
			if (!content) continue;
			const buffer = Buffer.concat(content,);
			if (parseCandidate && isManifest) {
				try {
					manifestJson.set(name, JSON.parse(buffer.toString("utf8",),),);
				} catch {
					issues.push({
						severity: "error",
						code: "manifest_invalid_json",
						message: "export-manifest.json is not valid JSON",
						member: name,
					},);
				}
			}
			if (parseCandidate && isRecipeJson) {
				try {
					recipes.set(
						name.slice(RECIPES_ROOT.length + 1, -5,),
						JSON.parse(buffer.toString("utf8",),),
					);
				} catch {
					// Recipe graph is not provable from this member; the ported
					// reference check skips unparseable recipes silently.
				}
			}
			if (scanCandidate && buffer.length <= MAX_RESIDUE_SCAN_BYTES) {
				const counts = countResidue(buffer.toString("utf8",),);
				if (counts.credential + counts.email + counts.homePath + counts.metadataField > 0) {
					issues.push(residueIssue(name, counts,),);
				}
			}
		}
		let sourceProjectKey: string | undefined;
		let manifestDatasetNames: Set<string> | undefined;
		const manifestMember = manifestJson.get(MANIFEST_MEMBER,);
		const manifest = plainRecord(manifestMember,);
		if (manifestMember === undefined) {
			issues.push({
				severity: "error",
				code: "manifest_missing",
				message: "archive does not contain export-manifest.json",
			},);
		} else if (!manifest) {
			issues.push({
				severity: "error",
				code: "manifest_invalid_json",
				message: "export-manifest.json is not a valid JSON object",
			},);
		} else {
			sourceProjectKey = stringField(manifest, "originalProjectKey",)
				?? stringField(manifest, "projectKey",);
			if (!sourceProjectKey) {
				issues.push({
					severity: "error",
					code: "manifest_project_key_missing",
					message: "export-manifest.json does not declare a source project key",
					member: MANIFEST_MEMBER,
				},);
			}
			const actualContent = plainRecord(manifest["actualContent"],);
			const included = actualContent?.["includedDatasets"];
			if (Array.isArray(included,)) {
				const names = new Set<string>();
				for (const itemValue of included) {
					const item = plainRecord(itemValue,);
					const name = stringField(item ?? {}, "name",);
					if (name) names.add(name,);
				}
				manifestDatasetNames = names;
			}
		}

		const datasetNames = datasetDefinitionNames(records,);
		const nonDatasetNames = new Set<string>();
		for (const root of NON_DATASET_OBJECT_ROOTS) {
			for (const name of definitionNames(records, root,)) {
				nonDatasetNames.add(name,);
			}
		}
		const anchored = new Set(datasetNames,);
		for (const name of manifestDatasetNames ?? []) {
			anchored.add(name,);
		}
		const recipeNames = recipeDefinitionNames(records,);
		checkRecipeReferences(recipes, datasetNames, nonDatasetNames, issues,);
		checkOrphanMembers(records, recipeNames, anchored, issues,);

		issues.sort((a, b,) => {
			const severityOrder = a.severity === b.severity ? 0 : a.severity === "error" ? -1 : 1;
			return (
				severityOrder
				|| a.code.localeCompare(b.code,)
				|| (a.member ?? "").localeCompare(b.member ?? "",)
				|| a.message.localeCompare(b.message,)
			);
		},);

		return {
			filePath,
			sizeBytes,
			memberCount: records.length,
			sourceProjectKey,
			valid: !issues.some((issue,) => issue.severity === "error"),
			issues,
		};
	} finally {
		if (zip) {
			try {
				await closeArchive(zip,);
			} catch {
				// The archive is already unreadable; the fd is released on best
				// effort only. Errors here do not change the inspection result.
			}
		}
	}
}

/**
 * Like `inspectProjectArchive`, but throws a `ClientValidationError` with the
 * `validation_failed` code when the inspection contains any error-severity
 * issue, so the archive cannot be imported.
 */
export async function assertImportableProjectArchive(
	filePath: string,
): Promise<ProjectArchiveInspection> {
	const inspection = await inspectProjectArchive(filePath,);
	const errorCount = inspection.issues.filter((issue,) => issue.severity === "error").length;
	if (errorCount > 0) {
		throw new ClientValidationError(
			`project archive is not importable: ${errorCount} validation error(s) found`,
			"validation_failed",
			undefined,
			{ filePath: inspection.filePath, issues: inspection.issues, },
		);
	}
	return inspection;
}
