import { ClientValidationError, DataikuError, type DataikuErrorCategory, } from "../errors.js";
import { projectIncarnationHash, } from "../utils/project-incarnation.js";
import { compareStrings, stableHash, stableJson, } from "../utils/stable-hash.js";
import { BaseResource, } from "./base.js";

export interface AppListItem extends Record<string, unknown> {
	appId?: string;
	projectKey?: string;
	name?: string;
}

export interface AppInstanceRef extends Record<string, unknown> {
	appId?: string;
	projectKey?: string;
	jobId?: string;
}

export interface BusinessAppListItem extends Record<string, unknown> {
	id?: string;
	projectKey?: string;
	name?: string;
}

export interface BusinessAppInstanceUserPermissions extends Record<string, unknown> {
	login?: string;
	admin?: boolean;
	readProjectContent?: boolean;
	writeProjectContent?: boolean;
}

export interface DeleteAppInstanceOptions {
	/** Refuse deletion unless the current creationTag hashes to this value. */
	expectedProjectIncarnationHash?: string;
}

// ---------------------------------------------------------------------------
// App manifest validation and comparison
// ---------------------------------------------------------------------------

export type AppManifestReferenceKind = "scenario" | "folder" | "variable";

/**
 * Source-verifiable app-manifest reference locations. Validation is scoped to
 * homepage tiles so identically named keys in arbitrary custom-form `config`
 * objects are never treated as references.
 */
export const APP_MANIFEST_REFERENCE_KEYS: Record<AppManifestReferenceKind, string[]> = {
	scenario: ["SCENARIO_RUN.scenarioId",],
	folder: ["DOWNLOAD_FILE.folderId", "DOWNLOAD_FILE.managedFolderId",],
	variable: ["params[].name",],
};

export interface AppManifestReference {
	kind: AppManifestReferenceKind;
	path: string;
	value: string;
	exists: boolean;
}

export interface AppManifestCheck {
	kind: AppManifestReferenceKind;
	status: "ok" | "failed" | "skipped";
	checked: number;
	malformed: number;
	missing: number;
}

export interface AppManifestIssue {
	code:
		| "INVALID_MANIFEST_ROOT"
		| "INVALID_REFERENCE_VALUE"
		| "MISSING_SCENARIO"
		| "MISSING_FOLDER"
		| "MISSING_VARIABLE";
	path: string;
	message: string;
}

export interface AppManifestValidationResult {
	valid: boolean;
	projectKey: string;
	manifestHash: string;
	references: AppManifestReference[];
	checks: AppManifestCheck[];
	errors: AppManifestIssue[];
}

export interface AppManifestPathDifference {
	path: string;
	kind: "added" | "removed" | "changed";
	template?: unknown;
	instance?: unknown;
}

export interface AppManifestComparisonResult {
	appId: string;
	projectKey: string;
	templateHash: string;
	instanceHash: string;
	identical: boolean;
	omittedFields: string[];
	differences: AppManifestPathDifference[];
}

/**
 * Raw app-manifest version markers. DSS renders the Application header version
 * from the manifest's own `version` string, so that field (with its optional
 * `versionNotes`) is the authoritative state; no separate publish/release
 * transaction exists on the public API.
 */
export interface AppManifestVersionState {
	projectKey: string;
	projectAppType: string | null;
	version: string | null;
	versionNotes: string | null;
	manifestHash: string;
}

export interface AppManifestVersionUpdate {
	/** New manifest `version` string. Must be non-empty when supplied. */
	version?: string;
	/** New manifest `versionNotes` string. May be empty to clear existing notes. */
	versionNotes?: string;
	/**
	 * 64-hex manifest hash the caller last read, used as a non-atomic stale-read
	 * guard: the write is refused when the manifest already differs at the
	 * verification read. It cannot prevent a concurrent write that lands between
	 * that read and this write.
	 */
	expectHash?: string;
	/** Compute and report the change without issuing a write. */
	dryRun?: boolean;
}

/**
 * The only concurrency control the public app-manifest endpoint permits. DSS
 * exposes no conditional write (no ETag, no If-Match, no version token), so
 * `expectHash` can only detect a hash that is already stale when it is checked;
 * it can never turn the subsequent PUT into a conditional write. Every
 * manifest-version write
 * result reports this literal so callers can machine-check the guarantee they
 * actually get instead of inferring one from prose.
 */
export const APP_MANIFEST_CONCURRENCY_CONTROL = "client-side-non-atomic-stale-read-check";

export type AppManifestConcurrencyControl = typeof APP_MANIFEST_CONCURRENCY_CONTROL;

export type AppManifestVersionWriteOutcome =
	| "persisted"
	| "dry-run"
	| "unchanged"
	| "indeterminate";

export interface AppManifestVersionWriteFailure {
	/** Sanitized summary; never echoes response bodies or secrets. */
	error: string;
	errorStatus: number | null;
	errorCategory: DataikuErrorCategory | null;
	remediation: string;
}

export interface AppManifestVersionWriteResult {
	projectKey: string;
	projectAppType: string | null;
	/** Fields the caller asked to set; null means "left untouched". */
	requested: { version: string | null; versionNotes: string | null; };
	/** True when the desired manifest differs from the manifest that was read. */
	changed: boolean;
	dryRun: boolean;
	/**
	 * True only after a write was accepted and re-read as exactly `desired`.
	 * False for unchanged/dry-run results. Null when the write outcome could
	 * not be observed (transport loss, transient/server failure, or failed
	 * verification read) — the write may still have landed.
	 */
	persisted: boolean | null;
	outcome: AppManifestVersionWriteOutcome;
	before: AppManifestVersionState;
	desired: AppManifestVersionState;
	/**
	 * Last state observed on the server; equals `before` when nothing was
	 * written. Null when the write outcome could not be observed.
	 */
	after: AppManifestVersionState | null;
	/**
	 * Always `"client-side-non-atomic-stale-read-check"`. The GET→PUT window is
	 * unguarded: this client can overwrite a competing write that lands after its
	 * GET, and that lost update is not detectable when this client's PUT wins.
	 * The final read only verifies the state observed after this client's PUT.
	 */
	concurrencyControl: AppManifestConcurrencyControl;
	/** Present only when outcome is "indeterminate". */
	error?: AppManifestVersionWriteFailure;
}

/**
 * Root-level project identity fields excluded from template-vs-instance
 * comparison. `projectAppType` differs by construction (APP_TEMPLATE vs
 * APP_INSTANCE), and `projectKey` names the distinct template/instance
 * projects. Every other field, including version markers and unknown fields,
 * remains governed and is compared.
 */
const APP_MANIFEST_COMPARE_OMITTED_FIELDS = [
	"projectKey",
	"projectAppType",
] as const;

function isPlainRecord(value: unknown,): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value,);
}

function propertyPath(path: string, key: string,): string {
	return `${path}[${JSON.stringify(key,)}]`;
}

/**
 * Extract a reference value using the manifest key's singular/plural shape:
 * singular `*Id`/`*Name` keys hold one non-empty string and plural
 * `*Ids`/`*Names` keys hold an array of non-empty strings. Anything else is
 * reported as malformed. Purely functional: never mutates the manifest.
 */
function collectReferenceValue(
	value: unknown,
	path: string,
	key: string,
	kind: AppManifestReferenceKind,
	references: AppManifestReference[],
	issues: Array<{ kind: AppManifestReferenceKind; issue: AppManifestIssue; }>,
): void {
	const plural = key.endsWith("Ids",) || key.endsWith("Names",);
	const expected = plural ? "an array of non-empty strings" : "a non-empty string";
	const malformed = (issuePath = path,): void => {
		issues.push({
			kind,
			issue: {
				code: "INVALID_REFERENCE_VALUE",
				path: issuePath,
				message: `Reference key "${key}" must hold ${expected}.`,
			},
		},);
	};
	if (!plural) {
		if (typeof value === "string" && value.length > 0) {
			references.push({ kind, path, value, exists: false, },);
		} else {
			malformed();
		}
		return;
	}
	if (!Array.isArray(value,)) {
		malformed();
		return;
	}
	for (let i = 0; i < value.length; i++) {
		const item = value[i];
		const itemPath = `${path}[${i}]`;
		if (typeof item === "string" && item.length > 0) {
			references.push({ kind, path: itemPath, value: item, exists: false, },);
		} else {
			malformed(itemPath,);
		}
	}
}

function omitRootFields(value: unknown, omitted: readonly string[],): unknown {
	if (!isPlainRecord(value,)) return value;
	const result: Record<string, unknown> = {};
	for (const key of Object.keys(value,)) {
		if (omitted.includes(key,)) continue;
		result[key] = value[key];
	}
	return result;
}

/** Lowercase or uppercase SHA-256 hex digest, as emitted by `stableHash`. */
const EXPECT_HASH_PATTERN = /^[0-9a-fA-F]{64}$/;

/**
 * Project a manifest onto its version state. Only string values count: a
 * non-string `version` is reported as null rather than coerced, so callers can
 * never mistake `3` or `null` for a released version string, and no `N/A`
 * placeholder is invented. The hash covers the whole manifest so the
 * stale-read check detects edits to any field, not just the version markers.
 */
function manifestVersionState(
	projectKey: string,
	projectAppType: string | null,
	manifest: Record<string, unknown>,
): AppManifestVersionState {
	return {
		projectKey,
		projectAppType,
		version: typeof manifest["version"] === "string" ? manifest["version"] : null,
		versionNotes: typeof manifest["versionNotes"] === "string" ? manifest["versionNotes"] : null,
		manifestHash: stableHash(manifest,),
	};
}

function diffValues(
	template: unknown,
	instance: unknown,
	path: string,
): AppManifestPathDifference[] {
	if (isPlainRecord(template,) && isPlainRecord(instance,)) {
		const keys = Array.from(
			new Set([...Object.keys(template,), ...Object.keys(instance,),],),
		).sort(compareStrings,);
		const differences: AppManifestPathDifference[] = [];
		for (const key of keys) {
			const childPath = propertyPath(path, key,);
			const inTemplate = Object.prototype.hasOwnProperty.call(template, key,);
			const inInstance = Object.prototype.hasOwnProperty.call(instance, key,);
			if (inTemplate && !inInstance) {
				differences.push({ path: childPath, kind: "removed", template: template[key], },);
			} else if (!inTemplate && inInstance) {
				differences.push({ path: childPath, kind: "added", instance: instance[key], },);
			} else {
				differences.push(...diffValues(template[key], instance[key], childPath,),);
			}
		}
		return differences;
	}
	if (Array.isArray(template,) && Array.isArray(instance,)) {
		const differences: AppManifestPathDifference[] = [];
		const length = Math.max(template.length, instance.length,);
		for (let i = 0; i < length; i++) {
			const childPath = `${path}[${i}]`;
			if (i >= template.length) {
				differences.push({ path: childPath, kind: "added", instance: instance[i], },);
			} else if (i >= instance.length) {
				differences.push({ path: childPath, kind: "removed", template: template[i], },);
			} else {
				differences.push(...diffValues(template[i], instance[i], childPath,),);
			}
		}
		return differences;
	}
	if (stableJson(template,) === stableJson(instance,)) return [];
	return [{ path, kind: "changed", template, instance, },];
}

const APP_MANIFEST_WRITE_INDETERMINATE_REMEDIATION =
	"Whether DSS persisted the manifest version could not be verified. Re-read the manifest version and compare its hash to the desired hash before retrying; a safe retry writes the identical manifest again.";

/** Never echoes response bodies or secrets, only the DataikuError taxonomy. */
function manifestVersionWriteErrorSummary(error: unknown,): AppManifestVersionWriteFailure {
	if (error instanceof DataikuError) {
		return {
			error: error.safeMessage,
			errorStatus: error.status,
			errorCategory: error.category,
			remediation: APP_MANIFEST_WRITE_INDETERMINATE_REMEDIATION,
		};
	}
	return {
		error: error instanceof Error ? error.message : String(error,),
		errorStatus: null,
		errorCategory: null,
		remediation: APP_MANIFEST_WRITE_INDETERMINATE_REMEDIATION,
	};
}

function indeterminateManifestVersionWriteResult(
	pk: string,
	before: AppManifestVersionState,
	desired: AppManifestVersionState,
	requested: { version: string | null; versionNotes: string | null; },
	error: unknown,
): AppManifestVersionWriteResult {
	return {
		projectKey: pk,
		projectAppType: before.projectAppType,
		requested,
		changed: true,
		dryRun: false,
		persisted: null,
		outcome: "indeterminate",
		before,
		desired,
		after: null,
		concurrencyControl: APP_MANIFEST_CONCURRENCY_CONTROL,
		error: manifestVersionWriteErrorSummary(error,),
	};
}
export class ApplicationsResource extends BaseResource {
	/** List all Dataiku Apps. */
	async listApps(): Promise<AppListItem[]> {
		return this.client.get<AppListItem[]>("/public/api/apps/",);
	}

	/** Get the manifest for a Dataiku App template. */
	async getAppManifest(appId: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/apps/${encodeURIComponent(appId,)}/`,
		);
	}

	/** List instances created from a Dataiku App template. */
	async listInstances(appId: string,): Promise<AppInstanceRef[]> {
		return this.client.get<AppInstanceRef[]>(
			`/public/api/apps/${encodeURIComponent(appId,)}/instances/`,
		);
	}

	/** Create an instance from a Dataiku App template. */
	async createInstance(appId: string, body: Record<string, unknown>,): Promise<AppInstanceRef> {
		return this.client.post<AppInstanceRef>(
			`/public/api/apps/${encodeURIComponent(appId,)}/instances`,
			body,
		);
	}

	/** Get the manifest for an app instance project. */
	async getInstanceManifest(projectKey?: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/app-manifest`,
		);
	}

	private async getProjectDetails(projectKey: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${encodeURIComponent(projectKey,)}/`,
		);
	}

	private async getProjectAppType(projectKey: string,): Promise<string | undefined> {
		const details = await this.getProjectDetails(projectKey,);
		return typeof details.projectAppType === "string" ? details.projectAppType : undefined;
	}

	/**
	 * Shared app-manifest write guard: DSS only accepts manifest saves for app
	 * template projects, so every mutation path rejects instances and ordinary
	 * projects before issuing a PUT.
	 */
	private requireAppTemplateForManifestSave(
		projectKey: string,
		projectAppType: string | null | undefined,
	): void {
		if (projectAppType === "APP_TEMPLATE") return;
		throw new ClientValidationError(
			"Only Dataiku App template project manifests can be saved through the app-manifest endpoint.",
			"validation_failed",
			projectAppType === "APP_INSTANCE"
				? "Save against the Dataiku App template project instead; existing classic app-instance manifests are read-only through this endpoint."
				: "Convert the project to a Dataiku App template before saving its manifest.",
			{ projectAppType: projectAppType ?? null, projectKey, },
		);
	}

	/** Save the manifest for a Dataiku App template project (rejects classic app-instance projects). */
	async saveInstanceManifest(
		manifest: Record<string, unknown>,
		projectKey?: string,
	): Promise<void> {
		const pk = this.resolveProjectKey(projectKey,);
		const currentManifest = await this.getInstanceManifest(pk,);
		const projectAppType = typeof currentManifest.projectAppType === "string"
			? currentManifest.projectAppType
			: await this.getProjectAppType(pk,);
		this.requireAppTemplateForManifestSave(pk, projectAppType,);
		await this.client.putVoid(
			`/public/api/projects/${encodeURIComponent(pk,)}/app-manifest`,
			manifest,
		);
	}

	/**
	 * Read the manifest plus its derived version state. The project type comes
	 * from the manifest when DSS includes it and from project details otherwise,
	 * so ordinary reads cost a single request.
	 */
	private async readManifestVersion(
		projectKey?: string,
	): Promise<{ manifest: Record<string, unknown>; state: AppManifestVersionState; }> {
		const pk = this.resolveProjectKey(projectKey,);
		const manifest = await this.getInstanceManifest(pk,);
		if (!isPlainRecord(manifest,)) {
			throw new ClientValidationError(
				`The app-manifest endpoint for project ${pk} did not return a JSON object.`,
				"validation_failed",
				"Confirm the project is a Dataiku App template or app instance and that no proxy rewrote the response.",
				{ projectKey: pk, },
			);
		}
		const projectAppType = typeof manifest["projectAppType"] === "string"
			? manifest["projectAppType"]
			: (await this.getProjectAppType(pk,)) ?? null;
		return { manifest, state: manifestVersionState(pk, projectAppType, manifest,), };
	}

	/**
	 * Read the authoritative app-manifest version markers. Allowed for app
	 * templates and app instances alike: instance verification compares an
	 * instance's inherited `version` against its template.
	 */
	async getManifestVersion(projectKey?: string,): Promise<AppManifestVersionState> {
		const { state, } = await this.readManifestVersion(projectKey,);
		return state;
	}

	/**
	 * Set the manifest `version`/`versionNotes` strings on an app template.
	 *
	 * This is a manifest save, not a publish transaction: DSS exposes no release
	 * endpoint, and new instances simply inherit whatever version string the
	 * template carries at creation time.
	 *
	 * The write is not atomic, and `expectHash` cannot change that: the
	 * endpoint takes every PUT unconditionally, so `expectHash` only rejects a
	 * caller whose hash is already stale at the verification read. This client
	 * can overwrite a writer that commits after that read but before this PUT,
	 * and a final state equal to this client's desired manifest cannot reveal
	 * that lost update. The final read still prevents a different state observed
	 * after the PUT from being reported as persisted.
	 */
	async setManifestVersion(
		update: AppManifestVersionUpdate,
		projectKey?: string,
	): Promise<AppManifestVersionWriteResult> {
		const pk = this.resolveProjectKey(projectKey,);
		if (!isPlainRecord(update,)) {
			throw new ClientValidationError(
				"App manifest version update must be an object.",
				"validation_failed",
				"Supply { version } and/or { versionNotes }.",
				{ projectKey: pk, },
			);
		}
		const requestedVersion = update.version;
		const requestedNotes = update.versionNotes;
		if (requestedVersion === undefined && requestedNotes === undefined) {
			throw new ClientValidationError(
				"No app-manifest version field was supplied.",
				"validation_failed",
				"Supply a version string, version notes, or both.",
				{ projectKey: pk, },
			);
		}
		if (
			requestedVersion !== undefined
			&& (typeof requestedVersion !== "string" || requestedVersion.trim() === "")
		) {
			throw new ClientValidationError(
				"App manifest version must be a non-empty string.",
				"validation_failed",
				"Pass the version string DSS should display in the Application header, for example 1.2.0.",
				{ projectKey: pk, },
			);
		}
		if (requestedNotes !== undefined && typeof requestedNotes !== "string") {
			throw new ClientValidationError(
				"App manifest version notes must be a string.",
				"validation_failed",
				"Pass release notes as text, or an empty string to clear them.",
				{ projectKey: pk, },
			);
		}
		const expectHash = update.expectHash;
		if (
			expectHash !== undefined
			&& (typeof expectHash !== "string" || !EXPECT_HASH_PATTERN.test(expectHash,))
		) {
			throw new ClientValidationError(
				"Expected manifest hash must be a 64-character SHA-256 hex digest.",
				"validation_failed",
				"Use the manifestHash value returned by the manifest-version read.",
				{ projectKey: pk, },
			);
		}
		if (update.dryRun !== undefined && typeof update.dryRun !== "boolean") {
			throw new ClientValidationError(
				"Dry-run flag must be a boolean.",
				"validation_failed",
				undefined,
				{ projectKey: pk, },
			);
		}
		const dryRun = update.dryRun === true;

		const { manifest, state: before, } = await this.readManifestVersion(pk,);
		this.requireAppTemplateForManifestSave(pk, before.projectAppType,);
		if (expectHash !== undefined && expectHash.toLowerCase() !== before.manifestHash) {
			throw new ClientValidationError(
				`The app manifest for project ${pk} changed since it was read.`,
				"validation_failed",
				"Re-read the manifest version and retry with the current manifestHash value.",
				{
					projectKey: pk,
					expectedHash: expectHash.toLowerCase(),
					actualHash: before.manifestHash,
				},
			);
		}

		const desiredManifest: Record<string, unknown> = { ...manifest, };
		if (requestedVersion !== undefined) desiredManifest["version"] = requestedVersion;
		if (requestedNotes !== undefined) desiredManifest["versionNotes"] = requestedNotes;
		const desired = manifestVersionState(pk, before.projectAppType, desiredManifest,);
		const requested = {
			version: requestedVersion ?? null,
			versionNotes: requestedNotes ?? null,
		};
		const changed = desired.manifestHash !== before.manifestHash;
		if (!changed || dryRun) {
			return {
				projectKey: pk,
				projectAppType: before.projectAppType,
				requested,
				changed,
				dryRun,
				persisted: false,
				outcome: dryRun ? "dry-run" : "unchanged",
				before,
				desired,
				after: before,
				concurrencyControl: APP_MANIFEST_CONCURRENCY_CONTROL,
			};
		}

		try {
			await this.client.putVoid(
				`/public/api/projects/${encodeURIComponent(pk,)}/app-manifest`,
				desiredManifest,
			);
		} catch (error) {
			// Definitive server rejections keep their normal DataikuError
			// classification; everything else means the write outcome cannot
			// be observed and is reported as indeterminate, never as success.
			const definitiveRejection = error instanceof DataikuError
				&& !error.retryable
				&& error.category !== "unknown"
				&& error.status >= 400
				&& error.status < 500;
			if (definitiveRejection) throw error;
			return indeterminateManifestVersionWriteResult(pk, before, desired, requested, error,);
		}
		let afterManifest: Record<string, unknown>;
		let after: AppManifestVersionState;
		try {
			const verification = await this.readManifestVersion(pk,);
			afterManifest = verification.manifest;
			after = verification.state;
		} catch (error) {
			return indeterminateManifestVersionWriteResult(pk, before, desired, requested, error,);
		}
		if (stableJson(afterManifest,) !== stableJson(desiredManifest,)) {
			throw new ClientValidationError(
				`Saving the app-manifest version for project ${pk} did not persist the requested manifest.`,
				"validation_failed",
				"DSS rejected or rewrote the submitted manifest; re-read the manifest version before claiming any version change.",
				{ projectKey: pk, before, desired, after, },
			);
		}
		return {
			projectKey: pk,
			projectAppType: after.projectAppType,
			requested,
			changed: true,
			dryRun: false,
			persisted: true,
			outcome: "persisted",
			before,
			desired,
			after,
			concurrencyControl: APP_MANIFEST_CONCURRENCY_CONTROL,
		};
	}

	/**
	 * Validate source-verifiable references in app homepage tiles:
	 * SCENARIO_RUN scenario IDs, DOWNLOAD_FILE managed-folder IDs, and runtime
	 * form parameter names (which DSS maps to project variables). Arbitrary
	 * custom-form `config` objects remain opaque. The manifest is never mutated.
	 * Missing/malformed references are returned deterministically; a reference
	 * kind with no applicable tiles does not trigger its project API.
	 */
	async validateAppManifest(
		manifest: unknown,
		projectKey?: string,
	): Promise<AppManifestValidationResult> {
		const pk = this.resolveProjectKey(projectKey,);
		const references: AppManifestReference[] = [];
		const referenceIssues: Array<{ kind: AppManifestReferenceKind; issue: AppManifestIssue; }> = [];
		const rootIssues: AppManifestIssue[] = [];

		if (!isPlainRecord(manifest,)) {
			rootIssues.push({
				code: "INVALID_MANIFEST_ROOT",
				path: "$",
				message: "App manifest root must be a JSON object.",
			},);
		} else {
			const sections = manifest["homepageSections"];
			if (Array.isArray(sections,)) {
				for (let sectionIndex = 0; sectionIndex < sections.length; sectionIndex += 1) {
					const section = sections[sectionIndex];
					if (!isPlainRecord(section,) || !Array.isArray(section["tiles"],)) continue;
					const tiles = section["tiles"];
					for (let tileIndex = 0; tileIndex < tiles.length; tileIndex += 1) {
						const tile = tiles[tileIndex];
						if (!isPlainRecord(tile,)) continue;
						const tilePath = `$["homepageSections"][${sectionIndex}]["tiles"][${tileIndex}]`;
						if (tile["type"] === "SCENARIO_RUN") {
							collectReferenceValue(
								tile["scenarioId"],
								propertyPath(tilePath, "scenarioId",),
								"scenarioId",
								"scenario",
								references,
								referenceIssues,
							);
						}
						if (tile["type"] === "DOWNLOAD_FILE") {
							for (const key of ["folderId", "managedFolderId",]) {
								if (!Object.prototype.hasOwnProperty.call(tile, key,)) continue;
								collectReferenceValue(
									tile[key],
									propertyPath(tilePath, key,),
									key,
									"folder",
									references,
									referenceIssues,
								);
							}
						}
						const params = tile["params"];
						if (!Array.isArray(params,)) continue;
						for (let paramIndex = 0; paramIndex < params.length; paramIndex += 1) {
							const param = params[paramIndex];
							if (!isPlainRecord(param,)) continue;
							collectReferenceValue(
								param["name"],
								`${propertyPath(tilePath, "params",)}[${paramIndex}]["name"]`,
								"name",
								"variable",
								references,
								referenceIssues,
							);
						}
					}
				}
			}
		}

		const scenarioIds = new Set<string>();
		const folderIds = new Set<string>();
		const variableNames = new Set<string>();
		if (references.some((r,) => r.kind === "scenario")) {
			const scenarios = await this.client.scenarios.list(pk,);
			for (const scenario of scenarios) scenarioIds.add(scenario.id,);
		}
		if (references.some((r,) => r.kind === "folder")) {
			const folders = await this.client.folders.list(pk,);
			for (const folder of folders) folderIds.add(folder.id,);
		}
		if (references.some((r,) => r.kind === "variable")) {
			const variables = await this.client.variables.get(pk,);
			for (const name of Object.keys({ ...variables.standard, ...variables.local, },)) {
				variableNames.add(name,);
			}
		}

		const missingCodeByKind: Record<AppManifestReferenceKind, AppManifestIssue["code"]> = {
			scenario: "MISSING_SCENARIO",
			folder: "MISSING_FOLDER",
			variable: "MISSING_VARIABLE",
		};
		for (const ref of references) {
			const exists = ref.kind === "scenario"
				? scenarioIds.has(ref.value,)
				: ref.kind === "folder"
				? folderIds.has(ref.value,)
				: variableNames.has(ref.value,);
			ref.exists = exists;
			if (!exists) {
				referenceIssues.push({
					kind: ref.kind,
					issue: {
						code: missingCodeByKind[ref.kind],
						path: ref.path,
						message: `${ref.kind} reference "${ref.value}" does not exist in project ${pk}.`,
					},
				},);
			}
		}

		const kindOrder: Record<AppManifestReferenceKind, number> = {
			scenario: 0,
			folder: 1,
			variable: 2,
		};
		references.sort((a, b,) => (
			a.kind === b.kind ? compareStrings(a.path, b.path,) : kindOrder[a.kind] - kindOrder[b.kind]
		));
		const errors = [...rootIssues, ...referenceIssues.map((e,) => e.issue),].sort((a, b,) =>
			compareStrings(a.path, b.path,) || compareStrings(a.code, b.code,)
		);

		const checks: AppManifestCheck[] = [];
		if (rootIssues.length > 0) {
			for (const kind of Object.keys(APP_MANIFEST_REFERENCE_KEYS,) as AppManifestReferenceKind[]) {
				checks.push({ kind, status: "skipped", checked: 0, malformed: 0, missing: 0, },);
			}
		} else {
			for (const kind of Object.keys(APP_MANIFEST_REFERENCE_KEYS,) as AppManifestReferenceKind[]) {
				const kindReferences = references.filter((r,) => r.kind === kind);
				const malformed = referenceIssues.filter((e,) =>
					e.kind === kind && e.issue.code === "INVALID_REFERENCE_VALUE"
				).length;
				const missing = kindReferences.filter((r,) => !r.exists).length;
				checks.push({
					kind,
					status: kindReferences.length === 0 && malformed === 0
						? "skipped"
						: malformed + missing > 0
						? "failed"
						: "ok",
					checked: kindReferences.length,
					malformed,
					missing,
				},);
			}
		}

		return {
			valid: errors.length === 0,
			projectKey: pk,
			manifestHash: stableHash(manifest,),
			references,
			checks,
			errors,
		};
	}

	/**
	 * Compare a Dataiku App template manifest with the app-instance manifest of
	 * the target project. Root-level project identity fields in
	 * APP_MANIFEST_COMPARE_OMITTED_FIELDS are omitted from both sides; every
	 * other field — including ones the SDK does not model — is compared.
	 * Hashes cover the omitted-field-normalized states; differences are emitted
	 * as deterministic, path-ordered entries.
	 */
	async compareAppManifest(
		appId: string,
		projectKey?: string,
	): Promise<AppManifestComparisonResult> {
		const pk = this.resolveProjectKey(projectKey,);
		const [templateManifest, instanceManifest,] = await Promise.all([
			this.getAppManifest(appId,),
			this.getInstanceManifest(pk,),
		],);
		const omitted = [...APP_MANIFEST_COMPARE_OMITTED_FIELDS,];
		const template = omitRootFields(templateManifest, omitted,);
		const instance = omitRootFields(instanceManifest, omitted,);
		const differences = diffValues(template, instance, "$",);
		return {
			appId,
			projectKey: pk,
			templateHash: stableHash(template,),
			instanceHash: stableHash(instance,),
			identical: differences.length === 0,
			omittedFields: omitted,
			differences,
		};
	}

	/** Delete an app instance project. */
	async deleteInstance(
		projectKey?: string,
		options: DeleteAppInstanceOptions = {},
	): Promise<void> {
		const expectedIncarnation = options.expectedProjectIncarnationHash;
		if (expectedIncarnation !== undefined && !/^[0-9a-f]{64}$/.test(expectedIncarnation,)) {
			throw new ClientValidationError(
				"expectedProjectIncarnationHash must be a 64-character lowercase SHA-256 hash.",
				"validation_failed",
			);
		}
		let manifest: Record<string, unknown> | undefined;
		try {
			manifest = await this.getInstanceManifest(projectKey,);
		} catch (error) {
			// DSS answers the app-manifest probe with 400 "neither an app template nor an app
			// instance" for ordinary projects; treat only that as "not an app instance". Rethrow
			// everything else (404 not-found, transient, permission) so real failures are not masked.
			if (error instanceof DataikuError && error.status === 400) {
				manifest = undefined;
			} else {
				throw error;
			}
		}
		const pk = this.resolveProjectKey(projectKey,);
		let projectDetails: Record<string, unknown> | undefined;
		const projectAppType = typeof manifest?.projectAppType === "string"
			? manifest.projectAppType
			: manifest
			? (projectDetails = await this.getProjectDetails(pk,)).projectAppType
			: undefined;
		if (projectAppType !== "APP_INSTANCE") {
			throw new ClientValidationError(
				"Only classic Dataiku App instance projects can be deleted through app delete-instance.",
				"validation_failed",
				"Use `dss app delete-instance` only for APP_INSTANCE projects; use `dss project delete` for templates or ordinary projects.",
				{
					projectAppType: typeof projectAppType === "string" ? projectAppType : null,
					projectKey: pk,
				},
			);
		}
		if (expectedIncarnation !== undefined) {
			const details = projectDetails ?? await this.getProjectDetails(pk,);
			const currentIncarnation = projectIncarnationHash(pk, details,);
			if (currentIncarnation !== expectedIncarnation) {
				throw new ClientValidationError(
					`Project ${pk} is not the project incarnation authorized for deletion.`,
					"validation_failed",
					"Refusing deletion after project-key reuse. Inspect the current project and do not delete it as cleanup for the replaced project.",
					{
						projectKey: pk,
						expectedProjectIncarnationHash: expectedIncarnation,
						currentProjectIncarnationHash: currentIncarnation ?? null,
					},
				);
			}
		}
		await this.client.del(`/public/api/projects/${encodeURIComponent(pk,)}`,);
	}

	/** List all Business Apps. */
	async listBusinessApps(): Promise<BusinessAppListItem[]> {
		return this.client.get<BusinessAppListItem[]>("/public/api/business-apps/",);
	}

	/** Get Business App details. */
	async getBusinessApp(id: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/business-apps/${encodeURIComponent(id,)}`,
		);
	}

	/** Get Business App settings. */
	async getBusinessAppSettings(id: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/settings`,
		);
	}

	/** Save Business App settings. */
	async saveBusinessAppSettings(id: string, body: Record<string, unknown>,): Promise<void> {
		await this.client.putVoid(
			`/public/api/business-apps/${encodeURIComponent(id,)}/settings`,
			body,
		);
	}

	/** List instances of a Business App. */
	async listBusinessAppInstances(id: string,): Promise<AppInstanceRef[]> {
		return this.client.get<AppInstanceRef[]>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances`,
		);
	}

	/** Create an instance of a Business App. */
	async createBusinessAppInstance(
		id: string,
		body: Record<string, unknown>,
	): Promise<AppInstanceRef> {
		return this.client.post<AppInstanceRef>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances`,
			body,
		);
	}

	/** Upgrade a Business App instance to the latest version. */
	async upgradeBusinessAppInstance(id: string, projectKey: string,): Promise<AppInstanceRef> {
		return this.client.post<AppInstanceRef>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances/${
				encodeURIComponent(projectKey,)
			}/upgrade`,
			{},
		);
	}

	/** Get a user's effective permissions on a Business App instance. */
	async getBusinessAppInstanceUserPermissions(
		id: string,
		projectKey: string,
		user: string,
	): Promise<BusinessAppInstanceUserPermissions> {
		return this.client.get<BusinessAppInstanceUserPermissions>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances/${
				encodeURIComponent(projectKey,)
			}/permissions/${encodeURIComponent(user,)}`,
		);
	}

	/** Install or upgrade a Business App from an archive. */
	async installBusinessAppFromArchive(filePath: string,): Promise<void> {
		await this.client.upload("/public/api/business-apps/install-from-archive", filePath,);
	}
}
