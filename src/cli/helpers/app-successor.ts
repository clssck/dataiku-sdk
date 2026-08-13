import type { DataikuClient, } from "../../client.js";
import { DataikuError, } from "../../errors.js";
import type { AppManifestValidationResult, } from "../../resources/applications.js";
import type { ProjectPermissions, } from "../../resources/projects.js";
import type { FutureWaitResult, } from "../../schemas.js";
import { projectIncarnationHash, } from "../../utils/project-incarnation.js";
import { stableHash, stringField, } from "../coerce.js";
import { readIfExists, } from "../output.js";
import { UsageError, } from "../usage.js";
import { appPermissionsHash, appPermissionsVerificationError, } from "./app-permissions.js";

/**
 * A DSS API key authenticates the public REST API only: the Application UI sits
 * behind the interactive (often SSO) session, so no command in this SDK can
 * observe it. Every app-release result therefore carries this gate explicitly
 * instead of implying that a green API check means the UI works.
 */
export const VISUAL_UI_GATE = {
	gate: "visual-ui",
	status: "required",
	reason:
		"API key authenticates public REST only; use a pre-authenticated SSO browser session or dedicated UI test identity.",
	requiredAuthentication: "pre-authenticated-sso-or-dedicated-ui-test-identity",
	evidenceRequired:
		"Open the instance in the Dataiku Apps UI and exercise the affected tiles, forms, and actions to confirm the release behaves as intended.",
} as const;

export type AppInstanceCheckName =
	| "project-type"
	| "app-registration"
	| "manifest-version"
	| "manifest-references";

export interface AppInstanceCheck {
	check: AppInstanceCheckName;
	status: "ok" | "failed";
	expected: unknown;
	actual: unknown;
	/** Machine-readable cause when the compared values alone do not explain a failure. */
	reason?: string;
}

export interface AppManifestReferenceSummary {
	valid: boolean;
	manifestHash: string;
	checks: AppManifestValidationResult["checks"];
	errors: AppManifestValidationResult["errors"];
}

export interface AppInstanceVerification {
	appId: string;
	projectKey: string;
	projectAppType: string | null;
	registered: boolean;
	templateVersion: string | null;
	expectedVersion: string | null;
	expectedVersionSource: "expect-version" | "template-manifest";
	instanceVersion: string | null;
	instanceManifestHash: string | null;
	templateManifestHash: string;
	referenceValidation: AppManifestReferenceSummary | null;
	checks: AppInstanceCheck[];
	valid: boolean;
}

export interface SuccessorInstanceRequest {
	appId: string;
	from: string;
	to: string;
	name?: string;
	copyPermissions: boolean;
	dryRun: boolean;
	timeoutMs?: number;
	pollIntervalMs?: number;
}

/**
 * DSS answers the app-manifest probe with 400 ("neither an app template nor an
 * app instance") for ordinary projects and 404 for missing ones. Both mean "not
 * a verifiable app project", so they become a failed check instead of an error;
 * every other failure (transient, permission) keeps propagating so real faults
 * are never reported as a clean negative answer.
 */
async function readAppProject<T,>(reader: () => Promise<T>,): Promise<T | undefined> {
	try {
		return await readIfExists(reader,);
	} catch (error) {
		if (error instanceof DataikuError && error.status === 400) return undefined;
		throw error;
	}
}

function isPlainRecord(value: unknown,): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value,);
}

async function assertProjectIncarnation(
	client: DataikuClient,
	projectKey: string,
	expectedHash: string,
): Promise<void> {
	const details = await client.projects.get(projectKey,);
	const currentHash = projectIncarnationHash(projectKey, details,);
	if (currentHash === expectedHash) return;
	throw new UsageError(
		`Project ${projectKey} changed incarnation during successor verification.`,
		"validation_failed",
		"Refusing to read or write permissions on a replacement project. Re-run the successor workflow with a new target key.",
		{
			projectKey,
			expectedProjectIncarnationHash: expectedHash,
			currentProjectIncarnationHash: currentHash ?? null,
		},
	);
}

/**
 * The authoritative raw manifest version marker. DSS stores a free-form string,
 * so only a non-whitespace string is a usable version: a missing, blank or
 * non-string marker is reported as absent instead of being compared as if it
 * named a release.
 */
function rawVersion(manifest: unknown,): string | null {
	if (!isPlainRecord(manifest,)) return null;
	const value = manifest["version"];
	return typeof value === "string" && value.trim() !== "" ? value : null;
}

/**
 * Project type of the snapshot's project. It comes from the manifest snapshot
 * whenever DSS includes it and only otherwise costs a project-details request,
 * so the ordinary verification path never re-reads the manifest.
 */
async function snapshotAppType(
	client: DataikuClient,
	snapshot: Record<string, unknown>,
	projectKey: string,
): Promise<string | null> {
	const declared = snapshot["projectAppType"];
	if (typeof declared === "string") return declared;
	const details = await readIfExists(() => client.projects.get(projectKey,));
	const value = (details as Record<string, unknown> | undefined)?.["projectAppType"];
	return typeof value === "string" ? value : null;
}

function instanceCheck(
	check: AppInstanceCheckName,
	expected: unknown,
	actual: unknown,
	ok: boolean,
	reason?: string,
): AppInstanceCheck {
	return {
		check,
		status: ok ? "ok" : "failed",
		expected,
		actual,
		...(!ok && reason !== undefined ? { reason, } : {}),
	};
}

/** Never echoes response bodies: only the taxonomy DataikuError already deems safe. */
export function safeErrorSummary(error: unknown,): Record<string, unknown> {
	if (error instanceof DataikuError) {
		return {
			error: error.safeMessage,
			errorStatus: error.status,
			errorCategory: error.category,
			retryable: error.retryable,
		};
	}
	return { error: error instanceof Error ? error.message : String(error,), };
}

/**
 * Read-only API readiness of one app instance. Only checks that the public REST
 * API can answer authoritatively are performed; the UI gate stays external.
 *
 * The instance manifest is read exactly once: its version, hash and reference
 * validation all describe that one snapshot, so a concurrent manifest edit can
 * never produce a report whose fields contradict each other.
 */
export async function verifyAppInstance(
	client: DataikuClient,
	appId: string,
	projectKey: string,
	expectVersion?: string,
): Promise<AppInstanceVerification> {
	const templateManifest = await client.applications.getAppManifest(appId,);
	const templateVersion = rawVersion(templateManifest,);
	const expectedVersion = expectVersion ?? templateVersion;
	const instances = await client.applications.listInstances(appId,);
	const registered = instances.some((instance,) => instance.projectKey === projectKey);
	const raw = await readAppProject(() => client.applications.getInstanceManifest(projectKey,));
	const snapshot = isPlainRecord(raw,) ? raw : undefined;
	const projectAppType = snapshot ? await snapshotAppType(client, snapshot, projectKey,) : null;
	const instanceVersion = rawVersion(snapshot,);
	const referenceValidation = snapshot
		? await client.applications.validateAppManifest(snapshot, projectKey,)
		: undefined;
	// A template without a usable version is a failed required check even when
	// --expect-version was supplied: without it nothing pins what the instance
	// was supposed to inherit, so an equal pair of strings proves nothing.
	const checks = [
		instanceCheck("project-type", "APP_INSTANCE", projectAppType, projectAppType === "APP_INSTANCE",),
		instanceCheck("app-registration", appId, registered ? appId : null, registered,),
		instanceCheck(
			"manifest-version",
			expectedVersion,
			instanceVersion,
			templateVersion !== null && expectedVersion !== null
				&& instanceVersion === expectedVersion,
			templateVersion === null ? "template-version-missing" : undefined,
		),
		instanceCheck(
			"manifest-references",
			true,
			referenceValidation?.valid ?? null,
			referenceValidation?.valid === true,
		),
	];
	return {
		appId,
		projectKey,
		projectAppType,
		registered,
		templateVersion,
		expectedVersion,
		expectedVersionSource: expectVersion !== undefined ? "expect-version" : "template-manifest",
		instanceVersion,
		instanceManifestHash: snapshot ? stableHash(snapshot,) : null,
		templateManifestHash: stableHash(templateManifest,),
		referenceValidation: referenceValidation
			? {
				valid: referenceValidation.valid,
				manifestHash: referenceValidation.manifestHash,
				checks: referenceValidation.checks,
				errors: referenceValidation.errors,
			}
			: null,
		checks,
		valid: checks.every((entry,) => entry.status === "ok"),
	};
}

const SUCCESSOR_REMEDIATION =
	"The successor project may exist: replay the recorded cleanup entry, which future-gates deletion when DSS supplied a future ID. Without a cleanup entry, inspect the creation future and wait for it to become terminal before deleting the requested successor project. The predecessor project was not modified.";

const CREATE_REJECTED_REMEDIATION =
	"DSS refused the creation request, so no successor project was created: fix the reported problem and retry. The predecessor project was not modified.";

/** Stage a permission copy reached. Only `copied`/`unchanged` mean the successor holds them. */
export type AppPermissionCopyState =
	| "not-requested"
	| "unchanged"
	| "copied"
	| "not-attempted"
	| "rejected"
	| "unknown"
	| "completed-unverified";

/**
 * True only when DSS definitively refused a write: a non-retryable 4xx with a
 * classified client-side taxonomy. Transport failures (status 0), 5xx, retryable
 * 4xx (408/425/429) and unclassified statuses leave the outcome unknown, so the
 * effect of the request must be assumed possible.
 */
export function isDefinitiveRejection(error: unknown,): boolean {
	return error instanceof DataikuError && !error.retryable && error.category !== "unknown"
		&& error.status >= 400 && error.status < 500;
}

/**
 * Successor identity as reported by the creation future's own result.
 *
 * DSS names the project it created in `targetProjectKey`, and live instances of
 * this future are also observed carrying `projectKey` for the *source template*,
 * so the target-specific field always wins and `projectKey` speaks for the
 * target only when no usable `targetProjectKey` is present. A missing, blank or
 * non-string value is no identity claim at all, exactly as a blank manifest
 * version names no release.
 */
export function futureTargetIdentity(
	result: unknown,
): { projectKey: string; field: "targetProjectKey" | "projectKey"; } | null {
	if (!isPlainRecord(result,)) return null;
	for (const field of ["targetProjectKey", "projectKey",] as const) {
		const value = result[field];
		if (typeof value === "string" && value.trim() !== "") {
			return { projectKey: value.trim(), field, };
		}
	}
	return null;
}

interface SuccessorPreflight {
	sourceProjectAppType: string;
	sourceVersion: string | null;
	sourceManifestHash: string;
	targetProjectName: string;
	targetExists: false;
	templateManifestHash: string;
	templateVersion: string;
	sourcePermissions?: ProjectPermissions;
	sourcePermissionsHash?: string;
}

/**
 * Creation cleanup is safe only when the target was proven absent before POST.
 * A hidden 403 is not absence: project listing can prove a collision, but it
 * cannot prove that an inaccessible project does not exist.
 */
export async function requireAbsentProjectTarget(
	client: DataikuClient,
	targetProjectKey: string,
	targetFlag: "--to" | "targetProjectKey",
): Promise<void> {
	try {
		const existing = await readIfExists(() => client.projects.get(targetProjectKey,));
		if (existing === undefined) return;
	} catch (error) {
		if (!(error instanceof DataikuError) || error.status !== 403) throw error;
		const accessible = await client.projects.list();
		if (!accessible.some((project,) => project.projectKey === targetProjectKey)) {
			throw new UsageError(
				`Could not confirm target project ${targetProjectKey} is absent.`,
				"validation_failed",
				"Grant project-list/details visibility or choose a target key whose absence DSS can confirm; creation never risks cleanup against an unproven target.",
				{ targetProjectKey, targetFlag, targetProbe: "forbidden-and-not-listable", },
			);
		}
	}
	throw new UsageError(
		`Target project ${targetProjectKey} already exists.`,
		"validation_failed",
		`Choose an unused ${targetFlag}: app instance creation never writes into an existing project.`,
		{ targetProjectKey, targetFlag, },
	);
}

/**
 * Everything that can still refuse the request runs here, before the single
 * POST. Nothing in this function mutates DSS.
 */
async function preflightSuccessor(
	client: DataikuClient,
	request: SuccessorInstanceRequest,
	usage: string,
): Promise<SuccessorPreflight> {
	const { appId, from, to, } = request;
	if (from === to) {
		throw new UsageError(
			"--from and --to must be different project keys: a successor instance is created alongside its predecessor.",
			"validation_failed",
			`Usage: ${usage}`,
			{ sourceProjectKey: from, targetProjectKey: to, },
		);
	}
	const instances = await client.applications.listInstances(appId,);
	if (!instances.some((instance,) => instance.projectKey === from)) {
		throw new UsageError(
			`Project ${from} is not registered as an instance of app ${appId}.`,
			"validation_failed",
			"Check `dss app instances <appId>` and pass the predecessor instance project key to --from.",
			{
				appId,
				sourceProjectKey: from,
				registeredInstances: instances.map((instance,) => instance.projectKey ?? null),
			},
		);
	}
	const source = await readAppProject(() => client.applications.getManifestVersion(from,));
	if (!source) {
		throw new UsageError(
			`Project ${from} could not be read as an app project.`,
			"validation_failed",
			"--from must name an existing classic Dataiku App instance project (APP_INSTANCE).",
			{ sourceProjectKey: from, },
		);
	}
	if (source.projectAppType !== "APP_INSTANCE") {
		throw new UsageError(
			`Project ${from} is a ${
				source.projectAppType ?? "non-app"
			} project, not a classic Dataiku App instance.`,
			"validation_failed",
			"--from must name an APP_INSTANCE project; app templates are versioned with `dss app set-manifest-version`.",
			{ sourceProjectKey: from, projectAppType: source.projectAppType ?? null, },
		);
	}
	await requireAbsentProjectTarget(client, to, "--to",);
	const templateManifest = await client.applications.getAppManifest(appId,);
	const templateVersion = rawVersion(templateManifest,);
	if (templateVersion === null) {
		throw new UsageError(
			`App template ${appId} has no manifest version, so a successor instance cannot be verified against it.`,
			"validation_failed",
			"Set the template version first with `dss app set-manifest-version --manifest-version V --project-key <templateProjectKey>`.",
			{ appId, },
		);
	}
	const sourcePermissions = request.copyPermissions
		? await client.projects.getPermissions(from,)
		: undefined;
	return {
		sourceProjectAppType: source.projectAppType,
		sourceVersion: source.version,
		sourceManifestHash: source.manifestHash,
		targetProjectName: request.name ?? to,
		targetExists: false,
		templateManifestHash: stableHash(templateManifest,),
		templateVersion,
		...(sourcePermissions
			? {
				sourcePermissions,
				sourcePermissionsHash: appPermissionsHash(sourcePermissions,),
			}
			: {}),
	};
}

/**
 * Create a new app instance from the current template version alongside an
 * existing instance. Classic app instances have no public upgrade endpoint, so
 * the predecessor is never touched: it stays available until it is separately
 * and deliberately retired.
 *
 * After the creation POST this function never throws. A post-POST failure
 * returns a failed-wait result carrying the target project key, so the CLI still
 * exits 4 and the cleanup ledger still records an addressable successor.
 */
export async function createSuccessorInstance(
	client: DataikuClient,
	request: SuccessorInstanceRequest,
	usage: string,
): Promise<Record<string, unknown>> {
	const { appId, from, to, } = request;
	const preflight = await preflightSuccessor(client, request, usage,);
	const source = {
		projectKey: from,
		projectAppType: preflight.sourceProjectAppType,
		version: preflight.sourceVersion,
		manifestHash: preflight.sourceManifestHash,
		...(preflight.sourcePermissionsHash
			? { permissionsHash: preflight.sourcePermissionsHash, }
			: {}),
	};
	const shared = {
		appId,
		source,
		templateVersion: preflight.templateVersion,
		templateManifestHash: preflight.templateManifestHash,
		additive: true,
		sourcePreserved: true,
		uiPublicationVerified: false,
		requiredExternalGates: [VISUAL_UI_GATE,],
		targetPreflight: "confirmed-absent",
	};
	if (request.dryRun) {
		return {
			dryRun: true,
			action: "create-successor-instance",
			...shared,
			target: {
				projectKey: to,
				name: preflight.targetProjectName,
				exists: false,
			},
			copyPermissions: request.copyPermissions,
			preflight: "passed",
		};
	}

	let created: unknown;
	try {
		created = await client.applications.createInstance(appId, {
			targetProjectKey: to,
			targetProjectName: preflight.targetProjectName,
		},);
	} catch (error) {
		const rejected = isDefinitiveRejection(error,);
		return {
			...shared,
			projectKey: to,
			target: { projectKey: to, name: preflight.targetProjectName, },
			success: false,
			state: "CREATE_FAILED",
			elapsedMs: 0,
			pollCount: 0,
			stage: "create",
			cleanupEligible: !rejected,
			remediation: rejected ? CREATE_REJECTED_REMEDIATION : SUCCESSOR_REMEDIATION,
			...safeErrorSummary(error,),
		};
	}

	// DSS names the target project in `targetProjectKey` first; `projectKey` is
	// only consulted when the target-specific field is absent, because live
	// responses also echo `projectKey` for the *source* template.
	const instance = isPlainRecord(created,) ? created : undefined;
	const jobId = instance ? stringField(instance, ["jobId",],) : undefined;
	const inlineState = instance?.hasResult === true ? instance : undefined;
	const echoedTarget = instance
		? stringField(instance, ["targetProjectKey", "projectKey",],)?.trim()
		: undefined;
	if (echoedTarget !== undefined && echoedTarget !== to) {
		return {
			...shared,
			projectKey: to,
			target: { projectKey: to, name: preflight.targetProjectName, },
			success: false,
			state: "VERIFICATION_FAILED",
			elapsedMs: 0,
			pollCount: 0,
			stage: "create",
			...(instance ? { instance, } : {}),
			...(jobId !== undefined ? { futureId: jobId, jobId, } : {}),
			cleanupEligible: true,
			error: "DSS named a different target project in the creation response.",
			expected: { projectKey: to, },
			actual: { projectKey: echoedTarget, },
			remediation: SUCCESSOR_REMEDIATION,
		};
	}
	const base = {
		...shared,
		projectKey: to,
		target: { projectKey: to, name: preflight.targetProjectName, },
		...(instance ? { instance, } : {}),
	};
	let waited: FutureWaitResult;
	if (inlineState) {
		// `hasResult` on the creation response is the future's own verdict: the
		// answer arrived with the POST, so polling is skipped entirely and the
		// verification below runs on the delivered result.
		waited = {
			...(jobId !== undefined ? { futureId: jobId, jobId, } : {}),
			state: "DONE",
			elapsedMs: 0,
			pollCount: 0,
			success: true,
			hasResult: true,
			...(isPlainRecord(inlineState.result,) ? { result: inlineState.result, } : {}),
		} as FutureWaitResult;
	} else if (!jobId) {
		return {
			...base,
			...(instance ? {} : {
				responseKind: created === undefined ? "empty" : created === null
					? "null"
					: Array.isArray(created,)
					? "array"
					: "scalar",
			}),
			success: false,
			state: "UNTRACKABLE",
			elapsedMs: 0,
			pollCount: 0,
			stage: "future-wait",
			cleanupEligible: true,
			error:
				"DSS returned no instance-creation future ID, so the successor instance cannot be verified.",
			remediation: SUCCESSOR_REMEDIATION,
		};
	} else {
		const startedAt = Date.now();
		try {
			waited = await client.futures.wait(jobId, {
				pollIntervalMs: request.pollIntervalMs,
				timeoutMs: request.timeoutMs,
			},);
		} catch (error) {
			return {
				...base,
				jobId,
				success: false,
				state: "WAIT_FAILED",
				elapsedMs: Date.now() - startedAt,
				pollCount: 0,
				stage: "future-wait",
				cleanupEligible: true,
				remediation: SUCCESSOR_REMEDIATION,
				...safeErrorSummary(error,),
			};
		}
	}
	// `projectKey` is re-pinned after the future spread: nothing DSS answers can
	// rename the cleanup target away from the requested successor key.
	const waitedBase = { ...base, ...waited, jobId, projectKey: to, };
	// Proof that the terminal creation future named this exact target. It is set
	// once, below, and then carried by every later result: a creation that DSS
	// already settled on this project stays settled even when verification or the
	// permission copy afterwards fails, so the cleanup ledger needs no second
	// future gate to delete the successor.
	let targetEvidence: { futureTargetVerified?: true; projectIncarnationHash?: string; } = {};
	const failure = (
		state: string,
		stage: string,
		extra: Record<string, unknown>,
	): Record<string, unknown> => ({
		...waitedBase,
		...extra,
		success: false,
		state,
		stage,
		cleanupEligible: true,
		remediation: SUCCESSOR_REMEDIATION,
		...targetEvidence,
	});
	// The future's result is the last place DSS can name the project it actually
	// created. Extract that identity even when the terminal future failed: later
	// cleanup should not depend on an ephemeral future once DSS explicitly named
	// this target, while a disagreement must never redirect cleanup.
	const futureTarget = futureTargetIdentity(waited.result,);
	if (futureTarget !== null && futureTarget.projectKey !== to) {
		return failure("VERIFICATION_FAILED", "future-target", {
			futureTargetMismatch: true,
			futureTargetField: futureTarget.field,
			expected: { projectKey: to, },
			actual: { projectKey: futureTarget.projectKey, },
			error:
				"The instance-creation future reported a different successor project than the one requested.",
		},);
	}
	if (futureTarget?.projectKey === to) targetEvidence = { futureTargetVerified: true, };

	if (!waited.success) {
		return failure(waited.state, "future-wait", {
			error: "The instance-creation future did not complete successfully.",
		},);
	}

	try {
		const details = await client.projects.get(to,);
		const incarnation = projectIncarnationHash(to, details,);
		if (!incarnation) {
			return failure("INCARNATION_UNVERIFIED", "project-incarnation", {
				error:
					"DSS did not return creationTag identity for the successor project. Refusing cleanup without project-incarnation binding.",
			},);
		}
		targetEvidence = { ...targetEvidence, projectIncarnationHash: incarnation, };
	} catch (error) {
		return failure(
			"INCARNATION_UNVERIFIED",
			"project-incarnation",
			safeErrorSummary(error,),
		);
	}
	const boundTargetIncarnation = targetEvidence.projectIncarnationHash;
	if (!boundTargetIncarnation) {
		return failure("INCARNATION_UNVERIFIED", "project-incarnation", {
			error: "The successor project incarnation could not be retained for verification.",
		},);
	}

	let verification: AppInstanceVerification;
	try {
		verification = await verifyAppInstance(
			client,
			appId,
			to,
			preflight.templateVersion,
		);
		await assertProjectIncarnation(client, to, boundTargetIncarnation,);
	} catch (error) {
		return failure("VERIFICATION_FAILED", "verification", safeErrorSummary(error,),);
	}
	const verified = {
		instanceVersion: verification.instanceVersion,
		instanceManifestHash: verification.instanceManifestHash,
		checks: verification.checks,
		referenceValidation: verification.referenceValidation,
	};
	// The template was pinned during preflight and verification re-reads it. A
	// changed version proves the successor came from a different release; an
	// unchanged version over a changed manifest hash proves the release's own
	// content moved underneath it. Both signals are reported independently so a
	// content-only edit is never dressed up as a version bump, nor the reverse.
	const templateVersionDrift = verification.templateVersion !== preflight.templateVersion;
	const templateManifestDrift = verification.templateManifestHash !== preflight.templateManifestHash;
	if (templateVersionDrift || templateManifestDrift) {
		return failure("VERIFICATION_FAILED", "verification", {
			...verified,
			templateVersionDrift,
			templateManifestDrift,
			expectedTemplateVersion: preflight.templateVersion,
			actualTemplateVersion: verification.templateVersion,
			expectedTemplateManifestHash: preflight.templateManifestHash,
			actualTemplateManifestHash: verification.templateManifestHash,
			error: templateVersionDrift
				? "The template manifest version changed after the successor creation request was accepted."
				: "The template manifest content changed after the successor creation request was accepted, although its version marker did not.",
		},);
	}
	if (!verification.valid) {
		return failure("VERIFICATION_FAILED", "verification", {
			...verified,
			error: "The successor instance did not pass every required API check.",
		},);
	}

	const desiredHash = preflight.sourcePermissionsHash;
	const sourcePermissions = preflight.sourcePermissions;
	const copyFailure = (
		state: AppPermissionCopyState,
		summary: Record<string, unknown> = {},
	): Record<string, unknown> => ({
		...verified,
		permissions: {
			requested: true,
			// Never overclaim: only a verified copy reports true; an unresolved
			// PUT outcome or failed verification read reports null, and every
			// stage that can prove no copy happened reports false.
			copied: state === "copied"
				? true
				: state === "unknown" || state === "completed-unverified"
				? null
				: false,
			state,
			sourceHash: desiredHash,
			verified: false,
			...summary,
		},
	});
	let permissions: Record<string, unknown> = {
		requested: false,
		copied: false,
		state: "not-requested",
		verified: false,
	};
	if (sourcePermissions && desiredHash) {
		// The stage variable separates failures that happened before the write
		// (the copy was definitely never started) from a definitive PUT rejection,
		// an indeterminate PUT outcome, and a resolved PUT whose verification
		// read failed. "recheck" is the source-ACL drift guard: the predecessor
		// snapshot is hashed again immediately before the target write.
		let stage: "before-read" | "recheck" | "put" | "after-read" = "before-read";
		try {
			const before = await client.projects.getPermissions(to,);
			await assertProjectIncarnation(client, to, boundTargetIncarnation,);
			const beforeHash = appPermissionsHash(before,);
			// The predecessor ACL may have changed since preflight. Re-read it
			// even when the target already has the old hash: "unchanged" is only
			// true when source and target still agree at this decision point.
			stage = "recheck";
			const sourceNow = await client.projects.getPermissions(from,);
			const sourceNowHash = appPermissionsHash(sourceNow,);
			if (sourceNowHash !== desiredHash) {
				return failure("PERMISSIONS_FAILED", "permissions", {
					...copyFailure("not-attempted", {
						beforeHash,
						sourceDrift: true,
						sourceHashNow: sourceNowHash,
					},),
					error:
						"The predecessor ACL changed after preflight, so the stale snapshot was not copied to the successor.",
				},);
			}
			await assertProjectIncarnation(client, to, boundTargetIncarnation,);
			if (beforeHash === desiredHash) {
				permissions = {
					requested: true,
					copied: false,
					state: "unchanged",
					reason: "unchanged",
					sourceHash: desiredHash,
					beforeHash,
					verifiedHash: beforeHash,
					verified: true,
				};
			} else {
				stage = "put";
				await client.projects.setPermissions(to, sourceNow,);
				stage = "after-read";
				const after = await client.projects.getPermissions(to,);
				await assertProjectIncarnation(client, to, boundTargetIncarnation,);
				const verifiedHash = appPermissionsHash(after,);
				if (verifiedHash !== desiredHash) {
					return failure("PERMISSIONS_FAILED", "permissions", {
						...copyFailure("rejected", {
							beforeHash,
							verifiedHash,
							reason: "verified-hash-mismatch",
						},),
						error: appPermissionsVerificationError(to, {
							beforeHash,
							desiredHash,
							verifiedHash,
						},).message,
					},);
				}
				permissions = {
					requested: true,
					copied: true,
					state: "copied",
					sourceHash: desiredHash,
					beforeHash,
					verifiedHash,
					verified: true,
				};
			}
		} catch (error) {
			return failure(
				"PERMISSIONS_FAILED",
				"permissions",
				copyFailure(
					stage === "before-read" || stage === "recheck"
						? "not-attempted"
						: stage === "put" && isDefinitiveRejection(error,)
						? "rejected"
						: stage === "put"
						? "unknown"
						: "completed-unverified",
					safeErrorSummary(error,),
				),
			);
		}
	}

	return { ...waitedBase, ...verified, permissions, ...targetEvidence, };
}
