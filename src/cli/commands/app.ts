import type { DataikuClient, } from "../../client.js";
import { ClientValidationError, DataikuError, } from "../../errors.js";
import type { FutureWaitResult, } from "../../schemas.js";
import { projectIncarnationHash, } from "../../utils/project-incarnation.js";
import {
	jsonInput,
	num,
	parseBooleanOption,
	plainRecord,
	requiredJsonInput,
	requiredStringFlag,
	stringField,
} from "../coerce.js";
import {
	appPermissionsHash,
	appPermissionsVerificationError,
	assertAppPermissionsSnapshotBinding,
	buildAppPermissionsSnapshot,
	diffAppPermissions,
	readAppPermissionsSnapshot,
	writeAppPermissionsSnapshot,
} from "../helpers/app-permissions.js";
import {
	createSuccessorInstance,
	futureTargetIdentity,
	isDefinitiveRejection,
	requireAbsentProjectTarget,
	safeErrorSummary,
	verifyAppInstance,
	VISUAL_UI_GATE,
} from "../helpers/app-successor.js";
import { CommandResultFailure, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, requireNoArgs, UsageError, } from "../usage.js";

const CREATE_INSTANCE_USAGE =
	"dss app create-instance <appId> (--data JSON|--data-file PATH|--stdin) [--wait] [--timeout MS] [--poll-interval MS]";
const MANIFEST_VERSION_USAGE = "dss app manifest-version [--project-key KEY]";
const SET_MANIFEST_VERSION_USAGE =
	"dss app set-manifest-version (--manifest-version V|--version-notes NOTES) [--expect-hash SHA256] [--dry-run] [--project-key KEY]";
const CREATE_SUCCESSOR_USAGE =
	"dss app create-successor-instance <appId> --from KEY --to KEY [--name NAME] [--copy-permissions] [--timeout MS] [--poll-interval MS] [--dry-run] [--record-cleanup PATH]";
const VERIFY_INSTANCE_USAGE =
	"dss app verify-instance <appId> --project-key KEY [--expect-version V]";
const DELETE_INSTANCE_USAGE =
	"dss app delete-instance --project-key KEY [--future-id ID] [--expect-project-incarnation SHA256] [--unconfirmed-creation] [--timeout MS] [--poll-interval MS]";
const CREATE_INSTANCE_INDETERMINATE_REMEDIATION =
	"DSS may have accepted the creation, but no future ID proves when it settles. The recorded cleanup entry intentionally stops as unresolved; inspect `dss app instances <appId>` and retry deletion only after creation is known terminal.";
const CREATE_INSTANCE_REJECTED_REMEDIATION =
	"DSS refused the creation request, so no instance project was created: fix the reported problem and retry.";
const DELETE_INSTANCE_FUTURE_REMEDIATION =
	"The supplied creation future could not authorize deletion. Inspect that future and the target project, then retry only with a future whose terminal result reports this project.";
const DELETE_INSTANCE_UNCONFIRMED_REMEDIATION =
	"Creation may still be running and DSS returned no future ID. Inspect the app's instances and wait until creation is known terminal before running delete-instance without --unconfirmed-creation.";
const DELETE_INSTANCE_DELETE_REMEDIATION =
	"Future safety checks passed, but the project DELETE outcome is unresolved. Inspect the target project; if it still exists, retry delete-instance.";
const DELETE_INSTANCE_TARGET_VALIDATION_REMEDIATION =
	"Deletion was not attempted: validating the target project failed before any DELETE request, so no project was deleted by this command. Fix the reported problem and retry.";

/**
 * Only a DataikuError whose retry metadata names the DELETE method proves the
 * DELETE request was issued: pre-delete manifest/details GET failures and
 * client-side validation errors carry retry.method "GET" (or none), which
 * means the deletion itself was never attempted.
 */
function deleteAttemptedBy(error: unknown,): boolean {
	return error instanceof DataikuError && error.retry?.method === "DELETE";
}

function deleteFailureClassification(error: unknown,): {
	deletePerformed: boolean | null;
	remediation: string;
} {
	if (deleteAttemptedBy(error,)) {
		return error instanceof ClientValidationError || isDefinitiveRejection(error,)
			? { deletePerformed: false, remediation: DELETE_INSTANCE_DELETE_REMEDIATION, }
			: { deletePerformed: null, remediation: DELETE_INSTANCE_DELETE_REMEDIATION, };
	}
	return {
		deletePerformed: false,
		remediation: DELETE_INSTANCE_TARGET_VALIDATION_REMEDIATION,
	};
}

async function requireProjectIncarnationHash(
	client: DataikuClient,
	projectKey: string,
): Promise<string> {
	const details = await client.projects.get(projectKey,);
	const hash = projectIncarnationHash(projectKey, details,);
	if (hash) return hash;
	throw new ClientValidationError(
		`DSS did not return creationTag identity for project ${projectKey}.`,
		"validation_failed",
		"Refusing an incarnation-sensitive operation because project-key reuse cannot be excluded.",
		{ projectKey, },
	);
}

async function assertProjectIncarnationHash(
	client: DataikuClient,
	projectKey: string,
	expected: string,
): Promise<void> {
	const current = await requireProjectIncarnationHash(client, projectKey,);
	if (current === expected) return;
	throw new ClientValidationError(
		`Project ${projectKey} is not the same project incarnation recorded for this operation.`,
		"validation_failed",
		"Refusing to continue after project-key reuse. Re-read the current project and retry only with an artifact captured from that incarnation.",
		{ projectKey, expectedProjectIncarnationHash: expected, currentProjectIncarnationHash: current, },
	);
}

export const appCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c,) => c.applications.listApps(),
		usage: "dss app list",
		description: "List all Dataiku App templates.",
		examples: ["dss app list",],
	},
	manifest: {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss app manifest <appId>",);
			return c.applications.getAppManifest(a[0],);
		},
		usage: "dss app manifest <appId>",
		description: "Get the manifest of a Dataiku App template.",
		examples: ["dss app manifest my-app",],
	},
	instances: {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss app instances <appId>",);
			return c.applications.listInstances(a[0],);
		},
		usage: "dss app instances <appId>",
		description: "List instances created from a Dataiku App template.",
		examples: ["dss app instances my-app",],
	},
	"manifest-version": {
		handler: (c, a, f,) => {
			requireNoArgs(a, MANIFEST_VERSION_USAGE,);
			return c.applications.getManifestVersion(f["project-key"] as string | undefined,);
		},
		usage: MANIFEST_VERSION_USAGE,
		description:
			"Read the app-manifest version of a Dataiku App template or app-instance project: the raw persisted string only, never a synthesized default.",
		examples: ["dss app manifest-version --project-key MYAPP_TEMPLATE",],
	},
	"verify-instance": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, VERIFY_INSTANCE_USAGE,);
			const projectKey = requiredStringFlag(f, "project-key", VERIFY_INSTANCE_USAGE,);
			const result = await verifyAppInstance(
				c,
				a[0],
				projectKey,
				f["expect-version"] as string | undefined,
			);
			if (!result.valid) {
				throw new UsageError(
					"App instance failed required API checks.",
					"validation_failed",
					"Fix the failing checks, or create a new successor instance from the current template version.",
					{ result: { ...result, requiredExternalGates: [VISUAL_UI_GATE,], }, },
				);
			}
			return {
				...result,
				status: "API_VERIFIED_UI_PENDING",
				apiReady: true,
				uiPublicationVerified: false,
				requiredExternalGates: [VISUAL_UI_GATE,],
			};
		},
		usage: VERIFY_INSTANCE_USAGE,
		description:
			"Read-only API readiness check for an app instance: project type, registration under the app, authoritative raw manifest version, and manifest reference validation. The visual UI remains an explicit external SSO gate.",
		examples: [
			"dss app verify-instance my-app --project-key RELEASE_INSTANCE",
			"dss app verify-instance my-app --project-key RELEASE_INSTANCE --expect-version 2.0.0",
		],
	},
	"create-instance": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, CREATE_INSTANCE_USAGE,);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (instance creation payload).",
			);
			const requestedProjectKey = stringField(body, ["targetProjectKey",],)?.trim();
			if (!requestedProjectKey) {
				throw new UsageError(
					"Instance creation payload must include a non-empty targetProjectKey.",
					"validation_failed",
					`Usage: ${CREATE_INSTANCE_USAGE}`,
				);
			}
			body.targetProjectKey = requestedProjectKey;
			await requireAbsentProjectTarget(c, requestedProjectKey, "targetProjectKey",);
			let created: unknown;
			try {
				created = await c.applications.createInstance(a[0], body,);
			} catch (error) {
				// A definitive rejection proves nothing was created; anything else
				// (5xx, retryable 4xx, transport failure) leaves the outcome
				// unknown and stays cleanup-eligible against the requested key.
				const rejected = isDefinitiveRejection(error,);
				return {
					success: false,
					state: "CREATE_FAILED",
					elapsedMs: 0,
					pollCount: 0,
					projectKey: requestedProjectKey,
					cleanupEligible: !rejected,
					remediation: rejected
						? CREATE_INSTANCE_REJECTED_REMEDIATION
						: CREATE_INSTANCE_INDETERMINATE_REMEDIATION,
					...safeErrorSummary(error,),
				};
			}
			const instance = plainRecord(created,);
			if (!instance) {
				return {
					success: false,
					state: "UNTRACKABLE",
					elapsedMs: 0,
					pollCount: 0,
					projectKey: requestedProjectKey,
					cleanupEligible: true,
					responseKind: created === undefined
						? "empty"
						: created === null
						? "null"
						: Array.isArray(created,)
						? "array"
						: "scalar",
					error: "DSS accepted instance creation without returning a structured creation state.",
					remediation: CREATE_INSTANCE_INDETERMINATE_REMEDIATION,
				};
			}
			// The create endpoint's `jobId` is a DSS future ID (the official client wraps it in
			// DSSFuture), not a project Flow job ID.
			const jobId = stringField(instance, ["jobId",],);
			// The requested key stays authoritative: a response that names a
			// different project fails instead of redirecting output or cleanup.
			const echoedTarget = stringField(instance, ["targetProjectKey", "projectKey",],)?.trim();
			if (echoedTarget !== undefined && echoedTarget !== requestedProjectKey) {
				return {
					success: false,
					state: "VERIFICATION_FAILED",
					elapsedMs: 0,
					pollCount: 0,
					projectKey: requestedProjectKey,
					instance,
					...(jobId !== undefined ? { futureId: jobId, jobId, } : {}),
					cleanupEligible: true,
					error: "DSS named a different target project in the creation response.",
					expected: { projectKey: requestedProjectKey, },
					actual: { projectKey: echoedTarget, },
					remediation: CREATE_INSTANCE_INDETERMINATE_REMEDIATION,
				};
			}
			if (instance["hasResult"] === true) {
				const futureTarget = futureTargetIdentity(instance.result,);
				if (futureTarget !== null && futureTarget.projectKey !== requestedProjectKey) {
					return {
						success: false,
						state: "VERIFICATION_FAILED",
						elapsedMs: 0,
						pollCount: 0,
						projectKey: requestedProjectKey,
						instance,
						...(jobId !== undefined ? { futureId: jobId, jobId, } : {}),
						cleanupEligible: true,
						error: "The instance-creation future named a different target project.",
						expected: { projectKey: requestedProjectKey, },
						actual: {
							projectKey: futureTarget.projectKey,
							field: futureTarget.field,
						},
						remediation: CREATE_INSTANCE_INDETERMINATE_REMEDIATION,
					};
				}
				let projectIncarnation: string;
				try {
					projectIncarnation = await requireProjectIncarnationHash(c, requestedProjectKey,);
				} catch (error) {
					return {
						success: false,
						state: "INCARNATION_UNVERIFIED",
						hasResult: true,
						elapsedMs: 0,
						pollCount: 0,
						projectKey: requestedProjectKey,
						instance,
						...(jobId !== undefined ? { futureId: jobId, jobId, } : {}),
						...(futureTarget?.projectKey === requestedProjectKey
							? { futureTargetVerified: true, }
							: {}),
						cleanupEligible: true,
						remediation: CREATE_INSTANCE_INDETERMINATE_REMEDIATION,
						...safeErrorSummary(error,),
					};
				}
				// Inline completion: the future answered with the POST itself, so
				// there is nothing to poll and no live future left to clean up.
				return {
					success: true,
					state: "DONE",
					hasResult: true,
					elapsedMs: 0,
					pollCount: 0,
					projectKey: requestedProjectKey,
					instance,
					projectIncarnationHash: projectIncarnation,
					...(jobId !== undefined ? { futureId: jobId, jobId, } : {}),
					...(futureTarget?.projectKey === requestedProjectKey
						? { futureTargetVerified: true, }
						: {}),
				};
			}
			if (f["wait"] !== true) {
				return {
					projectKey: requestedProjectKey,
					instance,
					...(jobId !== undefined ? { futureId: jobId, jobId, } : {}),
				};
			}
			if (!jobId) {
				return {
					success: false,
					state: "UNTRACKABLE",
					elapsedMs: 0,
					pollCount: 0,
					projectKey: requestedProjectKey,
					instance,
					cleanupEligible: true,
					error:
						"DSS returned no instance-creation future ID. The instance may still have been created; inspect app instances or replay the recorded cleanup entry.",
					remediation: CREATE_INSTANCE_INDETERMINATE_REMEDIATION,
				};
			}
			let waited: FutureWaitResult;
			try {
				waited = await c.futures.wait(jobId, {
					pollIntervalMs: num(f["poll-interval"],),
					timeoutMs: num(f["timeout"],),
				},);
			} catch (error) {
				return {
					success: false,
					state: "WAIT_FAILED",
					elapsedMs: 0,
					pollCount: 0,
					projectKey: requestedProjectKey,
					instance,
					jobId,
					cleanupEligible: true,
					remediation: CREATE_INSTANCE_INDETERMINATE_REMEDIATION,
					...safeErrorSummary(error,),
				};
			}
			const waitedTarget = futureTargetIdentity(waited.result,);
			if (waitedTarget !== null && waitedTarget.projectKey !== requestedProjectKey) {
				return {
					...waited,
					success: false,
					state: "VERIFICATION_FAILED",
					projectKey: requestedProjectKey,
					instance,
					jobId,
					cleanupEligible: true,
					error: "The instance-creation future named a different target project.",
					expected: { projectKey: requestedProjectKey, },
					actual: {
						projectKey: waitedTarget.projectKey,
						field: waitedTarget.field,
					},
					remediation: CREATE_INSTANCE_INDETERMINATE_REMEDIATION,
				};
			}
			let projectIncarnation: string | undefined;
			if (waited.success) {
				try {
					projectIncarnation = await requireProjectIncarnationHash(c, requestedProjectKey,);
				} catch (error) {
					return {
						...waited,
						success: false,
						state: "INCARNATION_UNVERIFIED",
						projectKey: requestedProjectKey,
						instance,
						jobId,
						...(waitedTarget?.projectKey === requestedProjectKey
							? { futureTargetVerified: true, }
							: {}),
						cleanupEligible: true,
						remediation: CREATE_INSTANCE_INDETERMINATE_REMEDIATION,
						...safeErrorSummary(error,),
					};
				}
			}
			return {
				...waited,
				instance,
				jobId,
				projectKey: requestedProjectKey,
				...(projectIncarnation !== undefined
					? { projectIncarnationHash: projectIncarnation, }
					: {}),
				...(waitedTarget?.projectKey === requestedProjectKey
					? { futureTargetVerified: true, }
					: {}),
			};
		},
		usage: CREATE_INSTANCE_USAGE,
		description:
			"Create an app instance from a Dataiku App template, optionally waiting for the instance-creation future returned by DSS.",
		examples: [
			'dss app create-instance my-app --data \'{"targetProjectKey":"NEWPROJ"}\'',
			'dss app create-instance my-app --data \'{"targetProjectKey":"NEWPROJ"}\' --wait --timeout 120000 --poll-interval 2000',
		],
	},
	"create-successor-instance": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, CREATE_SUCCESSOR_USAGE,);
			return createSuccessorInstance(c, {
				appId: a[0],
				from: requiredStringFlag(f, "from", CREATE_SUCCESSOR_USAGE,),
				to: requiredStringFlag(f, "to", CREATE_SUCCESSOR_USAGE,),
				...(f["name"] !== undefined ? { name: f["name"] as string, } : {}),
				copyPermissions: parseBooleanOption(f["copy-permissions"], "--copy-permissions",) ?? false,
				dryRun: parseBooleanOption(f["dry-run"], "--dry-run",) ?? false,
				timeoutMs: num(f["timeout"],),
				pollIntervalMs: num(f["poll-interval"],),
			}, CREATE_SUCCESSOR_USAGE,);
		},
		usage: CREATE_SUCCESSOR_USAGE,
		description:
			"Create a new app instance from the current template version alongside an existing instance. The predecessor is never modified and is retired separately and deliberately; the creation future is always awaited.",
		examples: [
			"dss app create-successor-instance my-app --from OLD_INSTANCE --to NEW_INSTANCE",
			'dss app create-successor-instance my-app --from OLD_INSTANCE --to NEW_INSTANCE --name "Release 2" --copy-permissions',
		],
	},
	"instance-manifest": {
		handler: (c, a, f,) => {
			requireNoArgs(a, "dss app instance-manifest [--project-key KEY]",);
			return c.applications.getInstanceManifest(f["project-key"] as string | undefined,);
		},
		usage: "dss app instance-manifest [--project-key KEY]",
		description: "Get the app manifest of a Dataiku App template or app-instance project.",
		examples: ["dss app instance-manifest --project-key MYINSTANCE",],
	},
	"save-instance-manifest": {
		handler: (c, a, f,) => {
			requireNoArgs(
				a,
				"dss app save-instance-manifest (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			);
			const manifest = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (manifest JSON).",
			);
			return c.applications.saveInstanceManifest(manifest, f["project-key"] as string | undefined,);
		},
		usage:
			"dss app save-instance-manifest (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description:
			"Save the app manifest of a Dataiku App template project (homepage sections, use-as-recipe settings). Classic app-instance project manifests are read-only through this endpoint.",
		examples: [
			"dss app save-instance-manifest --data-file manifest.json --project-key MYAPP_TEMPLATE",
		],
	},
	"set-manifest-version": {
		handler: async (c, a, f,) => {
			requireNoArgs(a, SET_MANIFEST_VERSION_USAGE,);
			const version = f["manifest-version"] as string | undefined;
			const versionNotes = f["version-notes"] as string | undefined;
			if (version === undefined && versionNotes === undefined) {
				throw new UsageError(
					"At least one of --manifest-version or --version-notes is required.",
					"usage_error",
					`Usage: ${SET_MANIFEST_VERSION_USAGE}`,
				);
			}
			// Shape validation runs before any client access: a batch dry-run
			// executes handlers against a throwing proxy client, so invalid
			// versions and hashes must fail here to keep the dry-run truthful.
			if (
				version !== undefined
				&& (typeof version !== "string" || version.trim() === "")
			) {
				throw new ClientValidationError(
					"App manifest version must be a non-empty string.",
					"validation_failed",
					"Pass the version string DSS should display in the Application header, for example 1.2.0.",
					{ projectKey: f["project-key"] ?? null, },
				);
			}
			if (versionNotes !== undefined && typeof versionNotes !== "string") {
				throw new ClientValidationError(
					"App manifest version notes must be a string.",
					"validation_failed",
					"Pass release notes as text, or an empty string to clear them.",
					{ projectKey: f["project-key"] ?? null, },
				);
			}
			const expectHash = f["expect-hash"] as string | undefined;
			if (
				expectHash !== undefined
				&& (typeof expectHash !== "string" || !/^[0-9a-fA-F]{64}$/.test(expectHash,))
			) {
				throw new ClientValidationError(
					"Expected manifest hash must be a 64-character SHA-256 hex digest.",
					"validation_failed",
					"Use the manifestHash value returned by the manifest-version read.",
					{ projectKey: f["project-key"] ?? null, },
				);
			}
			const result = await c.applications.setManifestVersion({
				...(version !== undefined ? { version, } : {}),
				...(versionNotes !== undefined ? { versionNotes, } : {}),
				...(f["expect-hash"] !== undefined ? { expectHash: f["expect-hash"] as string, } : {}),
				dryRun: parseBooleanOption(f["dry-run"], "--dry-run",) ?? false,
			}, f["project-key"] as string | undefined,);
			if (result.outcome === "indeterminate") {
				// The PUT may or may not have landed. Fail nonzero without
				// claiming publication: the structured result travels in the
				// error details, and direct/batch runs surface exit code 2.
				throw new CommandResultFailure(
					{
						...result,
						publicationTransport: "public-app-manifest",
						uiPublicationVerified: false,
					},
					2,
					"ambiguous_outcome",
				);
			}
			return {
				...result,
				publicationTransport: "public-app-manifest",
				uiPublicationVerified: false,
			};
		},
		usage: SET_MANIFEST_VERSION_USAGE,
		description:
			"Set the raw app-manifest version and/or version notes of a Dataiku App template through the public app-manifest endpoint. This is not a publish transaction: the persisted string becomes the template version that new instances inherit, and the visual UI remains an external gate.",
		examples: [
			"dss app set-manifest-version --manifest-version 2.0.0 --project-key MYAPP_TEMPLATE",
			'dss app set-manifest-version --version-notes "Release 2" --expect-hash 480039fce035bfcd98740cb4fd9e67763b8ba68cac43d16a6e25bd9abb7548e8 --dry-run',
		],
	},
	"validate-manifest": {
		handler: async (c, a, f,) => {
			requireNoArgs(
				a,
				"dss app validate-manifest [--data JSON|--data-file PATH|--stdin] [--project-key KEY]",
			);
			const payload = jsonInput(f,);
			const manifest = payload ?? await c.applications.getInstanceManifest(
				f["project-key"] as string | undefined,
			);
			const result = await c.applications.validateAppManifest(
				manifest,
				f["project-key"] as string | undefined,
			);
			if (!result.valid) {
				throw new UsageError(
					"App manifest validation failed.",
					"validation_failed",
					undefined,
					{ result, },
				);
			}
			return result;
		},
		usage: "dss app validate-manifest [--data JSON|--data-file PATH|--stdin] [--project-key KEY]",
		description:
			"Validate an app manifest against source-verifiable reference data (scenario IDs, managed-folder IDs, variable names). Without a payload, reads the target project's app manifest.",
		examples: [
			"dss app validate-manifest --data-file manifest.json --project-key MYAPP_TEMPLATE",
			"dss app validate-manifest --project-key MYAPP_TEMPLATE",
		],
	},
	"compare-manifest": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss app compare-manifest <appId> --project-key KEY",);
			const projectKey = requiredStringFlag(
				f,
				"project-key",
				"dss app compare-manifest <appId> --project-key KEY",
			);
			return c.applications.compareAppManifest(a[0], projectKey,);
		},
		usage: "dss app compare-manifest <appId> --project-key KEY",
		description:
			"Compare a Dataiku App template with the target app instance (normalized hashes and deterministic path-level differences; only project identity fields are omitted).",
		examples: ["dss app compare-manifest my-app --project-key MYINSTANCE",],
	},
	"delete-instance": {
		handler: async (c, a, f,) => {
			requireNoArgs(a, DELETE_INSTANCE_USAGE,);
			const projectKey = requiredStringFlag(f, "project-key", DELETE_INSTANCE_USAGE,);
			const futureId = f["future-id"] === undefined
				? undefined
				: requiredStringFlag(f, "future-id", DELETE_INSTANCE_USAGE,);
			const expectedProjectIncarnation = f["expect-project-incarnation"] === undefined
				? undefined
				: requiredStringFlag(f, "expect-project-incarnation", DELETE_INSTANCE_USAGE,);
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
				f["unconfirmed-creation"],
				"--unconfirmed-creation",
			) ?? false;
			if (unconfirmedCreation) {
				return {
					success: false,
					state: "UNCONFIRMED_CREATION",
					elapsedMs: 0,
					pollCount: 0,
					projectKey,
					...(futureId !== undefined ? { futureId, } : {}),
					deletePerformed: false,
					cleanupResolved: false,
					error:
						"Deletion cannot be automated because DSS returned no creation future ID and creation may still be running.",
					remediation: DELETE_INSTANCE_UNCONFIRMED_REMEDIATION,
				};
			}
			if (futureId === undefined) {
				try {
					await c.applications.deleteInstance(
						projectKey,
						expectedProjectIncarnation === undefined
							? undefined
							: { expectedProjectIncarnationHash: expectedProjectIncarnation, },
					);
					return { deleted: true, projectKey, };
				} catch (error) {
					if (error instanceof DataikuError && error.status === 404) {
						return { deleted: false, alreadyAbsent: true, projectKey, };
					}
					const classification = deleteFailureClassification(error,);
					return {
						success: false,
						state: "DELETE_FAILED",
						elapsedMs: 0,
						pollCount: 0,
						projectKey,
						deletePerformed: classification.deletePerformed,
						remediation: classification.remediation,
						...safeErrorSummary(error,),
					};
				}
			}

			// Reject an existing ordinary/template target before the supplied
			// future is touched. A missing target is allowed: its real creation
			// future may not have materialized the project yet.
			try {
				const manifest = await c.applications.getInstanceManifest(projectKey,);
				const details = typeof manifest.projectAppType === "string"
					? undefined
					: await c.projects.get(projectKey,);
				const projectAppType = typeof manifest.projectAppType === "string"
					? manifest.projectAppType
					: (details as Record<string, unknown> | undefined)?.projectAppType;
				if (projectAppType !== "APP_INSTANCE") {
					throw new ClientValidationError(
						"Only classic Dataiku App instance projects can be deleted through app delete-instance.",
						"validation_failed",
						"Use `dss app delete-instance` only for APP_INSTANCE projects; use `dss project delete` for templates or ordinary projects.",
						{ projectAppType: projectAppType ?? null, projectKey, },
					);
				}
			} catch (error) {
				if (error instanceof DataikuError && error.status === 404) {
					// The project may not exist yet while its verified creation
					// future is still live; let the future decide terminality.
				} else if (error instanceof DataikuError && error.status === 400) {
					throw new ClientValidationError(
						"Only classic Dataiku App instance projects can be deleted through app delete-instance.",
						"validation_failed",
						"Use `dss app delete-instance` only for APP_INSTANCE projects; use `dss project delete` for templates or ordinary projects.",
						{ projectAppType: null, projectKey, },
					);
				} else {
					throw error;
				}
			}

			// Waiting is read-only. The CLI never aborts a caller-supplied future:
			// a mismatched ID must not be able to cancel unrelated DSS work.
			let waited: FutureWaitResult;
			try {
				waited = await c.futures.wait(futureId, {
					pollIntervalMs: num(f["poll-interval"],),
					timeoutMs: num(f["timeout"],),
				},);
			} catch (error) {
				const absent = error instanceof DataikuError && error.status === 404;
				return {
					success: false,
					state: absent ? "FUTURE_UNVERIFIABLE" : "FUTURE_WAIT_FAILED",
					elapsedMs: 0,
					pollCount: 0,
					futureId,
					projectKey,
					deletePerformed: false,
					remediation: DELETE_INSTANCE_FUTURE_REMEDIATION,
					...(absent
						? {
							error:
								"The supplied creation future no longer exists, so its target identity cannot be verified.",
						}
						: safeErrorSummary(error,)),
				};
			}
			if (waited.timedOut === true) {
				return {
					...waited,
					success: false,
					state: "FUTURE_STILL_RUNNING",
					projectKey,
					deletePerformed: false,
					terminal: false,
					error:
						"The instance-creation future is still running, so the instance project was not deleted.",
					remediation: DELETE_INSTANCE_FUTURE_REMEDIATION,
				};
			}
			const futureTarget = futureTargetIdentity(waited.result,);
			if (futureTarget === null || futureTarget.projectKey !== projectKey) {
				return {
					...waited,
					success: false,
					state: futureTarget === null
						? "FUTURE_TARGET_UNVERIFIED"
						: "VERIFICATION_FAILED",
					projectKey,
					deletePerformed: false,
					error: futureTarget === null
						? "The terminal future did not report a target project, so it cannot authorize deletion."
						: "The terminal future names a different target project.",
					expected: { projectKey, },
					actual: futureTarget === null
						? { projectKey: null, }
						: { projectKey: futureTarget.projectKey, field: futureTarget.field, },
					remediation: DELETE_INSTANCE_FUTURE_REMEDIATION,
				};
			}
			if (expectedProjectIncarnation === undefined) {
				throw new UsageError(
					"--future-id requires --expect-project-incarnation so a completed future cannot authorize deletion of a replacement project that reused the same key.",
					"validation_failed",
				);
			}
			let deleted: boolean;
			try {
				await c.applications.deleteInstance(projectKey, {
					expectedProjectIncarnationHash: expectedProjectIncarnation,
				},);
				deleted = true;
			} catch (error) {
				if (error instanceof DataikuError && error.status === 404) {
					deleted = false;
				} else {
					const classification = deleteFailureClassification(error,);
					return {
						success: false,
						state: "DELETE_FAILED",
						elapsedMs: waited.elapsedMs,
						pollCount: waited.pollCount,
						futureId,
						projectKey,
						deletePerformed: classification.deletePerformed,
						remediation: classification.remediation,
						...safeErrorSummary(error,),
					};
				}
			}
			return {
				deleted,
				...(deleted ? {} : { alreadyAbsent: true, }),
				success: true,
				projectKey,
				futureId,
				futureState: waited.state,
				elapsedMs: waited.elapsedMs,
				pollCount: waited.pollCount,
			};
		},
		usage: DELETE_INSTANCE_USAGE,
		description:
			"Delete an app-instance project (destructive: removes the instance project). With --future-id, requires the recorded project-incarnation hash, waits without aborting, verifies that the terminal future reports this target, rechecks the incarnation, then deletes.",
		examples: [
			"dss app delete-instance --project-key MYINSTANCE",
			"dss app delete-instance --project-key MYINSTANCE --future-id dss-future-42 --expect-project-incarnation 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef --timeout 30000 --poll-interval 2000",
		],
	},
	"business-app-instance-permissions": {
		handler: (c, a,) => {
			requireArgs(
				a,
				3,
				"dss app business-app-instance-permissions <businessAppId> <instanceProjectKey> <userLogin>",
			);
			return c.applications.getBusinessAppInstanceUserPermissions(a[0], a[1], a[2],);
		},
		usage:
			"dss app business-app-instance-permissions <businessAppId> <instanceProjectKey> <userLogin>",
		description: "Get a user's effective permissions on a Business App instance.",
		examples: ["dss app business-app-instance-permissions my-bapp INSTANCEPROJ alice",],
	},
	"permissions-snapshot": {
		handler: async (c, _a, f,) => {
			const usage = "dss app permissions-snapshot --output PATH [--project-key KEY]";
			requireNoArgs(_a, usage,);
			const output = requiredStringFlag(f, "output", usage,);
			const projectKey = c.resolveProjectKey(f["project-key"] as string | undefined,);
			const projectIncarnation = await requireProjectIncarnationHash(c, projectKey,);
			const permissions = await c.projects.getPermissions(projectKey,);
			await assertProjectIncarnationHash(c, projectKey, projectIncarnation,);
			const snapshot = buildAppPermissionsSnapshot(
				projectKey,
				c.getBaseUrl(),
				projectIncarnation,
				permissions,
			);
			writeAppPermissionsSnapshot(output, snapshot,);
			return {
				output,
				projectKey,
				dssUrl: snapshot.dssUrl,
				hash: snapshot.hash,
				projectIncarnationHash: snapshot.projectIncarnationHash,
				permissionsHash: snapshot.permissionsHash,
				capturedAt: snapshot.capturedAt,
			};
		},
		usage: "dss app permissions-snapshot --output PATH [--project-key KEY]",
		description:
			"Snapshot a project's access-control permissions to a local owner-only JSON file with a canonical integrity hash; commit it only when repository policy permits.",
		examples: [
			"dss app permissions-snapshot --output app-permissions.json --project-key MYINSTANCE",
		],
	},
	"permissions-diff": {
		handler: async (c, _a, f,) => {
			const usage = "dss app permissions-diff --file PATH [--project-key KEY]";
			requireNoArgs(_a, usage,);
			const file = requiredStringFlag(f, "file", usage,);
			const snapshot = readAppPermissionsSnapshot(file,);
			const projectKey = c.resolveProjectKey(f["project-key"] as string | undefined,);
			assertAppPermissionsSnapshotBinding(snapshot, projectKey, c.getBaseUrl(),);
			const currentProjectIncarnationHash = await requireProjectIncarnationHash(c, projectKey,);
			assertAppPermissionsSnapshotBinding(
				snapshot,
				projectKey,
				c.getBaseUrl(),
				currentProjectIncarnationHash,
			);
			const current = await c.projects.getPermissions(projectKey,);
			await assertProjectIncarnationHash(c, projectKey, snapshot.projectIncarnationHash,);
			const currentHash = appPermissionsHash(current,);
			const diff = diffAppPermissions(snapshot.permissions, current,);
			return {
				projectKey,
				backupHash: snapshot.permissionsHash,
				currentHash,
				changed: diff.changed,
				differences: diff.differences,
			};
		},
		usage: "dss app permissions-diff --file PATH [--project-key KEY]",
		description:
			"Compare a permission snapshot file against the live project permissions, returning deterministic path-level differences.",
		examples: ["dss app permissions-diff --file app-permissions.json --project-key MYINSTANCE",],
	},
	"permissions-restore": {
		handler: async (c, _a, f,) => {
			const usage = "dss app permissions-restore --file PATH [--project-key KEY] [--dry-run]";
			requireNoArgs(_a, usage,);
			const file = requiredStringFlag(f, "file", usage,);
			const snapshot = readAppPermissionsSnapshot(file,);
			const projectKey = c.resolveProjectKey(f["project-key"] as string | undefined,);
			assertAppPermissionsSnapshotBinding(snapshot, projectKey, c.getBaseUrl(),);
			const currentProjectIncarnationHash = await requireProjectIncarnationHash(c, projectKey,);
			assertAppPermissionsSnapshotBinding(
				snapshot,
				projectKey,
				c.getBaseUrl(),
				currentProjectIncarnationHash,
			);
			const dryRun = parseBooleanOption(f["dry-run"], "--dry-run",) ?? false;
			const before = await c.projects.getPermissions(projectKey,);
			await assertProjectIncarnationHash(c, projectKey, snapshot.projectIncarnationHash,);
			const beforeHash = appPermissionsHash(before,);
			const desiredHash = snapshot.permissionsHash;
			if (beforeHash === desiredHash) {
				return {
					projectKey,
					beforeHash,
					desiredHash,
					verifiedHash: beforeHash,
					applied: false,
					reason: "unchanged",
				};
			}
			if (dryRun) {
				return {
					dryRun: true,
					projectKey,
					beforeHash,
					desiredHash,
					verifiedHash: null,
					applied: false,
					differences: diffAppPermissions(snapshot.permissions, before,).differences,
				};
			}
			await c.projects.setPermissions(projectKey, snapshot.permissions,);
			const after = await c.projects.getPermissions(projectKey,);
			await assertProjectIncarnationHash(c, projectKey, snapshot.projectIncarnationHash,);
			const verifiedHash = appPermissionsHash(after,);
			if (verifiedHash !== desiredHash) {
				throw appPermissionsVerificationError(projectKey, { beforeHash, desiredHash, verifiedHash, },);
			}
			return {
				projectKey,
				beforeHash,
				desiredHash,
				verifiedHash,
				applied: true,
			};
		},
		usage: "dss app permissions-restore --file PATH [--project-key KEY] [--dry-run]",
		description:
			"Restore a project's permissions from a snapshot file: hash-verifies the file, refuses cross-project restores, reads before writing, only PUTs when changed, and refetches to verify the result.",
		examples: [
			"dss app permissions-restore --file app-permissions.json --project-key MYINSTANCE --dry-run",
		],
	},
};
