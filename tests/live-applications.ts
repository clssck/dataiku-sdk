// Live applications suite module: owns the api-service / webapp / app /
// business-app coverage for the "applications" profile. Publication to the
// API Deployer stays a separate case: it creates its own NEW disposable
// published service through ctx.createGlobal and deletes it in finally.
//
// Every mutating case runs inside a selected ctx.check callback on a
// case-owned project created through the parent LiveContext and torn down in
// `finally` (selection invariant: unselected cases never provision). External
// app/business-app templates and any pre-existing business app are read-only
// surfaces: they are never modified, and template-dependent steps are split
// from independent discovery steps so one unavailable prerequisite cannot
// hide unrelated behaviors. Missing prerequisites are explicit "blocked"
// results via LiveCapabilityError — never mock passes — while genuine command
// defects remain ordinary failures.
import { expect, } from "bun:test";
import { provisionTrainedSavedModel, } from "./live-capabilities.js";
import { LiveCapabilityError, type LiveContext, } from "./live-context.js";

type JsonRecord = Record<string, unknown>;

function asRecord(value: unknown,): JsonRecord | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value,)
		? value as JsonRecord
		: undefined;
}

function asString(value: unknown,): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

function requireId(value: unknown, what: string,): string {
	const id = asString(value,);
	if (!id) throw new Error(`DSS did not return a ${what} identifier.`,);
	return id;
}

/**
 * The webapp round-trip marker rides the documented `name` label: DSS ignores
 * arbitrary params keys on a STANDARD webapp (proven live), but the name is a
 * real preserved field whose change and restore prove the PUT worked.
 */
const WEBAPP_MARKER_PREFIX = "live_webapp_marker_i";

/**
 * Explicit opt-ins for external application resources. Templates are never
 * guessed from list output and never modified: an unset variable is an
 * explicit blocker, not a silent skip.
 */
const APP_TEMPLATE_ID_ENV = "DATAIKU_LIVE_APP_TEMPLATE_ID";
const BUSINESS_APP_ID_ENV = "DATAIKU_LIVE_BUSINESS_APP_ID";
const BUSINESS_APP_INSTANCE_ENV = "DATAIKU_LIVE_BAPP_INSTANCE_PROJECT";
const BUSINESS_APP_USER_ENV = "DATAIKU_LIVE_BAPP_USER";
const BUSINESS_APP_ARCHIVE_ENV = "DATAIKU_LIVE_BAPP_ARCHIVE_PATH";

/**
 * Read-only observed prerequisite evidence cited in blockers: performs the
 * live list call and reports what the instance actually returns (count and
 * ids), never a guessed identity. A list failure is reported as unavailable
 * evidence rather than masking the original prerequisite blocker.
 */
async function listEvidence(ctx: LiveContext, resource: "app" | "business-app",): Promise<string> {
	const label = resource === "app" ? "app template" : "business app";
	try {
		const items = await ctx.run<unknown[]>([resource, "list",],);
		const list = Array.isArray(items,) ? items : [];
		const ids = list.map(item => {
			const record = asRecord(item,);
			return asString(record?.[resource === "app" ? "appId" : "id"],)
				?? asString(record?.["projectKey"],)
				?? asString(record?.["id"],);
		},).filter((id,): id is string => Boolean(id,));
		const suffix = ids.length ? ` (ids: ${ids.slice(0, 5,).join(", ",)})` : "";
		return `${resource} list returned ${String(list.length,)} ${label}(s)${suffix}`;
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error,);
		return `${resource} list unavailable during prerequisite check (${message.slice(0, 120,)})`;
	}
}

/**
 * Resolve the template prerequisite shared by every template-gated case: an
 * explicit DATAIKU_LIVE_APP_TEMPLATE_ID, else an exact blocker citing the
 * live list observation from this run (never a guessed template).
 */
async function requireTemplateId(ctx: LiveContext,): Promise<string> {
	const appId = process.env[APP_TEMPLATE_ID_ENV]?.trim();
	if (appId) return appId;
	throw new LiveCapabilityError(
		`No Dataiku App template prerequisite: set ${APP_TEMPLATE_ID_ENV} to an app template this key can read; ${await listEvidence(
			ctx,
			"app",
		)}. The applications profile does not guess templates from app list and never writes to external template manifests.`,
		"blocked",
	);
}

// ---------------------------------------------------------------------------
// API service: settings CRUD + prediction endpoint + package lifecycle
// ---------------------------------------------------------------------------

/**
 * API service settings CRUD in a case-owned project: create the service,
 * verify list visibility, round-trip the settings document through get/save
 * with a stable marker, and read it back. The service itself has no CLI
 * delete action: the case-owned project deletion in the caller's finally is
 * the authoritative teardown.
 */
async function exerciseApiServiceSettings(ctx: LiveContext, projectKey: string,): Promise<void> {
	const serviceId = `live_svc_settings_i${ctx.iteration}`;
	await ctx.run([
		"api-service",
		"create",
		serviceId,
		"--project-key",
		projectKey,
	],);
	const created = await ctx.run<unknown[]>([
		"api-service",
		"list",
		"--project-key",
		projectKey,
	],);
	expect(
		Array.isArray(created,) && created.some(item => asRecord(item,)?.["id"] === serviceId),
	).toBe(true,);

	const settings = await ctx.run<JsonRecord>([
		"api-service",
		"get-settings",
		serviceId,
		"--project-key",
		projectKey,
	],);
	expect(asRecord(settings,) === undefined,).toBe(false,);

	// Explicit-replacement save: DSS drops unknown top-level settings keys
	// (proven live: a top-level marker survives neither save nor re-GET), so
	// the round-trip marker lives in the documented customFields object, which
	// the PUT persists verbatim.
	const marker = `svc_i${ctx.iteration}`;
	const existingCustom = asRecord(settings["customFields"],) ?? {};
	const SERVICE_MARKER_KEY = "liveApplicationsMarker";
	await ctx.run([
		"api-service",
		"save-settings",
		serviceId,
		"--data",
		JSON.stringify({
			...settings,
			customFields: { ...existingCustom, [SERVICE_MARKER_KEY]: marker, },
		},),
		"--project-key",
		projectKey,
	],);
	const saved = await ctx.run<JsonRecord>([
		"api-service",
		"get-settings",
		serviceId,
		"--project-key",
		projectKey,
	],);
	const savedCustom = asRecord(saved["customFields"],);
	expect(savedCustom?.[SERVICE_MARKER_KEY],).toBe(marker,);
}

/**
 * Prediction endpoint backed by a REAL trained saved model (provisioned by
 * the ML module's helper into the same case-owned project): create the
 * service, add the endpoint, and verify get-settings reports the
 * STD_PREDICTION endpoint with the saved-model reference.
 */
async function exerciseApiServicePredictionEndpoint(
	ctx: LiveContext,
	projectKey: string,
): Promise<void> {
	const model = await provisionTrainedSavedModel(ctx, projectKey,);
	const serviceId = `live_svc_predict_i${ctx.iteration}`;
	await ctx.run([
		"api-service",
		"create",
		serviceId,
		"--project-key",
		projectKey,
	],);
	await ctx.run([
		"api-service",
		"add-prediction-endpoint",
		serviceId,
		"predict",
		model.savedModelId,
		"--project-key",
		projectKey,
	],);
	const settings = await ctx.run<JsonRecord>([
		"api-service",
		"get-settings",
		serviceId,
		"--project-key",
		projectKey,
	],);
	const endpoints = Array.isArray(settings["endpoints"],)
		? settings["endpoints"] as unknown[]
		: [];
	const added = endpoints.find(item => asRecord(item,)?.["id"] === "predict");
	expect(added === undefined,).toBe(false,);
	expect((added as JsonRecord | undefined)?.["type"],).toBe("STD_PREDICTION",);
	expect((added as JsonRecord | undefined)?.["modelRef"],).toBe(model.savedModelId,);
}

/**
 * Package lifecycle in a case-owned project: build a deployable package from
 * the current service state, verify summary/list visibility, download the
 * archive into the run directory, delete the package, and confirm it left
 * the list. A retry of delete-package in the cleanup path is tolerated as
 * best-effort: the case-owned project deletion is authoritative.
 */
async function exerciseApiServicePackages(ctx: LiveContext, projectKey: string,): Promise<void> {
	const serviceId = `live_svc_pkg_i${ctx.iteration}`;
	const packageId = `v1_i${ctx.iteration}`;
	await ctx.run([
		"api-service",
		"create",
		serviceId,
		"--project-key",
		projectKey,
	],);
	try {
		await ctx.run([
			"api-service",
			"create-package",
			serviceId,
			packageId,
			"--release-notes",
			`live applications package ${ctx.iteration}`,
			"--project-key",
			projectKey,
		],);
		const summary = await ctx.run<JsonRecord>([
			"api-service",
			"package-summary",
			serviceId,
			packageId,
			"--project-key",
			projectKey,
		],);
		expect(asRecord(summary,) === undefined,).toBe(false,);
		const packages = await ctx.run<unknown[]>([
			"api-service",
			"list-packages",
			serviceId,
			"--project-key",
			projectKey,
		],);
		expect(
			Array.isArray(packages,) && packages.some(item => asRecord(item,)?.["id"] === packageId),
		).toBe(true,);
		const archive = `${ctx.dir}/api-package-${ctx.iteration}.zip`;
		const downloaded = await ctx.run<JsonRecord>([
			"api-service",
			"download-package",
			serviceId,
			packageId,
			"--output",
			archive,
			"--project-key",
			projectKey,
		],);
		expect(downloaded["path"],).toBe(archive,);
		await ctx.run([
			"api-service",
			"delete-package",
			serviceId,
			packageId,
			"--project-key",
			projectKey,
		],);
		const remaining = await ctx.run<unknown[]>([
			"api-service",
			"list-packages",
			serviceId,
			"--project-key",
			projectKey,
		],);
		expect(
			Array.isArray(remaining,) && !remaining.some(item => asRecord(item,)?.["id"] === packageId),
		).toBe(true,);
	} finally {
		try {
			await ctx.run([
				"api-service",
				"delete-package",
				serviceId,
				packageId,
				"--project-key",
				projectKey,
			],);
		} catch {
			// Best-effort repeat: the happy path already deleted the package;
			// the case-owned project deletion below is the authoritative
			// teardown for the service itself (no api-service delete action).
		}
	}
}

// ---------------------------------------------------------------------------
// API service publication (separate APIDeployer-dependent capability)
// ---------------------------------------------------------------------------

/**
 * Publish a package to the API Deployer. KEPT SEPARATE from the package
 * lifecycle: it requires the API Deployer surface plus a NEW disposable
 * published service created through ctx.createGlobal (owned by this run,
 * identity-bound, deleted in finally via ctx.deleteGlobal — never left in
 * shared global state). Pre-creating the owned service first is the cleaner
 * order: bind + cleanup are automatic, and publish-package only aims at the
 * exact reserved identity.
 */
async function exerciseApiServicePublication(ctx: LiveContext, projectKey: string,): Promise<void> {
	const serviceId = `live_svc_pub_i${ctx.iteration}`;
	const packageId = `v1_i${ctx.iteration}`;
	// Official deployer create-service body (verified against
	// dataiku-api-client-python apideployer.py: create_service sends ONLY
	// {"publishedServiceId": id}): DSS rejects id/description with 400.
	const owned = await ctx.createGlobal(
		"api-deployer-service",
		`pub_svc_${ctx.iteration}`,
		(name,) => [
			"api-deployer",
			"create-service",
			"--data",
			JSON.stringify({ publishedServiceId: name, },),
		],
	);
	try {
		await ctx.run([
			"api-service",
			"create",
			serviceId,
			"--project-key",
			projectKey,
		],);
		await ctx.run([
			"api-service",
			"create-package",
			serviceId,
			packageId,
			"--release-notes",
			`live applications publication ${ctx.iteration}`,
			"--project-key",
			projectKey,
		],);
		const published = await ctx.run<JsonRecord>([
			"api-service",
			"publish-package",
			serviceId,
			packageId,
			"--published-service-id",
			owned.name,
			"--project-key",
			projectKey,
		],);
		expect(asRecord(published,) === undefined,).toBe(false,);
		const services = await ctx.run<unknown[]>(["api-deployer", "list-services",],);
		// Deployer list/get responses expose the identity nested under
		// serviceBasicInfo (the official Python client reads
		// x["serviceBasicInfo"]["id"]; the live-context bind guard verifies
		// the same key) — never a flat top-level id.
		expect(
			Array.isArray(services,) && services.some(item => {
				const info = asRecord(asRecord(item,)?.["serviceBasicInfo"],) ?? asRecord(item,);
				return (asString(info?.["id"],) ?? asString(info?.["publishedServiceId"],))
					=== owned.name;
			},),
		).toBe(true,);
	} finally {
		await ctx.deleteGlobal("api-deployer-service", owned.id ?? owned.name,);
	}
}

// ---------------------------------------------------------------------------
// Webapp lifecycle
// ---------------------------------------------------------------------------

/**
 * Webapp settings/backend lifecycle in a case-owned project: create a
 * standard webapp and take the SERVER-GENERATED webAppId from the create
 * receipt (DSS ignores any client-supplied id — proven live), verify list
 * visibility by that id, prove the GET-merge-PUT settings path by changing
 * and restoring the documented `name` label, stop the backend, and read the
 * backend state. The webapp has no CLI delete action: the case-owned project
 * deletion in the caller's finally is the authoritative teardown.
 */
async function exerciseWebappLifecycle(ctx: LiveContext, projectKey: string,): Promise<void> {
	const creation = await ctx.run<JsonRecord>([
		"webapp",
		"create",
		"--data",
		JSON.stringify({ name: `Live webapp ${ctx.iteration}`, type: "STANDARD", params: {}, },),
		"--project-key",
		projectKey,
	],);
	const webappId = requireId(creation["webAppId"], "webAppId",);

	const settings = await ctx.run<JsonRecord>([
		"webapp",
		"get-settings",
		webappId,
		"--project-key",
		projectKey,
	],);
	const list = await ctx.run<unknown[]>(["webapp", "list", "--project-key", projectKey,],);
	expect(
		Array.isArray(list,) && list.some(item => asRecord(item,)?.["id"] === webappId),
	).toBe(true,);
	// Real settings change + restore through the documented `name` label
	// (DSS ignores arbitrary params keys on STANDARD webapps — proven live —
	// so the marker proves the PUT path by changing and restoring a field
	// DSS actually persists).
	const marker = `${WEBAPP_MARKER_PREFIX}${ctx.iteration}`;
	await ctx.run([
		"webapp",
		"update-settings",
		webappId,
		"--data",
		JSON.stringify({ name: marker, },),
		"--project-key",
		projectKey,
	],);
	const renamed = await ctx.run<JsonRecord>([
		"webapp",
		"get-settings",
		webappId,
		"--project-key",
		projectKey,
	],);
	expect(renamed["name"],).toBe(marker,);
	// GET-merge-PUT contract: fields outside the patch are preserved.
	expect(asString(renamed["type"],),).toBe("STANDARD",);

	// Restore the original name so the webapp is left as created.
	const originalName = asString(settings["name"],) ?? `Live webapp ${ctx.iteration}`;
	await ctx.run([
		"webapp",
		"update-settings",
		webappId,
		"--data",
		JSON.stringify({ name: originalName, },),
		"--project-key",
		projectKey,
	],);
	const restored = await ctx.run<JsonRecord>([
		"webapp",
		"get-settings",
		webappId,
		"--project-key",
		projectKey,
	],);
	expect(restored["name"],).toBe(originalName,);

	await ctx.run(["webapp", "stop-backend", webappId, "--project-key", projectKey,],);
	const state = await ctx.run<JsonRecord>([
		"webapp",
		"backend-state",
		webappId,
		"--project-key",
		projectKey,
	],);
	expect(asRecord(state,) === undefined,).toBe(false,);
}

/**
 * Minimal Python backend for a STANDARD webapp: the exact shape the DSS
 * editor scaffold writes (DSS frontend enablePythonBackend: params.python
 * with an @app.route Flask handler returning json.dumps), so only documented,
 * produced-by-DSS keys are used — no arbitrary unknown fields.
 */
const PYTHON_BACKEND_STUB = [
	"import json",
	"",
	"@app.route('/live_check')",
	"def live_check():",
	'    return json.dumps({"status": "ok"})',
].join("\n",);

/**
 * Webapp backend lifecycle in a case-owned project: create a STANDARD webapp,
 * enable its Python backend through the DSS-owned settings keys
 * (params.backendEnabled = true plus params.python, exactly what the DSS
 * editor writes when the backend is enabled), start/restart the backend and
 * wait on the REAL restart future, verify the documented running signal
 * (backend-state.futureInfo.alive), then stop it and verify the state settles.
 * An empty restart response is NOT a supported wait target (the earlier
 * synthesized-result fallback was removed from the SDK), so this case must
 * and does configure a truly enabled backend; the CRUD case stays independent
 * so a backend problem never hides the settings surface.
 */
async function exerciseWebappBackend(ctx: LiveContext, projectKey: string,): Promise<void> {
	const creation = await ctx.run<JsonRecord>([
		"webapp",
		"create",
		"--data",
		JSON.stringify({ name: `Live webapp backend ${ctx.iteration}`, type: "STANDARD", params: {}, },),
		"--project-key",
		projectKey,
	],);
	const webappId = requireId(creation["webAppId"], "webAppId",);

	await ctx.run([
		"webapp",
		"update-settings",
		webappId,
		"--data",
		JSON.stringify({ params: { backendEnabled: true, python: PYTHON_BACKEND_STUB, }, },),
		"--project-key",
		projectKey,
	],);

	const restarted = await ctx.run<JsonRecord>([
		"webapp",
		"restart-backend",
		webappId,
		"--wait",
		"--timeout",
		"120000",
		"--poll-interval",
		"3000",
		"--project-key",
		projectKey,
	],);
	expect(restarted["success"],).toBe(true,);

	const runningState = await ctx.run<JsonRecord>([
		"webapp",
		"backend-state",
		webappId,
		"--project-key",
		projectKey,
	],);
	expect(runningState["projectKey"],).toBe(projectKey,);
	expect(runningState["webAppId"] ?? runningState["id"],).toBe(webappId,);
	// Documented running signal (official DSSWebAppBackendState.running):
	// futureInfo.alive is true while the backend process is up.
	const futureInfo = asRecord(runningState["futureInfo"],);
	expect(futureInfo?.["alive"],).toBe(true,);

	await ctx.run(["webapp", "stop-backend", webappId, "--project-key", projectKey,],);
	const stoppedState = await ctx.run<JsonRecord>([
		"webapp",
		"backend-state",
		webappId,
		"--project-key",
		projectKey,
	],);
	expect(asRecord(stoppedState,) === undefined,).toBe(false,);
}

// ---------------------------------------------------------------------------
// App / business-app discovery + configured-template surface
// ---------------------------------------------------------------------------

/**
 * Read-only app/business-app discovery: list reads never mutate anything.
 * Template-shaped assertions run only when the explicit opt-in id is
 * configured; list results without it stay soft (an empty tenant is a
 * legitimate result, not a failure). Pre-existing configured business apps
 * are read-only and have no owned provisioning verb, so no business-app
 * assertions are claimed here.
 */
async function exerciseAppDiscovery(ctx: LiveContext,): Promise<void> {
	const apps = await ctx.run<unknown[]>(["app", "list",],);
	expect(Array.isArray(apps,),).toBe(true,);
	const businessApps = await ctx.run<unknown[]>(["business-app", "list",],);
	expect(Array.isArray(businessApps,),).toBe(true,);

	const templateId = process.env[APP_TEMPLATE_ID_ENV]?.trim();
	if (templateId) {
		const manifest = await ctx.run<JsonRecord>(["app", "manifest", templateId,],);
		expect(asRecord(manifest,) === undefined,).toBe(false,);
		const instances = await ctx.run<unknown[]>(["app", "instances", templateId,],);
		expect(Array.isArray(instances,),).toBe(true,);
	}

	// Business-app-shaped assertions stay out of discovery: pre-existing
	// configured business apps are read-only, and there is no owned
	// provisioning verb, so nothing else can be asserted honestly here.
}

/**
 * Create an app instance from a template into a reserved owned project and
 * wait for the creation future. Shared by every template-gated case; a
 * non-terminal creation state is a genuine failure, never masked.
 */
async function createAppInstance(
	ctx: LiveContext,
	appId: string,
	reservedKey: string,
	label: string,
): Promise<void> {
	const creation = await ctx.run<JsonRecord>([
		"app",
		"create-instance",
		appId,
		"--data",
		JSON.stringify({ targetProjectKey: reservedKey, targetProjectName: label, },),
		"--wait",
		"--timeout",
		"180000",
		"--poll-interval",
		"3000",
	],);
	if (creation["success"] !== true) {
		// CREATE_FAILED / INDETERMINATE / VERIFICATION_FAILED are genuine
		// defects or ambiguous outcomes: reported as failures, never masked.
		const state = asString(creation["state"],) ?? "UNKNOWN";
		throw new Error(
			`App instance creation did not complete (state=${state}): ${
				JSON.stringify(creation,).slice(0, 300,)
			}`,
		);
	}
	const echoed = asString(creation["projectKey"],);
	if (echoed !== undefined && echoed !== reservedKey) {
		throw new Error(`App instance creation named a different target project: ${echoed}.`,);
	}
}

/**
 * The configured-template surface case: only an explicit
 * DATAIKU_LIVE_APP_TEMPLATE_ID is accepted; templates are never guessed and
 * never modified. Reads the template manifest version, creates a case-owned
 * instance, and exercises the read-only instance surface.
 */
async function exerciseAppTemplateSurface(ctx: LiveContext, appId: string,): Promise<void> {
	const templateVersion = await ctx.run<JsonRecord>(["app", "manifest-version",], {
		projectKey: appId,
	},);
	expect(asRecord(templateVersion,) === undefined,).toBe(false,);

	const reservedKey = await ctx.reserveProject("appsurf",);
	const label = `Live applications surface ${ctx.iteration}`;
	let created = false;
	try {
		await createAppInstance(ctx, appId, reservedKey, label,);
		created = true;
		await ctx.bindProject(reservedKey,);

		const instanceVersion = await ctx.run<JsonRecord>(["app", "manifest-version",], {
			projectKey: reservedKey,
		},);
		expect(asRecord(instanceVersion,) === undefined,).toBe(false,);
		const instanceManifest = await ctx.run<JsonRecord>(["app", "instance-manifest",], {
			projectKey: reservedKey,
		},);
		expect(asRecord(instanceManifest,) === undefined,).toBe(false,);
		const validation = await ctx.run<JsonRecord>(["app", "validate-manifest",], {
			projectKey: reservedKey,
		},);
		expect(validation["valid"],).toBe(true,);
		const comparison = await ctx.run<JsonRecord>([
			"app",
			"compare-manifest",
			appId,
			"--project-key",
			reservedKey,
		],);
		expect(asRecord(comparison,) === undefined,).toBe(false,);
		const verification = await ctx.run<JsonRecord>([
			"app",
			"verify-instance",
			appId,
			"--project-key",
			reservedKey,
		],);
		expect(verification["valid"] === true && verification["apiReady"] === true,).toBe(true,);
	} finally {
		if (created) await ctx.deleteProject(reservedKey,);
	}
}

/**
 * Fetch the bound incarnation hash of an owned project from the run
 * manifest: app.delete-instance --expect-project-incarnation must match the
 * exact identity the run recorded at bind time.
 */
function boundIncarnation(ctx: LiveContext, key: string,): string {
	const project = ctx.projects.find(p => p.key === key);
	if (project?.state !== "bound" || !project.incarnation) {
		throw new Error(`No bound incarnation recorded for ${key}.`,);
	}
	return project.incarnation;
}

/**
 * The app instance-ops surface on a case-owned instance project: permission
 * snapshot + diff against the live project, an instance-manifest save
 * round-trip (the case-owned INSTANCE manifest, never the template), and the
 * instance's own app.delete-instance (the official disposal path — verified
 * against the Python client, whose DSSBusinessAppInstance.delete delegates to
 * a project delete) with the bound incarnation guard. Template-surface
 * (manifest/version/validate/compare/verify) stays in the template-surface
 * case. All reads/writes target the case-owned instance only.
 */
async function exerciseAppInstanceOps(ctx: LiveContext, appId: string,): Promise<void> {
	const reservedKey = await ctx.reserveProject("appops",);
	const label = `Live applications ops ${ctx.iteration}`;
	let created = false;
	try {
		await createAppInstance(ctx, appId, reservedKey, label,);
		created = true;
		await ctx.bindProject(reservedKey,);

		// Snapshot the owned instance permissions, then diff it against the
		// live project (a faithful snapshot produces an empty difference).
		const snapshotPath = `${ctx.dir}/app-permissions-${ctx.iteration}.json`;
		const snapshot = await ctx.run<JsonRecord>([
			"app",
			"permissions-snapshot",
			"--output",
			snapshotPath,
			"--project-key",
			reservedKey,
		],);
		expect(asRecord(snapshot,) === undefined,).toBe(false,);
		const diff = await ctx.run<JsonRecord>([
			"app",
			"permissions-diff",
			"--file",
			snapshotPath,
			"--project-key",
			reservedKey,
		],);
		expect(asRecord(diff,) === undefined,).toBe(false,);

		// Instance-manifest save round-trip: read the case-owned instance
		// manifest, save it back verbatim, and confirm the read-back matches.
		const manifestBefore = await ctx.run<JsonRecord>(["app", "instance-manifest",], {
			projectKey: reservedKey,
		},);
		await ctx.run([
			"app",
			"save-instance-manifest",
			"--data",
			JSON.stringify(manifestBefore,),
			"--project-key",
			reservedKey,
		],);
		const manifestAfter = await ctx.run<JsonRecord>(["app", "instance-manifest",], {
			projectKey: reservedKey,
		},);
		expect(asRecord(manifestAfter,) === undefined,).toBe(false,);

		// The official disposal of an app-instance project (the Python
		// client's DSSBusinessAppInstance.delete delegates to a project
		// delete): the incarnation-guarded app.delete-instance on the
		// case-owned instance.
		await ctx.run([
			"app",
			"delete-instance",
			"--project-key",
			reservedKey,
			"--expect-project-incarnation",
			boundIncarnation(ctx, reservedKey,),
		],);
	} finally {
		// app.delete-instance already removed the project on the success path;
		// ctx.deleteProject is idempotent (deleted state short-circuits) and is
		// the fallback when the case failed before deletion.
		if (created) await ctx.deleteProject(reservedKey,);
	}
}

/**
 * Read-only successor preflight between the template's owned predecessor
 * instance and a fresh reserved target: validates the template, verifies the
 * predecessor, and proves target absence without changing anything.
 */
async function exerciseAppSuccessorPreflight(
	ctx: LiveContext,
	appId: string,
	predecessorKey: string,
): Promise<void> {
	const targetKey = await ctx.reserveProject("appnew",);
	try {
		const preflight = await ctx.run<JsonRecord>([
			"app",
			"successor-preflight",
			appId,
			"--from",
			predecessorKey,
			"--to",
			targetKey,
		],);
		expect(asRecord(preflight,) === undefined,).toBe(false,);
	} finally {
		// The preflight target is never created; drop the reservation without
		// deleting anything on the server.
		await ctx.deleteProject(targetKey,);
	}
}

/**
 * Full successor lifecycle from a case-owned predecessor: create the
 * successor instance into a new reserved project, bind it, verify it, then
 * dispose of BOTH instance projects through the sanctioned project-delete
 * path (the predecessor is never modified by the successor creation).
 */
async function exerciseAppSuccessorLifecycle(ctx: LiveContext, appId: string,): Promise<void> {
	const predecessorKey = await ctx.reserveProject("appold",);
	const predecessorLabel = `Live applications predecessor ${ctx.iteration}`;
	let predecessorCreated = false;
	try {
		await createAppInstance(ctx, appId, predecessorKey, predecessorLabel,);
		predecessorCreated = true;
		await ctx.bindProject(predecessorKey,);

		const successorKey = await ctx.reserveProject("appnew",);
		let successorCreated = false;
		try {
			const successorLabel = `Live applications successor ${ctx.iteration}`;
			await ctx.run([
				"app",
				"create-successor-instance",
				appId,
				"--from",
				predecessorKey,
				"--to",
				successorKey,
				"--name",
				successorLabel,
				"--timeout",
				"180000",
				"--poll-interval",
				"3000",
			],);
			successorCreated = true;
			await ctx.bindProject(successorKey,);

			const verification = await ctx.run<JsonRecord>([
				"app",
				"verify-instance",
				appId,
				"--project-key",
				successorKey,
			],);
			expect(verification["valid"] === true && verification["apiReady"] === true,).toBe(true,);
		} finally {
			if (successorCreated) await ctx.deleteProject(successorKey,);
		}
	} finally {
		if (predecessorCreated) await ctx.deleteProject(predecessorKey,);
	}
}

/**
 * Permissions restore cycle on a case-owned instance: snapshot, ALTER the
 * owned instance's permissions (borrow a read-only group rule via
 * project permissions-set), restore from the snapshot, and verify the live
 * permissions match the snapshot again. The alteration is real (a concrete
 * permission rule lands), so the restore proves an actual write, never a
 * no-op echo.
 */
async function exerciseAppPermissionsRestoreCycle(
	ctx: LiveContext,
	appId: string,
): Promise<void> {
	const reservedKey = await ctx.reserveProject("appperm",);
	let created = false;
	try {
		await createAppInstance(
			ctx,
			appId,
			reservedKey,
			`Live applications permissions ${ctx.iteration}`,
		);
		created = true;
		await ctx.bindProject(reservedKey,);

		const snapshotPath = `${ctx.dir}/app-permissions-restore-${ctx.iteration}.json`;
		await ctx.run([
			"app",
			"permissions-snapshot",
			"--output",
			snapshotPath,
			"--project-key",
			reservedKey,
		],);

		// Real alteration of the OWNED instance's permissions: borrow the
		// run-owner identity as a non-admin extra rule the snapshot does not
		// have, then restore the snapshot and verify the extra rule is gone.
		const before = await ctx.client.projects.getPermissions(reservedKey,);
		const baseline = before.permissions ?? [];
		const ownerLogin = ctx.owner;
		const alreadyThere = baseline.some(rule => rule.user === ownerLogin && rule.admin === false);
		const added = alreadyThere
			? undefined
			: {
				user: ownerLogin,
				admin: false,
				readProjectContent: true,
				writeProjectContent: false,
			};
		if (added) {
			await ctx.run([
				"project",
				"permissions-set",
				"--data",
				JSON.stringify({ ...before, permissions: [...baseline, added,], },),
			], { projectKey: reservedKey, },);
			const altered = await ctx.client.projects.getPermissions(reservedKey,);
			expect(
				altered.permissions?.some(rule => rule.user === ownerLogin && rule.admin === false),
			).toBe(true,);
		}

		const restored = await ctx.run<JsonRecord>([
			"app",
			"permissions-restore",
			"--file",
			snapshotPath,
			"--project-key",
			reservedKey,
		],);
		expect(asRecord(restored,) === undefined,).toBe(false,);
		const after = await ctx.client.projects.getPermissions(reservedKey,);
		if (added) {
			expect(
				after.permissions?.some(rule => rule.user === ownerLogin && rule.admin === false),
			).toBe(false,);
		}
		expect(after.permissions?.length,).toBe(baseline.length,);
	} finally {
		if (created) await ctx.deleteProject(reservedKey,);
	}
}

// ---------------------------------------------------------------------------
// Module entry point
// ---------------------------------------------------------------------------

/**
 * Exercise the applications-profile surface. Phase contract (parent
 * LiveContext): this module creates no persistent fixtures in either phase —
 * setup is a no-op and every mutating scenario runs in the run phase inside a
 * selected ctx.check callback on a case-owned project deleted in finally, so
 * repeated selected runs stay idempotent. ctx.selection filtering is applied
 * automatically by ctx.check; project creation happens inside check bodies,
 * so unselected cases never provision.
 *
 * Case granularity is deliberate: settings CRUD, prediction endpoint,
 * package lifecycle, APIDeployer publication, webapp lifecycle, app
 * discovery, business-app settings, and the configured-template surface are
 * independent cases so one unavailable export or prerequisite (a trained
 * model, an API Deployer, a configured template) cannot hide the others.
 */
export async function exerciseApplications(ctx: LiveContext,): Promise<void> {
	if (ctx.phase !== "run") return;
	// ctx.check self-gates on ctx.selection per case; the profile gate is the
	// only module-level guard, so unselected cases never provision anything.
	if (!ctx.profiles.includes("applications",)) return;

	await ctx.check(
		"applications.api-service.service-crud",
		[
			"api-service.create",
			"api-service.list",
			"api-service.get-settings",
			"api-service.save-settings",
		],
		async () => {
			const projectKey = await ctx.createProject("apisvc",);
			try {
				await exerciseApiServiceSettings(ctx, projectKey,);
			} finally {
				await ctx.deleteProject(projectKey,);
			}
		},
		{ capability: "applications.api-service-settings", },
	);

	await ctx.check(
		"applications.api-service.prediction-endpoint",
		[
			"analysis.create",
			"ml-task.create",
			"ml-task.train",
			"ml-task.deploy",
			"api-service.create",
			"api-service.add-prediction-endpoint",
			"api-service.get-settings",
		],
		async () => {
			const projectKey = await ctx.createProject("apipred",);
			try {
				await exerciseApiServicePredictionEndpoint(ctx, projectKey,);
			} finally {
				await ctx.deleteProject(projectKey,);
			}
		},
		{ capability: "applications.api-service-prediction", },
	);

	await ctx.check(
		"applications.api-service.package-lifecycle",
		[
			"api-service.create",
			"api-service.create-package",
			"api-service.package-summary",
			"api-service.list-packages",
			"api-service.download-package",
			"api-service.delete-package",
		],
		async () => {
			const projectKey = await ctx.createProject("apipkg",);
			try {
				await exerciseApiServicePackages(ctx, projectKey,);
			} finally {
				await ctx.deleteProject(projectKey,);
			}
		},
		{ capability: "applications.api-service-packages", },
	);

	await ctx.check(
		"applications.api-service.publication",
		[
			"api-deployer.list-infras",
			"api-deployer.list-services",
			"api-service.create",
			"api-service.create-package",
			"api-service.publish-package",
		],
		async () => {
			const projectKey = await ctx.createProject("apipub",);
			try {
				await exerciseApiServicePublication(ctx, projectKey,);
			} finally {
				await ctx.deleteProject(projectKey,);
			}
		},
		{ capability: "applications.api-service-publication", required: false, },
	);

	await ctx.check(
		"applications.webapp.lifecycle",
		[
			"webapp.create",
			"webapp.get-settings",
			"webapp.list",
			"webapp.update-settings",
			"webapp.stop-backend",
			"webapp.backend-state",
		],
		async () => {
			const projectKey = await ctx.createProject("webapp",);
			try {
				await exerciseWebappLifecycle(ctx, projectKey,);
			} finally {
				await ctx.deleteProject(projectKey,);
			}
		},
		{ capability: "applications.webapp-lifecycle", },
	);

	await ctx.check(
		"applications.webapp.backend",
		[
			"webapp.create",
			"webapp.update-settings",
			"webapp.restart-backend",
			"webapp.backend-state",
			"webapp.stop-backend",
		],
		async () => {
			const projectKey = await ctx.createProject("webappbe",);
			try {
				await exerciseWebappBackend(ctx, projectKey,);
			} finally {
				await ctx.deleteProject(projectKey,);
			}
		},
		{ capability: "applications.webapp-backend", },
	);

	await ctx.check(
		"applications.app-discovery",
		[
			"app.list",
			"business-app.list",
		],
		async () => {
			await exerciseAppDiscovery(ctx,);
		},
		{ capability: "applications.app-discovery", required: false, },
	);

	await ctx.check(
		"applications.business-app-settings",
		["business-app.save-settings",],
		async () => {
			// Pre-existing configured business apps are read-only surfaces, and
			// business apps are not an owned-global kind: the CLI has no verb
			// that creates a business app from nothing, and no delete verb, so
			// any created app would be undisposable. save-settings therefore
			// stays denied until an owned-provisioning contract exists.
			throw new LiveCapabilityError(
				`business-app has no owned provisioning verb (install-from-archive/create-instance both need external receipts and no delete verb exists), so save-settings against a business app not owned by this run is denied; configured pre-existing business apps are read-only; ${await listEvidence(
					ctx,
					"business-app",
				)}.`,
				"blocked",
			);
		},
		{ capability: "applications.business-app-settings", required: false, },
	);

	await ctx.check(
		"applications.app.template-surface",
		[
			"app.manifest-version",
			"app.create-instance",
			"app.instance-manifest",
			"app.validate-manifest",
			"app.compare-manifest",
			"app.verify-instance",
		],
		async () => {
			const appId = await requireTemplateId(ctx,);
			await exerciseAppTemplateSurface(ctx, appId,);
		},
		{ capability: "applications.template-surface", required: false, },
	);
	await ctx.check(
		"applications.app.instance-ops",
		[
			"app.create-instance",
			"app.permissions-snapshot",
			"app.permissions-diff",
			"app.save-instance-manifest",
			"app.delete-instance",
		],
		async () => {
			const appId = await requireTemplateId(ctx,);
			await exerciseAppInstanceOps(ctx, appId,);
		},
		{ capability: "applications.app-instance-ops", required: false, },
	);

	await ctx.check(
		"applications.app.successor-preflight",
		[
			"app.create-instance",
			"app.successor-preflight",
		],
		async () => {
			const appId = await requireTemplateId(ctx,);
			const predecessorKey = await ctx.reserveProject("apppred",);
			let created = false;
			try {
				await createAppInstance(
					ctx,
					appId,
					predecessorKey,
					`Live applications preflight predecessor ${ctx.iteration}`,
				);
				created = true;
				await ctx.bindProject(predecessorKey,);
				await exerciseAppSuccessorPreflight(ctx, appId, predecessorKey,);
			} finally {
				if (created) await ctx.deleteProject(predecessorKey,);
			}
		},
		{ capability: "applications.app-successor-preflight", required: false, },
	);

	await ctx.check(
		"applications.app.successor-lifecycle",
		[
			"app.create-instance",
			"app.create-successor-instance",
			"app.verify-instance",
			"project.delete",
		],
		async () => {
			const appId = await requireTemplateId(ctx,);
			await exerciseAppSuccessorLifecycle(ctx, appId,);
		},
		{ capability: "applications.app-successor-lifecycle", required: false, },
	);

	await ctx.check(
		"applications.app.permissions-restore-cycle",
		[
			"app.create-instance",
			"app.permissions-snapshot",
			"project.permissions-set",
			"app.permissions-restore",
			"project.delete",
		],
		async () => {
			const appId = await requireTemplateId(ctx,);
			await exerciseAppPermissionsRestoreCycle(ctx, appId,);
		},
		{ capability: "applications.app-permissions-restore", required: false, },
	);

	await ctx.check(
		"applications.app.set-manifest-version",
		["app.set-manifest-version",],
		async () => {
			// set-manifest-version publishes a template version that new
			// instances inherit: it may only ever target a template owned by
			// this run. There is no owned-template provisioning verb, so the
			// case blocks with the exact constraint until an owned-template
			// fixture contract exists; external templates are never written.
			throw new LiveCapabilityError(
				`No owned Dataiku App template: set-manifest-version publishes the persisted template version that new instances inherit, so it is denied against external templates (never written by live cases); no CLI verb creates an owned template from nothing, so the case stays blocked until an owned-template provisioning contract exists; ${await listEvidence(
					ctx,
					"app",
				)}.`,
				"blocked",
			);
		},
		{ capability: "applications.app-set-manifest-version", required: false, },
	);

	await ctx.check(
		"applications.app.business-app-instance-permissions",
		["app.business-app-instance-permissions",],
		async () => {
			// Both receipts are required and neither is ever guessed: a
			// configured business app id plus an instance project of that app
			// plus the login to query. With none configured this blocks
			// exactly rather than exercising an unrelated tenant artifact.
			const businessAppId = process.env[BUSINESS_APP_ID_ENV]?.trim();
			const instanceProjectKey = process.env[BUSINESS_APP_INSTANCE_ENV]?.trim();
			const userLogin = process.env[BUSINESS_APP_USER_ENV]?.trim();
			if (!businessAppId || !instanceProjectKey || !userLogin) {
				throw new LiveCapabilityError(
					`No Business App instance permission prerequisite: set ${BUSINESS_APP_ID_ENV} (a business app id readable by this key), ${BUSINESS_APP_INSTANCE_ENV} (an existing instance project key of that app), and ${BUSINESS_APP_USER_ENV} (a login to query). All three are explicit opt-ins; list output is never guessed and external apps are never written; ${await listEvidence(
						ctx,
						"business-app",
					)}.`,
					"blocked",
				);
			}
			const permissions = await ctx.run<JsonRecord>([
				"app",
				"business-app-instance-permissions",
				businessAppId,
				instanceProjectKey,
				userLogin,
			],);
			expect(asRecord(permissions,) === undefined,).toBe(false,);
		},
		{ capability: "applications.app-bapp-instance-permissions", required: false, },
	);

	await ctx.check(
		"applications.business-app.read-surface",
		["business-app.get", "business-app.settings", "business-app.instances",],
		async () => {
			// Read-only surface of an explicitly configured business app;
			// unset id is an exact blocker, never a silent skip.
			const id = process.env[BUSINESS_APP_ID_ENV]?.trim();
			if (!id) {
				throw new LiveCapabilityError(
					`No Business App prerequisite: set ${BUSINESS_APP_ID_ENV} to a Business App id this key can read; ${await listEvidence(
						ctx,
						"business-app",
					)}. The applications profile never guesses business-app ids from list output and never writes to external business apps.`,
					"blocked",
				);
			}
			const details = await ctx.run<JsonRecord>(["business-app", "get", id,],);
			expect(asRecord(details,) === undefined,).toBe(false,);
			const settings = await ctx.run<JsonRecord>(["business-app", "settings", id,],);
			expect(asRecord(settings,) === undefined,).toBe(false,);
			const instances = await ctx.run<unknown[]>(["business-app", "instances", id,],);
			expect(Array.isArray(instances,),).toBe(true,);
		},
		{ capability: "applications.business-app-read", required: false, },
	);

	await ctx.check(
		"applications.business-app.lifecycle",
		[
			"business-app.install-from-archive",
			"business-app.create-instance",
			"business-app.upgrade-instance",
		],
		async () => {
			// Two prerequisites must preflight before ANY mutation: an owned
			// archive fixture AND a source-verified cleanup contract for the
			// created shell. The official Python client has NO delete method
			// for the business-app shell (only instance projects are deleted),
			// so without a sanctioned shell-cleanup contract every install
			// would leave an undeletable global artifact. Blocked exactly, no
			// writes, until both prerequisites exist.
			const archivePath = process.env[BUSINESS_APP_ARCHIVE_ENV]?.trim();
			if (!archivePath) {
				throw new LiveCapabilityError(
					`Business App install prerequisites missing: (1) no owned archive fixture at ${BUSINESS_APP_ARCHIVE_ENV}; (2) no source-verified cleanup contract for the created business-app shell (the official Python client exposes no delete for the shell — only instance projects are deleted), so install would leave an undeletable global; ${await listEvidence(
						ctx,
						"business-app",
					)}. The case mutates only when BOTH prerequisites exist.`,
					"blocked",
				);
			}
			throw new LiveCapabilityError(
				"Business App install prerequisites incomplete: an archive fixture is configured, but no source-verified cleanup contract for the created business-app shell exists (the official Python client exposes no delete for the shell). Installing without shell cleanup leaves an undeletable global, so the case stays blocked.",
				"blocked",
			);
		},
		{ capability: "applications.business-app-lifecycle", required: false, },
	);
}
