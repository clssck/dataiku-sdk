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
//
// App-instance creation from an OWNED template runs in generated-key mode
// through the shared LiveContext.runGeneratedAppCreation authorization: the
// payload omits targetProjectKey, the CLI generates the target key, the
// harness binds the created project, and the precursor carries the
// pendingDependentCreation marker until the creation outcome is proven. The
// strict explicit-target path (reserved key, --to) is retained for the
// read-only EXTERNAL template binding only.
import { expect, } from "bun:test";
import { provisionTrainedSavedModel, } from "./live-capabilities.js";
import { LiveCapabilityError, LiveCommandError, type LiveContext, } from "./live-context.js";

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
 * Explicit opt-in for the EXTERNAL application resources. An external
 * template is only ever named by this variable — never guessed from list
 * output — and external templates/business apps are read-only surfaces: they
 * are never written. When the variable is unset, the dss app cases that need
 * a template self-provision a disposable owned one instead of blocking.
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
 * A template binding names the template surface a case exercises. An owned
 * self-provisioned template is identified by its own case-owned projectKey,
 * which is bound and therefore cleanup-guarded. An external template (explicit
 * DATAIKU_LIVE_APP_TEMPLATE_ID opt-in) is identified by its appId ONLY: an app
 * id is not a project key, so the external member carries no projectKey and
 * every project-scoped use must narrow to the owned member first.
 */
interface OwnedTemplateBinding {
	appId: string;
	projectKey: string;
	external: false;
}

interface ExternalTemplateBinding {
	appId: string;
	external: true;
}

type TemplateBinding = OwnedTemplateBinding | ExternalTemplateBinding;

/**
 * Precursor recovery guard, shared by the owned-template fixture, the
 * successor lifecycle and the successor preflight: armed with the PRECURSOR
 * (template or predecessor instance) project key, it reports whether the
 * precursor may still be the source of an unresolved descendant creation. Two
 * independent signals retain the precursor for recovery instead of deleting it
 * under a possible in-flight clone: (1) a `pendingDependentCreation` marker on
 * the precursor — a generated-mode creation's target key does not exist before
 * the creation POST, so the marker (not any receipt) is the only truthful
 * in-flight evidence; (2) a descendant reservation created since arming that
 * is still pending — a creation whose POST outcome is unconfirmed. The
 * precursor is then retained for recovery. A strict no-POST refusal clears its
 * reservation intent centrally (and the shared generated-creation
 * authorization clears the marker), so it never trips this guard and normal
 * strict cleanup still runs. A `null` precursor (external-template cases,
 * whose template is never owned by this run) only observes new pending
 * reservations.
 */
function unconfirmedDescendantGuard(
	ctx: LiveContext,
	precursorKey: string | null,
): () => boolean {
	const snapshot = new Set(ctx.projects.map(project => project.key),);
	return () =>
		(precursorKey !== null
			&& Boolean(
				ctx.projects.find(project => project.key === precursorKey)?.pendingDependentCreation,
			))
		|| ctx.projects.some(project => !snapshot.has(project.key,) && project.state === "pending");
}

/**
 * Callback-scoped owned Dataiku App template fixture: a case-owned project is
 * converted to APP_TEMPLATE through the generic `project settings-set`
 * surface (live-proven: settings and project details both report
 * APP_TEMPLATE, and the initialized app manifest is readable), then its
 * DSS-initialized manifest plus owned version markers are persisted through
 * `app save-instance-manifest` (template-only by the SDK guard). The appId is
 * read from the persisted manifest's `id` field and is never assumed to equal
 * the project key. The case-owned project is deleted in `finally`, so an
 * unselected case never provisions and a selected case always cleans up —
 * with one recovery guard: a body failure that leaves an unresolved
 * descendant creation (a generated-mode pendingDependentCreation marker on
 * this template, or a NEW pending explicit reservation) may have an in-flight
 * creation POST, so the template is retained for recovery instead of being
 * deleted under a possible clone; the failure itself is always reported.
 */
async function withOwnedAppTemplate<T,>(
	ctx: LiveContext,
	label: string,
	body: (template: OwnedTemplateBinding,) => Promise<T>,
): Promise<T> {
	const projectKey = await ctx.createProject(label,);
	const hasUnconfirmedChild = unconfirmedDescendantGuard(ctx, projectKey,);
	let retainedForRecovery = false;
	try {
		await ctx.run([
			"project",
			"settings-set",
			"--data",
			JSON.stringify({ projectAppType: "APP_TEMPLATE", },),
		], { projectKey, },);
		const settings = await ctx.run<JsonRecord>(["project", "settings-get",], { projectKey, },);
		expect(settings["projectAppType"],).toBe("APP_TEMPLATE",);

		const initialized = await ctx.run<JsonRecord>(["app", "instance-manifest",], {
			projectKey,
		},);
		const appId = asString(initialized["id"],);
		if (!appId) {
			throw new Error(
				`Owned template project ${projectKey} exposed no generated app id in its initialized manifest.`,
			);
		}
		const knownManifest = {
			...initialized,
			version: `live-${ctx.iteration}`,
			versionNotes: `Live applications owned template ${ctx.iteration}`,
		};
		await ctx.run([
			"app",
			"save-instance-manifest",
			"--data",
			JSON.stringify(knownManifest,),
			"--project-key",
			projectKey,
		],);
		const persisted = await ctx.run<JsonRecord>(["app", "instance-manifest",], { projectKey, },);
		expect(persisted["version"],).toBe(knownManifest.version,);

		return await body({ appId, projectKey, external: false, },);
	} catch (error) {
		// A creation whose POST outcome is indeterminate leaves its descendant
		// unresolved — a generated-mode creation carries the
		// pendingDependentCreation marker on this template instead of a
		// reserved receipt — so this template may be the source of an
		// in-flight clone: it is retained for recovery and the failure is
		// re-thrown. An exact no-POST refusal clears its reservation intent
		// centrally (and the marker), so the normal strict cleanup still runs.
		retainedForRecovery = hasUnconfirmedChild();
		throw error;
	} finally {
		if (!retainedForRecovery) await ctx.deleteProject(projectKey,);
	}
}

/**
 * Resolve the template prerequisite for a template-gated case: an explicit
 * DATAIKU_LIVE_APP_TEMPLATE_ID selects the external read-only template (its
 * appId only — no project key is derived for it), else the case
 * self-provisions an owned disposable template through withOwnedAppTemplate
 * (and its cleanup contract).
 */
async function withTemplateBinding<T,>(
	ctx: LiveContext,
	label: string,
	body: (template: TemplateBinding,) => Promise<T>,
): Promise<T> {
	const appId = process.env[APP_TEMPLATE_ID_ENV]?.trim();
	if (appId) return await body({ appId, external: true, },);
	return await withOwnedAppTemplate(ctx, label, body,);
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
 * Strict explicit-target path for the EXTERNAL template binding: create an
 * app instance from a template into a reserved owned project (the reserved
 * key is named as targetProjectKey and proven absent before the POST) and wait
 * for the creation future. A non-terminal creation state is a genuine
 * failure, never masked, and the pre-POST target-absence refusal propagates
 * untouched for central classification instead of being re-mapped here. Owned
 * templates never take this path: createTemplateInstance runs them in
 * generated mode instead.
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
 * Create an app instance from the bound template and return the created
 * project key, dispatching on the template kind:
 * - EXTERNAL binding: the strict explicit path (createAppInstance into a
 *   reserved owned project, then bind) — an app id is never treated as a
 *   project key.
 * - OWNED binding: generated mode through the shared
 *   LiveContext.runGeneratedAppCreation authorization, with the template
 *   project as the precursor. The payload omits targetProjectKey, so the CLI
 *   generates the target key client-side; the returned key is already
 *   reserved and incarnation-bound by that shared contract, and the precursor
 *   carries the pendingDependentCreation marker until the outcome is proven.
 * Nothing here ever fabricates a projectKey from the app id, and a partial
 * binding failure propagates untouched (never a source-skipping catch).
 */
async function createTemplateInstance(
	ctx: LiveContext,
	template: TemplateBinding,
	reserveLabel: string,
	label: string,
): Promise<string> {
	if (template.external) {
		const reservedKey = await ctx.reserveProject(reserveLabel,);
		await createAppInstance(ctx, template.appId, reservedKey, label,);
		await ctx.bindProject(reservedKey,);
		return reservedKey;
	}
	return await ctx.runGeneratedAppCreation(
		[
			"app",
			"create-instance",
			template.appId,
			"--data",
			JSON.stringify({ targetProjectName: label, },),
			"--wait",
			"--timeout",
			"180000",
			"--poll-interval",
			"3000",
		],
		template.projectKey,
	);
}

/** Tile types present in a manifest's homepage sections, sorted for comparison. */
function manifestTileTypes(manifest: JsonRecord,): string[] {
	const types = new Set<string>();
	const sections = Array.isArray(manifest["homepageSections"],) ? manifest["homepageSections"] : [];
	for (const section of sections) {
		const record = asRecord(section,);
		const tiles = record && Array.isArray(record["tiles"],) ? record["tiles"] : [];
		for (const tile of tiles) {
			const type = asString(asRecord(tile,)?.["type"],);
			if (type) types.add(type,);
		}
	}
	return [...types,].sort();
}

/**
 * The live-proven owned-template Designer flow, seeded exactly once (this
 * case only, on the owned path): a project variable feeds a runtime form tile
 * (PROJECT_VARIABLES_EDIT), a custom-Python scenario (SCENARIO_RUN) reads that
 * variable and writes result.json into a managed folder, and a download tile
 * (DOWNLOAD_MANAGED_FOLDER_FILE) points at that exact file. Every step stays
 * inside the case-owned template project — the external override never enters
 * this function, so an external template is never written.
 */
async function exerciseOwnedTemplateDesignerFlow(
	ctx: LiveContext,
	template: OwnedTemplateBinding,
): Promise<void> {
	const { projectKey, } = template;
	await ctx.run([
		"variable",
		"set",
		"--standard",
		JSON.stringify({ designer_name: "Ada", },),
	], { projectKey, },);
	const folder = await ctx.run<JsonRecord>([
		"folder",
		"create",
		"--name",
		"Designer results",
		"--connection",
		ctx.connection,
	], { projectKey, },);
	const folderId = requireId(folder["id"], "created managed folder",);

	const scenario = "DESIGNER_RUN";
	await ctx.run([
		"scenario",
		"create",
		scenario,
		"Generate Designer result",
		"--type",
		"custom_python",
	], { projectKey, },);
	await ctx.run([
		"scenario",
		"update",
		scenario,
		"--data",
		JSON.stringify({ params: { envSelection: { envMode: "INHERIT", }, }, },),
	], { projectKey, },);
	// The scenario inherits the project's Python env, reads the project
	// variable, and writes the result file into the managed folder.
	const script = "import dataiku,json\n"
		+ `folder_id=json.loads(${JSON.stringify(JSON.stringify(folderId,),)},)\n`
		+ "name=dataiku.get_custom_variables()['designer_name']\n"
		+ "with dataiku.Folder(folder_id).get_writer('result.json') as writer:\n"
		+ "    writer.write(json.dumps(dict(name=name,message='Hello '+name)).encode('utf-8'))\n";
	await ctx.run([
		"scenario",
		"payload-set",
		scenario,
		"--data",
		JSON.stringify({ script, extension: "py", },),
	], { projectKey, },);

	const initialized = await ctx.run<JsonRecord>(["app", "instance-manifest",], { projectKey, },);
	await ctx.run([
		"app",
		"save-instance-manifest",
		"--data",
		JSON.stringify({
			...initialized,
			version: `live-${ctx.iteration}`,
			homepageSections: [
				{
					title: "Designer workflow",
					tiles: [
						{
							type: "PROJECT_VARIABLES_EDIT",
							behavior: "MODAL",
							params: [{ name: "designer_name", label: "Name", type: "STRING", },],
						},
						{ type: "SCENARIO_RUN", scenarioId: scenario, buttonText: "Generate result", },
						{ type: "DOWNLOAD_MANAGED_FOLDER_FILE", folderId, itemPath: "result.json", },
					],
				},
			],
		},),
		"--project-key",
		projectKey,
	],);

	// The source API strips section titles, so only the tile contract is
	// asserted from the persisted manifest — never a byte-identical echo.
	const persisted = await ctx.run<JsonRecord>(["app", "instance-manifest",], { projectKey, },);
	expect(manifestTileTypes(persisted,),).toEqual([
		"DOWNLOAD_MANAGED_FOLDER_FILE",
		"PROJECT_VARIABLES_EDIT",
		"SCENARIO_RUN",
	],);

	// validate-manifest must CHECK every source-verifiable kind (form variable,
	// scenario id, managed-folder id) — a skipped kind would be a false pass.
	const validation = await ctx.run<JsonRecord>(["app", "validate-manifest",], { projectKey, },);
	expect(validation["valid"],).toBe(true,);
	expect(validation["errors"],).toEqual([],);
	const checks = Array.isArray(validation["checks"],) ? validation["checks"] : [];
	for (const kind of ["scenario", "folder", "variable",] as const) {
		const check = checks.map(entry => asRecord(entry,))
			.find(entry => asString(entry?.["kind"],) === kind);
		expect(check?.["status"],).toBe("ok",);
	}

	// The runtime form variable must reach the scenario output: change it and
	// prove the exact downloaded result reflects the new value.
	await ctx.run([
		"variable",
		"set",
		"--standard",
		JSON.stringify({ designer_name: "Grace", },),
	], { projectKey, },);
	const run = await ctx.run<JsonRecord>([
		"scenario",
		"run",
		scenario,
		"--wait",
		"--timeout",
		"120000",
	], { projectKey, },);
	expect(run["success"],).toBe(true,);
	expect(run["outcome"],).toBe("SUCCESS",);

	const output = await ctx.writeFile(`designer-result-i${ctx.iteration}.json`, "",);
	await ctx.run(["folder", "download", folderId, "result.json", output,], { projectKey, },);
	expect(await Bun.file(output,).json(),).toEqual({ name: "Grace", message: "Hello Grace", },);

	// Download-tile safety: once the referenced folder is gone the same
	// manifest must NOT validate — a missing folder reference is a real
	// defect, never a silently skipped check.
	await ctx.run(["folder", "delete", folderId,], { projectKey, },);
	const afterDeletion = await ctx.client.applications.validateAppManifest(persisted, projectKey,);
	expect(afterDeletion.valid,).toBe(false,);
	expect(afterDeletion.errors.some(issue => issue.code === "MISSING_FOLDER"),).toBe(true,);
}

/**
 * The template-prerequisite case, consolidated from the capabilities module
 * with its original case id, capability and required flag. The external
 * override (explicit DATAIKU_LIVE_APP_TEMPLATE_ID) stays strictly read-only:
 * manifest + instances by appId, never a project-scoped read or write. Without
 * the override an owned template is self-provisioned (withOwnedAppTemplate
 * owns its teardown) and executes its reads plus the Designer flow
 * independently of any app-instance creation.
 */
async function exerciseAppTemplatePrerequisite(ctx: LiveContext,): Promise<void> {
	await withTemplateBinding(ctx, "apptmpl", async (template,) => {
		const manifest = await ctx.run<JsonRecord>(["app", "manifest", template.appId,],);
		expect(asRecord(manifest,) === undefined,).toBe(false,);
		const instances = await ctx.run<unknown[]>(["app", "instances", template.appId,],);
		expect(Array.isArray(instances,),).toBe(true,);
		if (template.external) return;
		// Owned template reads, independent of instance creation: the stored
		// instance manifest and the raw persisted version markers of the
		// case-owned template project.
		const stored = await ctx.run<JsonRecord>(["app", "instance-manifest",], {
			projectKey: template.projectKey,
		},);
		expect(asRecord(stored,) === undefined,).toBe(false,);
		const version = await ctx.run<JsonRecord>(["app", "manifest-version",], {
			projectKey: template.projectKey,
		},);
		expect(asRecord(version,) === undefined,).toBe(false,);
		await exerciseOwnedTemplateDesignerFlow(ctx, template,);
	},);
}

/**
 * The instance-lifecycle case, consolidated from the capabilities module with
 * its original case id, actions and required flag: create an app instance from
 * the bound template, then verify API readiness and compare the instance
 * manifest with the template. External templates keep the strict
 * reserved-target path; owned templates run generated mode through
 * createTemplateInstance and the returned key is already reserved and
 * incarnation-bound. The template itself is never modified; the created
 * project is torn down incarnation-guarded in `finally` — an unresolved
 * creation returns no key and is never guessed at (the template fixture's
 * recovery guard retains the precursor).
 */
async function exerciseAppInstanceLifecycle(
	ctx: LiveContext,
	template: TemplateBinding,
): Promise<void> {
	let targetKey: string | undefined;
	try {
		targetKey = await createTemplateInstance(
			ctx,
			template,
			"appinst",
			`Live applications instance ${ctx.iteration}`,
		);

		const verification = await ctx.run<JsonRecord>([
			"app",
			"verify-instance",
			template.appId,
			"--project-key",
			targetKey,
		],);
		expect(verification["valid"] === true && verification["apiReady"] === true,).toBe(true,);

		const comparison = await ctx.run<JsonRecord>([
			"app",
			"compare-manifest",
			template.appId,
			"--project-key",
			targetKey,
		],);
		expect(asRecord(comparison,) === undefined,).toBe(false,);
	} finally {
		if (targetKey !== undefined) await ctx.deleteProject(targetKey,);
	}
}

/**
 * The template-surface case: with an explicit DATAIKU_LIVE_APP_TEMPLATE_ID the
 * external template is used read-only; otherwise a case-owned template is
 * self-provisioned (see withOwnedAppTemplate) and deleted in the case
 * teardown. Templates are never guessed and external ones are never modified.
 * Reads the template manifest version, creates a case-owned instance (strict
 * reserved target for the external template, generated mode for the owned
 * one), and exercises the read-only instance surface. A pre-POST
 * target-absence refusal (external path) propagates untouched for central
 * classification, never bypassed.
 */
async function exerciseAppTemplateSurface(
	ctx: LiveContext,
	template: TemplateBinding,
): Promise<void> {
	const { appId, } = template;
	if (template.external === false) {
		// The OWNED template's project key is bound and known, so its raw
		// persisted version markers are readable (an external template is
		// identified only by appId; its project key is not derivable here).
		const templateVersion = await ctx.run<JsonRecord>(["app", "manifest-version",], {
			projectKey: template.projectKey,
		},);
		expect(asRecord(templateVersion,) === undefined,).toBe(false,);
	}

	const label = `Live applications surface ${ctx.iteration}`;
	let targetKey: string | undefined;
	try {
		targetKey = await createTemplateInstance(ctx, template, "appsurf", label,);

		const instanceVersion = await ctx.run<JsonRecord>(["app", "manifest-version",], {
			projectKey: targetKey,
		},);
		expect(asRecord(instanceVersion,) === undefined,).toBe(false,);
		const instanceManifest = await ctx.run<JsonRecord>(["app", "instance-manifest",], {
			projectKey: targetKey,
		},);
		expect(asRecord(instanceManifest,) === undefined,).toBe(false,);
		const validation = await ctx.run<JsonRecord>(["app", "validate-manifest",], {
			projectKey: targetKey,
		},);
		expect(validation["valid"],).toBe(true,);
		const comparison = await ctx.run<JsonRecord>([
			"app",
			"compare-manifest",
			appId,
			"--project-key",
			targetKey,
		],);
		expect(asRecord(comparison,) === undefined,).toBe(false,);
		const verification = await ctx.run<JsonRecord>([
			"app",
			"verify-instance",
			appId,
			"--project-key",
			targetKey,
		],);
		expect(verification["valid"] === true && verification["apiReady"] === true,).toBe(true,);
	} finally {
		if (targetKey !== undefined) await ctx.deleteProject(targetKey,);
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
 * snapshot + diff against the live project, an app-manifest save round-trip
 * against a case-owned APP_TEMPLATE project (save-instance-manifest is
 * template-only by the SDK guard, which is never suppressed — classic app
 * instance manifests are read-only through that endpoint), and the
 * instance's own app.delete-instance (the official disposal path — verified
 * against the Python client, whose DSSBusinessAppInstance.delete delegates to
 * a project delete) with the bound incarnation guard. The instance is created
 * through createTemplateInstance (strict reserved target for an external
 * template, generated mode for an owned one); both paths return a bound
 * project identity. Template-surface (manifest/version/validate/compare/
 * verify) stays in the template-surface case. All reads/writes target
 * case-owned projects only.
 */
async function exerciseAppInstanceOps(
	ctx: LiveContext,
	template: TemplateBinding,
): Promise<void> {
	const label = `Live applications ops ${ctx.iteration}`;
	let targetKey: string | undefined;
	try {
		targetKey = await createTemplateInstance(ctx, template, "appops", label,);

		// Snapshot the owned instance permissions, then diff it against the
		// live project (a faithful snapshot produces an empty difference).
		const snapshotPath = `${ctx.dir}/app-permissions-${ctx.iteration}.json`;
		const snapshot = await ctx.run<JsonRecord>([
			"app",
			"permissions-snapshot",
			"--output",
			snapshotPath,
			"--project-key",
			targetKey,
		],);
		expect(asRecord(snapshot,) === undefined,).toBe(false,);
		const diff = await ctx.run<JsonRecord>([
			"app",
			"permissions-diff",
			"--file",
			snapshotPath,
			"--project-key",
			targetKey,
		],);
		expect(asRecord(diff,) === undefined,).toBe(false,);

		// Manifest save round-trip on a case-owned APP_TEMPLATE project (the
		// save guard accepts templates only, so the successful save itself
		// re-proves the template type): read the owned template manifest, save
		// it back verbatim, and confirm the read-back still carries the
		// persisted manifest identity. projectAppType is re-read from the
		// settings surface it actually lives on: live DSS's app-manifest
		// response omits it (the SDK's own fallback probes document this), so
		// asserting it on the raw manifest is asserting a field DSS never
		// returns there.
		await withOwnedAppTemplate(ctx, "appopstmpl", async (manifestTemplate,) => {
			const manifestBefore = await ctx.run<JsonRecord>(["app", "instance-manifest",], {
				projectKey: manifestTemplate.projectKey,
			},);
			await ctx.run([
				"app",
				"save-instance-manifest",
				"--data",
				JSON.stringify(manifestBefore,),
				"--project-key",
				manifestTemplate.projectKey,
			],);
			const manifestAfter = await ctx.run<JsonRecord>(["app", "instance-manifest",], {
				projectKey: manifestTemplate.projectKey,
			},);
			expect(asRecord(manifestAfter,) === undefined,).toBe(false,);
			// The save round-trip persisted the manifest identity verbatim.
			expect(manifestAfter["id"],).toBe(manifestBefore["id"],);
			expect(manifestAfter["version"],).toBe(manifestBefore["version"],);
			// The template type is sourced from project settings (its raw
			// shape), not from the app-manifest response.
			const settings = await ctx.run<JsonRecord>(["project", "settings-get",], {
				projectKey: manifestTemplate.projectKey,
			},);
			expect(settings["projectAppType"],).toBe("APP_TEMPLATE",);
		},);

		// The official disposal of an app-instance project (the Python
		// client's DSSBusinessAppInstance.delete delegates to a project
		// delete): the incarnation-guarded app.delete-instance on the
		// case-owned instance.
		await ctx.run([
			"app",
			"delete-instance",
			"--project-key",
			targetKey,
			"--expect-project-incarnation",
			boundIncarnation(ctx, targetKey,),
		],);
	} finally {
		// app.delete-instance already removed the project on the success path;
		// ctx.deleteProject is idempotent (deleted state short-circuits) and is
		// the fallback when the case failed before deletion. An unresolved
		// creation returns no key and is never guessed at.
		if (targetKey !== undefined) await ctx.deleteProject(targetKey,);
	}
}

/**
 * Read-only successor preflight between the case-owned predecessor instance
 * and a fresh successor target: validates the template, verifies the
 * predecessor, and proves the target gates without changing anything. An
 * EXTERNAL template names a strictly reserved target (--to) — the pre-POST
 * target-absence refusal propagates untouched for central classification and
 * the never-created reservation is left to the run-end reservation
 * accounting. An OWNED template runs GENERATED-mode preflight: the successor
 * would be created with no --to, so the CLI generates the target key itself
 * and the read-only preflight runs without naming any target — nothing is
 * reserved and no key is fabricated. A preflight is read-only either way, so
 * a failure here is reported as-is, never reinterpreted as an expected
 * environment outcome.
 */
async function exerciseAppSuccessorPreflight(
	ctx: LiveContext,
	template: TemplateBinding,
	predecessorKey: string,
): Promise<void> {
	if (template.external) {
		const targetKey = await ctx.reserveProject("appnew",);
		const preflight = await ctx.run<JsonRecord>([
			"app",
			"successor-preflight",
			template.appId,
			"--from",
			predecessorKey,
			"--to",
			targetKey,
		],);
		expect(asRecord(preflight,) === undefined,).toBe(false,);
		return;
	}
	const preflight = await ctx.run<JsonRecord>([
		"app",
		"successor-preflight",
		template.appId,
		"--from",
		predecessorKey,
	],);
	expect(asRecord(preflight,) === undefined,).toBe(false,);
	// Frozen generated-mode contract (source-verified): the read-only preflight
	// allocates nothing, so it reports the generated-apply mode instead of
	// naming or probing a target project key — a silent fallback to an explicit
	// target would be a real regression, not an environment outcome. The
	// receipt therefore omits target.projectKey entirely.
	expect(preflight["targetPreflight"],).toBe("not-applicable-generated-key",);
	expect(preflight["targetProjectKeyGeneratedDuringApply"],).toBe(true,);
	expect(asString(asRecord(preflight["target"],)?.["projectKey"],),).toBeUndefined();
}

/**
 * Full successor lifecycle from a case-owned predecessor: create the
 * predecessor instance from the template, create the successor instance
 * beside it, verify the successor, then dispose of BOTH instance projects
 * through the sanctioned project-delete path (the predecessor is never
 * modified by the successor creation).
 *
 * An EXTERNAL template keeps the strict explicit path (reserved target keys,
 * successor named via --to). An OWNED template runs generated mode end to
 * end: the predecessor comes from createTemplateInstance (the template is its
 * precursor) and the successor from the shared runGeneratedAppCreation
 * authorization with the predecessor as precursor and NO --to — the CLI
 * generates the successor key, and the returned key is already
 * incarnation-bound.
 *
 * Recovery semantics: an unresolved successor creation leaves the
 * predecessor carrying its pendingDependentCreation marker (or, on the strict
 * path, a fresh pending successor reservation), so the predecessor is
 * retained for recovery instead of being deleted under a possible in-flight
 * clone. The failure itself is always re-thrown — never swallowed to skip
 * source steps.
 */
async function exerciseAppSuccessorLifecycle(
	ctx: LiveContext,
	template: TemplateBinding,
): Promise<void> {
	const predecessorKey = await createTemplateInstance(
		ctx,
		template,
		"appold",
		`Live applications predecessor ${ctx.iteration}`,
	);
	const hasUnconfirmedSuccessor = unconfirmedDescendantGuard(ctx, predecessorKey,);
	const successorLabel = `Live applications successor ${ctx.iteration}`;
	let predecessorRetainedForRecovery = false;
	try {
		let successorKey: string;
		if (template.external) {
			successorKey = await ctx.reserveProject("appnew",);
			const creation = await ctx.run<JsonRecord>([
				"app",
				"create-successor-instance",
				template.appId,
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
			if (creation["success"] !== true) {
				// CREATE_FAILED / INDETERMINATE / VERIFICATION_FAILED are
				// genuine defects or ambiguous outcomes: reported as failures,
				// never masked.
				const state = asString(creation["state"],) ?? "UNKNOWN";
				throw new Error(
					`Successor instance creation did not complete (state=${state}): ${
						JSON.stringify(creation,).slice(0, 300,)
					}`,
				);
			}
			await ctx.bindProject(successorKey,);
		} else {
			successorKey = await ctx.runGeneratedAppCreation(
				[
					"app",
					"create-successor-instance",
					template.appId,
					"--from",
					predecessorKey,
					"--name",
					successorLabel,
					"--timeout",
					"180000",
					"--poll-interval",
					"3000",
				],
				predecessorKey,
			);
		}
		try {
			const verification = await ctx.run<JsonRecord>([
				"app",
				"verify-instance",
				template.appId,
				"--project-key",
				successorKey,
			],);
			expect(verification["valid"] === true && verification["apiReady"] === true,).toBe(true,);
		} finally {
			await ctx.deleteProject(successorKey,);
		}
	} catch (error) {
		predecessorRetainedForRecovery = hasUnconfirmedSuccessor();
		throw error;
	} finally {
		if (!predecessorRetainedForRecovery) await ctx.deleteProject(predecessorKey,);
	}
}

/**
 * Permissions restore cycle on a case-owned instance: snapshot, ALTER the
 * owned instance's permissions (borrow a read-only group rule via
 * project permissions-set), restore from the snapshot, and verify the live
 * permissions match the snapshot again. The alteration is real (a concrete
 * permission rule lands), so the restore proves an actual write, never a
 * no-op echo. The instance is created through createTemplateInstance (strict
 * reserved target for an external template, generated mode for an owned
 * one); an unresolved creation returns no key and is never guessed at.
 */
async function exerciseAppPermissionsRestoreCycle(
	ctx: LiveContext,
	template: TemplateBinding,
): Promise<void> {
	let targetKey: string | undefined;
	try {
		targetKey = await createTemplateInstance(
			ctx,
			template,
			"appperm",
			`Live applications permissions ${ctx.iteration}`,
		);

		const snapshotPath = `${ctx.dir}/app-permissions-restore-${ctx.iteration}.json`;
		await ctx.run([
			"app",
			"permissions-snapshot",
			"--output",
			snapshotPath,
			"--project-key",
			targetKey,
		],);

		// Real alteration of the OWNED instance's permissions: borrow the
		// run-owner identity as a non-admin extra rule the snapshot does not
		// have, then restore the snapshot and verify the extra rule is gone.
		const before = await ctx.client.projects.getPermissions(targetKey,);
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
			], { projectKey: targetKey, },);
			const altered = await ctx.client.projects.getPermissions(targetKey,);
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
			targetKey,
		],);
		expect(asRecord(restored,) === undefined,).toBe(false,);
		const after = await ctx.client.projects.getPermissions(targetKey,);
		if (added) {
			expect(
				after.permissions?.some(rule => rule.user === ownerLogin && rule.admin === false),
			).toBe(false,);
		}
		expect(after.permissions?.length,).toBe(baseline.length,);
	} finally {
		if (targetKey !== undefined) await ctx.deleteProject(targetKey,);
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
 * model, an API Deployer, a configured template) cannot hide the others. This
 * module is also the single owner of applications.template-prerequisite and
 * applications.instance-lifecycle (consolidated from the capabilities module
 * with their original case ids, actions and required flags).
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
		"applications.template-prerequisite",
		[
			"app.manifest",
			"app.instances",
			"app.instance-manifest",
			"app.manifest-version",
			"app.save-instance-manifest",
			"variable.set",
			"folder.create",
			"scenario.create",
			"scenario.update",
			"scenario.payload-set",
			"app.validate-manifest",
			"scenario.run",
			"folder.download",
			"folder.delete",
		],
		async () => {
			await exerciseAppTemplatePrerequisite(ctx,);
		},
		{ capability: "applications.template-read", required: false, },
	);

	await ctx.check(
		"applications.app.template-surface",
		[
			"project.settings-set",
			"app.manifest-version",
			"app.save-instance-manifest",
			"app.create-instance",
			"app.instance-manifest",
			"app.validate-manifest",
			"app.compare-manifest",
			"app.verify-instance",
		],
		async () => {
			await withTemplateBinding(
				ctx,
				"appsurf",
				(template,) => exerciseAppTemplateSurface(ctx, template,),
			);
		},
		{ capability: "applications.template-surface", required: false, },
	);
	await ctx.check(
		"applications.instance-lifecycle",
		[
			"project.create",
			"app.create-instance",
			"app.verify-instance",
			"app.compare-manifest",
			"project.delete",
		],
		async () => {
			await withTemplateBinding(
				ctx,
				"appinst",
				(template,) => exerciseAppInstanceLifecycle(ctx, template,),
			);
		},
		{ capability: "applications.instance-lifecycle", },
	);
	await ctx.check(
		"applications.app.instance-ops",
		[
			"project.settings-set",
			"app.create-instance",
			"app.permissions-snapshot",
			"app.permissions-diff",
			"app.save-instance-manifest",
			"app.delete-instance",
		],
		async () => {
			await withTemplateBinding(ctx, "appops", (template,) => exerciseAppInstanceOps(ctx, template,),);
		},
		{ capability: "applications.app-instance-ops", required: false, },
	);

	await ctx.check(
		"applications.app.successor-preflight",
		[
			"project.settings-set",
			"app.create-instance",
			"app.successor-preflight",
		],
		async () => {
			await withTemplateBinding(ctx, "apppred", async (template,) => {
				const predecessorKey = await createTemplateInstance(
					ctx,
					template,
					"apppred",
					`Live applications preflight predecessor ${ctx.iteration}`,
				);
				const hasUnconfirmedDescendant = unconfirmedDescendantGuard(ctx, predecessorKey,);
				let retainedForRecovery = false;
				try {
					// The preflight itself is read-only (external path names a
					// reserved target; owned path runs generated mode with no
					// target at all), so only the predecessor creation can have
					// an unresolved descendant.
					await exerciseAppSuccessorPreflight(ctx, template, predecessorKey,);
				} catch (error) {
					// A descendant whose outcome is indeterminate (generated
					// marker, or a fresh pending reservation on the strict path)
					// retains the predecessor — the possible clone source — for
					// recovery; a strict no-POST refusal clears its intent
					// centrally and still cleans up. The failure is never
					// reinterpreted as an expected environment outcome.
					retainedForRecovery = hasUnconfirmedDescendant();
					throw error;
				} finally {
					if (!retainedForRecovery) await ctx.deleteProject(predecessorKey,);
				}
			},);
		},
		{ capability: "applications.app-successor-preflight", required: false, },
	);

	await ctx.check(
		"applications.app.successor-lifecycle",
		[
			"project.settings-set",
			"app.create-instance",
			"app.create-successor-instance",
			"app.verify-instance",
			"project.delete",
		],
		async () => {
			await withTemplateBinding(
				ctx,
				"appsucc",
				(template,) => exerciseAppSuccessorLifecycle(ctx, template,),
			);
		},
		{ capability: "applications.app-successor-lifecycle", required: false, },
	);

	await ctx.check(
		"applications.app.permissions-restore-cycle",
		[
			"project.settings-set",
			"app.create-instance",
			"app.permissions-snapshot",
			"project.permissions-set",
			"app.permissions-restore",
			"project.delete",
		],
		async () => {
			await withTemplateBinding(
				ctx,
				"appperm",
				(template,) => exerciseAppPermissionsRestoreCycle(ctx, template,),
			);
		},
		{ capability: "applications.app-permissions-restore", required: false, },
	);

	await ctx.check(
		"applications.app.set-manifest-version",
		[
			"project.settings-set",
			"app.save-instance-manifest",
			"app.set-manifest-version",
			"app.manifest-version",
		],
		async () => {
			// set-manifest-version publishes the persisted template version
			// that new instances inherit, so it is ALWAYS exercised against a
			// self-provisioned owned template (never an external one, which is
			// never written): publish, read back, prove the dry-run persists
			// nothing, and prove a stale expected hash is rejected before any
			// PUT.
			await withOwnedAppTemplate(ctx, "appver", async (template,) => {
				const baseline = await ctx.run<JsonRecord>(["app", "manifest-version",], {
					projectKey: template.projectKey,
				},);
				const baselineHash = asString(baseline["manifestHash"],);
				expect(baselineHash,).toBeTruthy();

				const published = await ctx.run<JsonRecord>([
					"app",
					"set-manifest-version",
					"--manifest-version",
					"1.0.0",
					"--version-notes",
					`Live owned publish ${ctx.iteration}`,
					"--expect-hash",
					baselineHash!,
					"--project-key",
					template.projectKey,
				],);
				expect(published["outcome"],).toBe("persisted",);
				expect(published["persisted"],).toBe(true,);

				const readBack = await ctx.run<JsonRecord>(["app", "manifest-version",], {
					projectKey: template.projectKey,
				},);
				expect(readBack["version"],).toBe("1.0.0",);
				expect(readBack["versionNotes"],).toBe(`Live owned publish ${ctx.iteration}`,);
				const readBackHash = asString(readBack["manifestHash"],);
				expect(readBackHash,).toBeTruthy();

				const dryRun = await ctx.run<JsonRecord>([
					"app",
					"set-manifest-version",
					"--manifest-version",
					"9.9.9",
					"--dry-run",
					"--project-key",
					template.projectKey,
				],);
				expect(dryRun["dryRun"],).toBe(true,);
				expect(dryRun["persisted"],).toBe(false,);
				const afterDryRun = await ctx.run<JsonRecord>(["app", "manifest-version",], {
					projectKey: template.projectKey,
				},);
				expect(afterDryRun["version"],).toBe("1.0.0",);
				expect(afterDryRun["manifestHash"],).toBe(readBackHash,);

				// A stale expected hash is refused before any PUT: the write
				// must fail nonzero and the manifest must stay untouched.
				let staleRefused = false;
				try {
					await ctx.run([
						"app",
						"set-manifest-version",
						"--manifest-version",
						"3.0.0",
						"--expect-hash",
						"0".repeat(64,),
						"--project-key",
						template.projectKey,
					],);
				} catch (error) {
					if (!(error instanceof LiveCommandError)) throw error;
					staleRefused = true;
				}
				expect(staleRefused,).toBe(true,);
				const afterStale = await ctx.run<JsonRecord>(["app", "manifest-version",], {
					projectKey: template.projectKey,
				},);
				expect(afterStale["version"],).toBe("1.0.0",);
				expect(afterStale["manifestHash"],).toBe(readBackHash,);
			},);
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
