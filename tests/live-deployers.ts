// Live deployer module: API Deployer and Project Deployer lifecycles on
// NEW, run-owned, disposable global resources. Ownership rides the
// DOCUMENTED identifiers only: every create body carries the ledger's
// nonce-bearing reserved id in its official id field (api-deployer
// create-infra {id}, create-service {publishedServiceId}, create-deployment
// {deploymentId, publishedServiceId, infraId, version}; project-deployer
// create-infra {id, stage, governCheckPolicy}, create-project
// {publishedProjectKey}, create-deployment {deploymentId,
// publishedProjectKey, infraId, bundleId}) — no description or other
// fictional field is ever sent (source-verified against
// dataiku-api-client-python apideployer.py / projectdeployer.py). Every
// resource is reserved through the owned-global ledger and torn down in
// nested finally blocks in reverse creation order, even when an assertion
// fails. Steps that need an external node (API node / Automation node on
// the infra) are separate cases reported as exact LiveCapabilityError
// blockers — the lab has no node URL/key and never mutates existing global
// settings. Never ctx.cleanup (it deletes the persistent root).
// Case ids were registered by Main; the serialized live-suite dispatcher is
// the only caller of exerciseDeployers.
import { join, } from "node:path";
import { provisionTrainedSavedModel, } from "./live-capabilities.js";
import type { LiveCaseId, } from "./live-cases.js";
import {
	LiveCapabilityError,
	LiveCommandError,
	type LiveContext,
	type OwnedGlobal,
	type OwnedGlobalKind,
} from "./live-context.js";

type JsonRecord = Record<string, unknown>;

function asRecord(value: unknown,): JsonRecord | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value,)
		? value as JsonRecord
		: undefined;
}

function asString(value: unknown,): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

function asArray(value: unknown,): unknown[] {
	return Array.isArray(value,) ? value : [];
}

function require(condition: boolean, message: string,): asserts condition {
	if (!condition) throw new Error(message,);
}

/**
 * Identifier of a deployer object from a create receipt or a light status:
 * the official light-status shapes nest the id under `*BasicInfo.id`
 * (infraBasicInfo, serviceBasicInfo, deploymentBasicInfo, projectBasicInfo);
 * flat receipts of the CLI contract are accepted as fallbacks in
 * most-specific-first order (a deployment settings doc carries several
 * reference ids, so `deploymentId` must win over `publishedProjectKey`),
 * matching the tolerant receipt parsing the disposable-infrastructure
 * module already uses.
 */
function deployerId(value: unknown, basicInfo: string,): string | undefined {
	const record = asRecord(value,);
	if (!record) return undefined;
	return asString(asRecord(record[basicInfo],)?.["id"],)
		?? asString(record["id"],)
		?? asString(record["deploymentId"],)
		?? asString(record["infraId"],)
		?? asString(record["publishedServiceId"],)
		?? asString(record["publishedProjectKey"],);
}

/** True when a deployer list contains the id (nested under `*BasicInfo.id` or flat). */
function listHas(list: unknown, basicInfo: string, id: string,): boolean {
	return asArray(list,).some(item => deployerId(item, basicInfo,) === id);
}

/** Ids of the `packages[]` entries of a published service/project light status. */
function packageIds(status: unknown,): string[] {
	return asArray(asRecord(status,)?.["packages"],)
		.map(item => asString(asRecord(item,)?.["id"],))
		.filter((id,): id is string => id !== undefined);
}

/** Deep search for an exact string value anywhere inside a JSON document. */
function containsString(value: unknown, needle: string,): boolean {
	if (typeof value === "string") return value === needle;
	if (Array.isArray(value,)) return value.some(item => containsString(item, needle,));
	const record = asRecord(value,);
	return record !== undefined
		&& Object.values(record,).some(item => containsString(item, needle,));
}

/** A create is only real when the ledger holds a bound, identity-matched entry. */
function requireBound(entry: OwnedGlobal | undefined, kind: string,): OwnedGlobal {
	if (!entry || entry.state !== "bound" || !entry.id) {
		throw new LiveCapabilityError(
			`${kind} creation did not produce a bound owned-global receipt`,
		);
	}
	return entry;
}

function aggregate(errors: unknown[],): never {
	if (errors.length === 1) throw errors[0];
	throw new AggregateError(
		errors,
		errors.map(e => e instanceof Error ? e.message : String(e,)).join("; ",),
	);
}

/**
 * Guaranteed teardown for one owned global: runs body, then deletes the
 * bound entry through ctx.deleteGlobal (identity re-verified by the ledger),
 * optionally after a `beforeDelete` step the resource needs (API Deployer
 * deployments must be disabled first). Body and teardown errors are both
 * surfaced; a failed teardown never hides a failed body — and never gets
 * swallowed when the body succeeded.
 */
async function withOwnedGlobal<T,>(
	ctx: LiveContext,
	kind: OwnedGlobalKind,
	entry: OwnedGlobal,
	body: (id: string,) => Promise<T>,
	beforeDelete?: (id: string,) => Promise<void>,
): Promise<T> {
	const bound = requireBound(entry, kind,);
	const id = bound.id!;
	const errors: unknown[] = [];
	let result: T | undefined;
	try {
		result = await body(id,);
	} catch (error) {
		errors.push(error,);
	} finally {
		if (beforeDelete) {
			try {
				await beforeDelete(id,);
			} catch (error) {
				errors.push(error,);
			}
		}
		try {
			await ctx.deleteGlobal(kind, id,);
		} catch (error) {
			errors.push(error,);
		}
	}
	if (errors.length) aggregate(errors,);
	return result!;
}

/** Case-owned disposable project, deleted in finally (never the persistent root). */
async function withOwnedProject<T,>(
	ctx: LiveContext,
	label: string,
	body: (projectKey: string,) => Promise<T>,
): Promise<T> {
	const errors: unknown[] = [];
	let key: string | undefined;
	let result: T | undefined;
	try {
		key = await ctx.createProject(label,);
		result = await body(key,);
	} catch (error) {
		errors.push(error,);
	} finally {
		if (key) {
			try {
				await ctx.deleteProject(key,);
			} catch (error) {
				errors.push(error,);
			}
		}
	}
	if (errors.length) aggregate(errors,);
	return result!;
}

// ---------------------------------------------------------------------------
// Shared prerequisites
// ---------------------------------------------------------------------------

/**
 * Infrastructure stage id. The API Deployer exposes its stages
 * (GET /api-deployer/stages → [{id, desc}]); the Project Deployer's own
 * stage list (GET /project-deployer/stages) is not exposed by the SDK, so a
 * project-deployer infra reuses the stage of an existing project-deployer
 * infra when one exists and otherwise the first API Deployer stage id — DSS
 * ships both deployers with the same default stage set, and a server
 * rejection of that id is reported as the real result.
 */
async function apiDeployerStage(ctx: LiveContext,): Promise<string> {
	const stages = await ctx.run<unknown>(["api-deployer", "list-stages",],);
	const stage = asArray(stages,).map(item => asString(asRecord(item,)?.["id"],)).find(Boolean,);
	if (!stage) {
		throw new LiveCapabilityError(
			"api-deployer list-stages returned no stage id; infra creation requires a configured stage.",
		);
	}
	return stage;
}

async function projectDeployerStage(ctx: LiveContext,): Promise<string> {
	const infras = await ctx.run<unknown>(["project-deployer", "list-infras",],);
	for (const item of asArray(infras,)) {
		const stage = asString(asRecord(asRecord(item,)?.["infraBasicInfo"],)?.["stage"],);
		if (stage) return stage;
	}
	return apiDeployerStage(ctx,);
}

/** Official API Deployer infra body: {id, stage, type, governCheckPolicy}. */
async function createApiInfra(ctx: LiveContext, label: string,): Promise<OwnedGlobal> {
	const stage = await apiDeployerStage(ctx,);
	return ctx.createGlobal("api-deployer-infra", label, name => [
		"api-deployer",
		"create-infra",
		"--data",
		JSON.stringify({ id: name, stage, type: "STATIC", governCheckPolicy: "NO_CHECK", },),
	], { id: result => deployerId(result, "infraBasicInfo",), },);
}

/** Official Project Deployer infra body: {id, stage, governCheckPolicy}. */
async function createProjectInfra(ctx: LiveContext, label: string,): Promise<OwnedGlobal> {
	const stage = await projectDeployerStage(ctx,);
	return ctx.createGlobal("project-deployer-infra", label, name => [
		"project-deployer",
		"create-infra",
		"--data",
		JSON.stringify({ id: name, stage, governCheckPolicy: "NO_CHECK", },),
	], { id: result => deployerId(result, "infraBasicInfo",), },);
}

/**
 * Build a REAL API service package inside a case-owned project and download
 * its archive: `api-service create-package <packageId>` (the service was
 * created by createLocalApiService) + `download-package`. The package id
 * becomes the published version id checked against the owned service's
 * `packages[]`.
 */
async function buildApiPackage(
	ctx: LiveContext,
	projectKey: string,
	serviceId: string,
	packageId: string,
): Promise<string> {
	await ctx.run([
		"api-service",
		"create-package",
		serviceId,
		packageId,
		"--release-notes",
		`live deployers package ${ctx.iteration}`,
		"--project-key",
		projectKey,
	],);
	const archive = join(ctx.dir, `deployer-package-${ctx.iteration}-${packageId}.zip`,);
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
	require(downloaded["path"] === archive, "download-package did not write the requested archive",);
	return archive;
}

async function createLocalApiService(
	ctx: LiveContext,
	projectKey: string,
	predictionEndpoint: boolean,
): Promise<string> {
	const serviceId = `live_deployer_svc_i${ctx.iteration}`;
	await ctx.run(["api-service", "create", serviceId, "--project-key", projectKey,],);
	if (predictionEndpoint) {
		const model = await provisionTrainedSavedModel(ctx, projectKey,);
		await ctx.run([
			"api-service",
			"add-prediction-endpoint",
			serviceId,
			"predict",
			model.savedModelId,
			"--project-key",
			projectKey,
		],);
	}
	return serviceId;
}

/**
 * Export a bundle from a case-owned project and download it. Server-default
 * Project Standards evaluation (the instance forbids disabling it — observed
 * raw 400 in live-bundles.ts) and a fresh id (DSS 15 refuses overwriting an
 * exported id). `bundleId` becomes the published project's `packages[]` id.
 */
async function exportBundle(
	ctx: LiveContext,
	projectKey: string,
	bundleId: string,
): Promise<string> {
	const exported = await ctx.run<{ exported?: string; }>([
		"bundle",
		"export",
		bundleId,
		"--release-notes",
		`live deployers bundle ${ctx.iteration}`,
	], { projectKey, },);
	require(exported.exported === bundleId, `bundle export did not echo ${bundleId}`,);
	const archive = join(ctx.dir, `deployer-bundle-${ctx.iteration}-${bundleId}.zip`,);
	const download = await ctx.run<{ path?: string; bytes?: number; }>([
		"bundle",
		"download-exported",
		bundleId,
		"--output",
		archive,
	], { projectKey, },);
	require(
		download.path === archive && typeof download.bytes === "number" && download.bytes > 0,
		"download-exported did not write a non-empty archive",
	);
	return archive;
}

// ---------------------------------------------------------------------------
// API Deployer stack
// ---------------------------------------------------------------------------

interface ApiDeployerStack {
	projectKey: string;
	localServiceId: string;
	infraId: string;
	serviceId: string;
	/**
	 * Published version id: the harness marker of the service reservation
	 * (run-scoped nonce), doubling as the local package id checked in
	 * packages[] — so the version artifact itself is attributable by id.
	 */
	version: string;
}

/**
 * Case-owned project → local API service (+ optional real prediction
 * endpoint) → owned STATIC infra → owned published service (documented
 * `publishedServiceId` = the nonce-bearing reserved name). `stack.version`
 * is the reservation marker (run-scoped), used as the package/version id
 * whenever a case publishes. Publication itself is left to each case so the
 * metadata case never depends on publish-version. Teardown in reverse:
 * published service, infra, project.
 */
async function withApiDeployerStack<T,>(
	ctx: LiveContext,
	label: string,
	options: { predictionEndpoint: boolean; },
	body: (stack: ApiDeployerStack,) => Promise<T>,
): Promise<T> {
	return withOwnedProject(ctx, label, async projectKey => {
		const localServiceId = await createLocalApiService(ctx, projectKey, options.predictionEndpoint,);
		const infra = await createApiInfra(ctx, label,);
		return withOwnedGlobal(ctx, "api-deployer-infra", infra, async infraId => {
			const service = await ctx.createGlobal("api-deployer-service", label, name => [
				"api-deployer",
				"create-service",
				"--data",
				JSON.stringify({ publishedServiceId: name, },),
			], { id: result => deployerId(result, "serviceBasicInfo",), },);
			return withOwnedGlobal(ctx, "api-deployer-service", service, async serviceId => {
				const version = ctx.markerFor("api-deployer-service", service.name,);
				return body({ projectKey, localServiceId, infraId, serviceId, version, },);
			},);
		},);
	},);
}

/**
 * Build the package for `version` in the local service, publish it to the
 * owned published service, and verify the version id is listed in the
 * published service's packages[]. Real package: server-built from the real
 * local service state, downloaded, uploaded.
 */
async function publishApiVersion(
	ctx: LiveContext,
	stack: ApiDeployerStack,
	version: string,
): Promise<void> {
	const archive = await buildApiPackage(ctx, stack.projectKey, stack.localServiceId, version,);
	const published = await ctx.run<JsonRecord>([
		"api-deployer",
		"publish-version",
		stack.serviceId,
		archive,
	],);
	require(published["published"] === true, "publish-version did not report published",);
	const status = await ctx.run<unknown>(["api-deployer", "get-service", stack.serviceId,],);
	require(
		packageIds(status,).includes(version,),
		`published service ${stack.serviceId} does not list version ${version} after publish-version`,
	);
}

/** Official deployment body: {deploymentId, publishedServiceId, infraId, version}. */
async function createApiDeployment(
	ctx: LiveContext,
	label: string,
	stack: ApiDeployerStack,
): Promise<OwnedGlobal> {
	return ctx.createGlobal("api-deployer-deployment", label, name => [
		"api-deployer",
		"create-deployment",
		"--data",
		JSON.stringify({
			deploymentId: name,
			publishedServiceId: stack.serviceId,
			infraId: stack.infraId,
			version: stack.version,
		},),
	], { id: result => deployerId(result, "deploymentBasicInfo",), },);
}

/**
 * Full settings-document round trip (official DSSAPIDeployerDeploymentSettings:
 * GET /settings → mutate `enabled` → PUT the WHOLE document). Disabling is
 * also the official precondition for deletion (delete() refuses an enabled
 * deployment), so the saved value is re-read from both the settings document
 * and the light status.
 */
async function disableApiDeployment(ctx: LiveContext, deploymentId: string,): Promise<void> {
	const settings = await ctx.run<JsonRecord>([
		"api-deployer",
		"deployment-settings",
		deploymentId,
	],);
	require(asRecord(settings,) !== undefined, "deployment-settings returned no document",);
	await ctx.run([
		"api-deployer",
		"save-deployment-settings",
		deploymentId,
		"--data",
		JSON.stringify({ ...settings, enabled: false, },),
	],);
	const saved = await ctx.run<JsonRecord>(["api-deployer", "deployment-settings", deploymentId,],);
	require(saved["enabled"] === false, "save-deployment-settings did not persist enabled=false",);
	const light = await ctx.run<JsonRecord>(["api-deployer", "get-deployment", deploymentId,],);
	const basic = asRecord(light["deploymentBasicInfo"],);
	require(
		basic === undefined || basic["enabled"] === false,
		"deploymentBasicInfo.enabled still true after saving enabled=false",
	);
}

async function apiDeployerMetadata(ctx: LiveContext,): Promise<void> {
	await withApiDeployerStack(ctx, "apidep", { predictionEndpoint: false, }, async stack => {
		const infra = await ctx.run<unknown>(["api-deployer", "get-infra", stack.infraId,],);
		require(asRecord(infra,) !== undefined, "get-infra returned no object",);
		require(
			listHas(
				await ctx.run<unknown>(["api-deployer", "list-infras",],),
				"infraBasicInfo",
				stack.infraId,
			),
			`infra ${stack.infraId} missing from list-infras`,
		);
		const service = await ctx.run<unknown>(["api-deployer", "get-service", stack.serviceId,],);
		require(
			deployerId(service, "serviceBasicInfo",) === stack.serviceId,
			"get-service did not return the owned service",
		);
		require(
			listHas(
				await ctx.run<unknown>(["api-deployer", "list-services",],),
				"serviceBasicInfo",
				stack.serviceId,
			),
			`service ${stack.serviceId} missing from list-services`,
		);
	},);
}

/**
 * Real second version: a second package of the same REAL service (with a
 * STD_PREDICTION endpoint on a model trained in the case project) is built
 * locally, published through `publish-version`, seen in `packages[]`, then
 * removed through `delete-version` and seen gone.
 */
async function apiDeployerPublishVersion(ctx: LiveContext,): Promise<void> {
	await withApiDeployerStack(ctx, "apipub", { predictionEndpoint: true, }, async stack => {
		await publishApiVersion(ctx, stack, stack.version,);
		const second = `${stack.version}_2`;
		await publishApiVersion(ctx, stack, second,);
		const deleted = await ctx.run<JsonRecord>([
			"api-deployer",
			"delete-version",
			stack.serviceId,
			second,
		],);
		require(deleted["deleted"] === true, "delete-version did not report deleted",);
		const remaining = packageIds(
			await ctx.run<unknown>(["api-deployer", "get-service", stack.serviceId,],),
		);
		require(
			remaining.includes(stack.version,),
			`version ${stack.version} disappeared after deleting only ${second}`,
		);
		require(!remaining.includes(second,), `version ${second} still listed after delete-version`,);
	},);
}

async function apiDeployerDeploymentLifecycle(ctx: LiveContext,): Promise<void> {
	await withApiDeployerStack(ctx, "apidepl", { predictionEndpoint: false, }, async stack => {
		await publishApiVersion(ctx, stack, stack.version,);
		const deployment = await createApiDeployment(ctx, "apidepl", stack,);
		await withOwnedGlobal(ctx, "api-deployer-deployment", deployment, async deploymentId => {
			const light = await ctx.run<JsonRecord>(["api-deployer", "get-deployment", deploymentId,],);
			const basic = asRecord(light["deploymentBasicInfo"],);
			require(
				basic !== undefined && basic["publishedServiceId"] === stack.serviceId
					&& basic["infraId"] === stack.infraId,
				"deploymentBasicInfo does not reference the owned service/infra",
			);
			require(
				listHas(
					await ctx.run<unknown>(["api-deployer", "list-deployments",],),
					"deploymentBasicInfo",
					deploymentId,
				),
				`deployment ${deploymentId} missing from list-deployments`,
			);
			const settings = await ctx.run<JsonRecord>([
				"api-deployer",
				"deployment-settings",
				deploymentId,
			],);
			require(
				settings["publishedServiceId"] === stack.serviceId && settings["infraId"] === stack.infraId,
				"deployment settings do not reference the owned service/infra",
			);
			await disableApiDeployment(ctx, deploymentId,);
		}, id => disableApiDeployment(ctx, id,),);
	},);
}

/**
 * Exact external prerequisite for `deploy`: the owned STATIC infra was
 * created with the official body only, so its settings.apiNodes (the only
 * place API nodes live, per DSSAPIDeployerInfraSettings.add_apinode) is
 * empty; the lab has no API node URL/admin key and never edits existing
 * infrastructure settings. Deploy is therefore never issued at a missing
 * target. The real deployment-status read runs first; a server refusal of
 * that read on the node-less infra is recorded with the server's own text
 * as the exact prerequisite (the run's classification comes from actual
 * calls), while success moves on to the explicit deploy blocker.
 */
async function apiDeployerDeploy(ctx: LiveContext,): Promise<void> {
	await withApiDeployerStack(ctx, "apinode", { predictionEndpoint: false, }, async stack => {
		await publishApiVersion(ctx, stack, stack.version,);
		const deployment = await createApiDeployment(ctx, "apinode", stack,);
		await withOwnedGlobal(ctx, "api-deployer-deployment", deployment, async deploymentId => {
			let status: unknown;
			try {
				status = await ctx.run<unknown>(["api-deployer", "deployment-status", deploymentId,],);
			} catch (error) {
				if (error instanceof LiveCommandError) {
					throw new LiveCapabilityError(
						`api-deployer deployment-status ${deploymentId} refused on the node-less owned infra ${stack.infraId}: ${error.message}. Prerequisite: a configured API node (node URL + admin API key) on the infra.`,
					);
				}
				throw error;
			}
			require(asRecord(status,) !== undefined, "deployment-status returned no object",);
			throw new LiveCapabilityError(
				`api-deployer deploy ${deploymentId} needs an API node on infra ${stack.infraId}: the owned STATIC infra was created with no settings.apiNodes entry (official add_apinode needs a node URL + admin API key the lab does not have, and existing infra settings are never mutated), so the update would target no node.`,
			);
		}, id => disableApiDeployment(ctx, id,),);
	},);
}

// ---------------------------------------------------------------------------
// Project Deployer stack
// ---------------------------------------------------------------------------

interface ProjectDeployerStack {
	projectKey: string;
	infraId: string;
	/** Published project key (= the owned source project key). */
	publishedProjectKey: string;
	/** Uploaded bundle id (checked in the published project's packages[]). */
	bundleId: string;
}

/**
 * The published project is keyed by its SOURCE project's key (the SDK's
 * upload-bundle sends no projectKey), so the source project itself is
 * created through ctx.createProjectForGlobal(label): the ledger reserves a
 * nonce-bearing project-deployer-project name and the Design project is
 * created with that EXACT key, making the documented published-project key
 * the ownership carrier. Sequence: owned project → owned infra → bundle
 * export → upload-bundle → bindGlobal (project-status readback) → body.
 * Teardown order: published project (deleteGlobal, raw DELETE), infra, then
 * the Design project (ctx.deleteProject).
 */
async function withProjectDeployerStack<T,>(
	ctx: LiveContext,
	label: string,
	body: (stack: ProjectDeployerStack,) => Promise<T>,
): Promise<T> {
	const errors: unknown[] = [];
	let result: T | undefined;
	const projectKey = await ctx.createProjectForGlobal(label,);
	try {
		const infra = await createProjectInfra(ctx, label,);
		result = await withOwnedGlobal(ctx, "project-deployer-infra", infra, async infraId => {
			const bundleId = ctx.markerFor("project-deployer-project", projectKey,);
			const archive = await exportBundle(ctx, projectKey, bundleId,);
			await ctx.run(["project-deployer", "upload-bundle", archive,],);
			const published = await ctx.bindGlobal("project-deployer-project", projectKey, projectKey,);
			return withOwnedGlobal(
				ctx,
				"project-deployer-project",
				published,
				async publishedProjectKey => {
					const status = await ctx.run<unknown>([
						"project-deployer",
						"project-status",
						publishedProjectKey,
					],);
					require(
						packageIds(status,).includes(bundleId,),
						`published project ${publishedProjectKey} does not list bundle ${bundleId}`,
					);
					return body({ projectKey, infraId, publishedProjectKey, bundleId, },);
				},
			);
		},);
	} catch (error) {
		errors.push(error,);
	} finally {
		try {
			await ctx.deleteProject(projectKey,);
		} catch (error) {
			errors.push(error,);
		}
	}
	if (errors.length) aggregate(errors,);
	return result!;
}

/** Official deployment body: {deploymentId, publishedProjectKey, infraId, bundleId}. */
async function createProjectDeployment(
	ctx: LiveContext,
	label: string,
	stack: ProjectDeployerStack,
): Promise<{ entry: OwnedGlobal; receipt: unknown; }> {
	let receipt: unknown;
	const entry = await ctx.createGlobal("project-deployer-deployment", label, name => [
		"project-deployer",
		"create-deployment",
		"--data",
		JSON.stringify({
			deploymentId: name,
			publishedProjectKey: stack.publishedProjectKey,
			infraId: stack.infraId,
			bundleId: stack.bundleId,
		},),
	], {
		id: result => {
			receipt = result;
			return deployerId(result, "deploymentBasicInfo",);
		},
	},);
	return { entry, receipt, };
}

/**
 * The Project Deployer settings document (official
 * DSSProjectDeployerDeploymentSettings: bundleId + publishedProjectKey, saved
 * as a WHOLE document). GET /settings is not exposed by the SDK, so the
 * document is taken from the create receipt or the light status only when
 * one of them IS the settings document (carries bundleId, publishedProjectKey
 * and infraId — top-level or inside its deploymentBasicInfo) ; a partial PUT
 * is never sent.
 */
function projectDeploymentSettings(candidates: unknown[],): JsonRecord | undefined {
	for (const candidate of candidates) {
		const record = asRecord(candidate,);
		if (!record) continue;
		for (const doc of [record, asRecord(record["deploymentBasicInfo"],),]) {
			if (
				doc && asString(doc["bundleId"],) && asString(doc["publishedProjectKey"],)
				&& asString(doc["infraId"],)
			) return doc;
		}
	}
	return undefined;
}

async function projectDeployerDeploymentLifecycle(ctx: LiveContext,): Promise<void> {
	await withProjectDeployerStack(ctx, "pddepl", async stack => {
		// A second real bundle gives save-settings an observable change
		// (bundleId) that the server must echo back.
		const secondBundle = `${stack.bundleId}_2`;
		const secondArchive = await exportBundle(ctx, stack.projectKey, secondBundle,);
		await ctx.run(["project-deployer", "upload-bundle", secondArchive,],);
		require(
			packageIds(
				await ctx.run<unknown>(["project-deployer", "project-status", stack.publishedProjectKey,],),
			).includes(secondBundle,),
			`second bundle ${secondBundle} missing from the published project`,
		);
		const { entry, receipt, } = await createProjectDeployment(ctx, "pddepl", stack,);
		await withOwnedGlobal(ctx, "project-deployer-deployment", entry, async deploymentId => {
			const light = await ctx.run<JsonRecord>(["project-deployer", "get-deployment", deploymentId,],);
			require(
				deployerId(light, "deploymentBasicInfo",) === deploymentId,
				"get-deployment did not return the owned deployment",
			);
			require(
				listHas(
					await ctx.run<unknown>(["project-deployer", "list-deployments",],),
					"deploymentBasicInfo",
					deploymentId,
				),
				`deployment ${deploymentId} missing from list-deployments`,
			);
			const settings = projectDeploymentSettings([receipt, light,],);
			if (!settings) {
				throw new LiveCapabilityError(
					"project-deployer save-deployment-settings needs the full settings document: GET /project-deployer/deployments/{id}/settings is not exposed by the SDK and neither the create receipt nor get-deployment carried {bundleId, publishedProjectKey, infraId}; a partial PUT is never sent.",
				);
			}
			await ctx.run([
				"project-deployer",
				"save-deployment-settings",
				deploymentId,
				"--data",
				JSON.stringify({ ...settings, bundleId: secondBundle, },),
			],);
			const observed = [
				await ctx.run<unknown>(["project-deployer", "get-deployment", deploymentId,],),
				await ctx.run<unknown>(["project-deployer", "project-status", stack.publishedProjectKey,],),
			];
			require(
				observed.some(doc => containsString(doc, secondBundle,)),
				`saved bundleId ${secondBundle} is not visible in get-deployment or project-status`,
			);
		},);
	},);
}

/**
 * Exact external prerequisite for `deploy`: the owned infra was created with
 * the official body only ({id, stage, governCheckPolicy}); an Automation
 * node is bound through the infra settings the lab never edits and has no
 * node URL/API key for. The real deployment-status read runs first; a
 * server refusal of that read on the node-less infra is recorded with the
 * server's own text as the exact prerequisite, while success moves on to
 * the explicit deploy blocker.
 */
async function projectDeployerDeploy(ctx: LiveContext,): Promise<void> {
	await withProjectDeployerStack(ctx, "pdnode", async stack => {
		const { entry, } = await createProjectDeployment(ctx, "pdnode", stack,);
		await withOwnedGlobal(ctx, "project-deployer-deployment", entry, async deploymentId => {
			let status: unknown;
			try {
				status = await ctx.run<unknown>([
					"project-deployer",
					"deployment-status",
					deploymentId,
				],);
			} catch (error) {
				if (error instanceof LiveCommandError) {
					throw new LiveCapabilityError(
						`project-deployer deployment-status ${deploymentId} refused on the node-less owned infra ${stack.infraId}: ${error.message}. Prerequisite: a configured Automation node (node URL + API key) on the infra.`,
					);
				}
				throw error;
			}
			require(asRecord(status,) !== undefined, "deployment-status returned no object",);
			throw new LiveCapabilityError(
				`project-deployer deploy ${deploymentId} needs an Automation node on infra ${stack.infraId}: the owned infra was created with the official {id, stage, governCheckPolicy} body only, the lab has no Automation node URL/API key, and existing infra settings are never mutated, so the update would target no node.`,
			);
		},);
	},);
}

async function projectDeployerInfraLifecycle(ctx: LiveContext,): Promise<void> {
	const infra = await createProjectInfra(ctx, "pdinfra",);
	await withOwnedGlobal(ctx, "project-deployer-infra", infra, async infraId => {
		require(
			listHas(
				await ctx.run<unknown>(["project-deployer", "list-infras",],),
				"infraBasicInfo",
				infraId,
			),
			`infra ${infraId} missing from list-infras`,
		);
	},);
}

async function projectDeployerUploadBundle(ctx: LiveContext,): Promise<void> {
	await withProjectDeployerStack(ctx, "pdupload", async stack => {
		require(
			listHas(
				await ctx.run<unknown>(["project-deployer", "list-projects",],),
				"projectBasicInfo",
				stack.publishedProjectKey,
			),
			`published project ${stack.publishedProjectKey} missing from list-projects`,
		);
	},);
}

/** Official create-project body: {publishedProjectKey}; empty project, no bundle. */
async function projectDeployerCreateProject(ctx: LiveContext,): Promise<void> {
	const entry = await ctx.createGlobal("project-deployer-project", "pdproj", name => [
		"project-deployer",
		"create-project",
		"--data",
		JSON.stringify({ publishedProjectKey: name, },),
	], { id: result => deployerId(result, "projectBasicInfo",), },);
	await withOwnedGlobal(ctx, "project-deployer-project", entry, async key => {
		const status = await ctx.run<unknown>(["project-deployer", "project-status", key,],);
		require(deployerId(status, "projectBasicInfo",) === key, "project-status did not echo the key",);
		require(
			listHas(
				await ctx.run<unknown>(["project-deployer", "list-projects",],),
				"projectBasicInfo",
				key,
			),
			`published project ${key} missing from list-projects`,
		);
	},);
}

/** Read-only project-deployer coverage: no mutations, no external infra. */
async function projectDeployerReads(ctx: LiveContext,): Promise<void> {
	for (
		const argv of [
			["project-deployer", "list-projects",],
			["project-deployer", "list-deployments",],
			["project-deployer", "list-infras",],
		]
	) {
		require(
			Array.isArray(await ctx.run<unknown>(argv,),),
			`${argv.join(" ",)} did not return an array`,
		);
	}
}

// ---------------------------------------------------------------------------
// Deployer details against NEW owned metadata stacks
// ---------------------------------------------------------------------------

/**
 * The full deployer-details surface (every action of the old
 * infrastructure.deployer-details case) against NEW, case-owned metadata
 * stacks, so details have a guaranteed real target instead of depending on
 * pre-existing instance objects. Every response is validated against the
 * exact owned identities (ledger nonce-bearing ids) and the observable
 * control-plane contract — never a bare mock echo and never a faked
 * healthy deployment. No deploy and no redeploy is issued — the owned STATIC
 * infras carry no API/Automation node (see apiDeployerDeploy /
 * projectDeployerDeploy for the exact external prerequisite) — and
 * deployment-status runs as a plain getter whose raw failure propagates
 * unclassified: any SDK/auth/route/shape error stays a visible failure
 * instead of being converted into an invented unavailable claim. Teardown
 * is the standard reverse-order
 * error-aggregating helpers: deployments before services/infra, published
 * projects before the source project.
 */
export async function exerciseOwnedDeployerDetails(ctx: LiveContext,): Promise<void> {
	await withApiDeployerStack(ctx, "apidet", { predictionEndpoint: false, }, async stack => {
		await publishApiVersion(ctx, stack, stack.version,);
		const deployment = await createApiDeployment(ctx, "apidet", stack,);
		await withOwnedGlobal(ctx, "api-deployer-deployment", deployment, async deploymentId => {
			require(
				listHas(
					await ctx.run<unknown>(["api-deployer", "list-infras",],),
					"infraBasicInfo",
					stack.infraId,
				),
				`owned infra ${stack.infraId} missing from api-deployer list-infras`,
			);
			require(
				listHas(
					await ctx.run<unknown>(["api-deployer", "list-services",],),
					"serviceBasicInfo",
					stack.serviceId,
				),
				`owned service ${stack.serviceId} missing from api-deployer list-services`,
			);
			require(
				listHas(
					await ctx.run<unknown>(["api-deployer", "list-deployments",],),
					"deploymentBasicInfo",
					deploymentId,
				),
				`owned deployment ${deploymentId} missing from api-deployer list-deployments`,
			);
			const infra = await ctx.run<JsonRecord>(["api-deployer", "get-infra", stack.infraId,],);
			require(
				deployerId(infra, "infraBasicInfo",) === stack.infraId
					&& asRecord(infra["infraBasicInfo"],)?.["type"] === "STATIC",
				`api-deployer get-infra did not return the owned STATIC infra ${stack.infraId}`,
			);
			const service = await ctx.run<JsonRecord>(["api-deployer", "get-service", stack.serviceId,],);
			require(
				deployerId(service, "serviceBasicInfo",) === stack.serviceId,
				`api-deployer get-service did not return the owned service ${stack.serviceId}`,
			);
			require(
				packageIds(service,).includes(stack.version,),
				`api-deployer get-service does not list the owned published version ${stack.version}`,
			);
			const light = await ctx.run<JsonRecord>(["api-deployer", "get-deployment", deploymentId,],);
			const basic = asRecord(light["deploymentBasicInfo"],);
			require(
				basic !== undefined && basic["publishedServiceId"] === stack.serviceId
					&& basic["infraId"] === stack.infraId,
				`api-deployer get-deployment ${deploymentId} does not reference the owned service/infra`,
			);
			const settings = await ctx.run<JsonRecord>([
				"api-deployer",
				"deployment-settings",
				deploymentId,
			],);
			require(
				settings["publishedServiceId"] === stack.serviceId && settings["infraId"] === stack.infraId,
				`api-deployer deployment-settings ${deploymentId} do not reference the owned service/infra`,
			);
			const status = await ctx.run<unknown>([
				"api-deployer",
				"deployment-status",
				deploymentId,
			],);
			require(asRecord(status,) !== undefined, "api-deployer deployment-status returned no object",);
		}, id => disableApiDeployment(ctx, id,),);
	},);
	await withProjectDeployerStack(ctx, "pddet", async stack => {
		const { entry, receipt, } = await createProjectDeployment(ctx, "pddet", stack,);
		await withOwnedGlobal(ctx, "project-deployer-deployment", entry, async deploymentId => {
			require(
				listHas(
					await ctx.run<unknown>(["project-deployer", "list-projects",],),
					"projectBasicInfo",
					stack.publishedProjectKey,
				),
				`owned published project ${stack.publishedProjectKey} missing from project-deployer list-projects`,
			);
			require(
				listHas(
					await ctx.run<unknown>(["project-deployer", "list-deployments",],),
					"deploymentBasicInfo",
					deploymentId,
				),
				`owned deployment ${deploymentId} missing from project-deployer list-deployments`,
			);
			const light = await ctx.run<JsonRecord>([
				"project-deployer",
				"get-deployment",
				deploymentId,
			],);
			require(
				deployerId(light, "deploymentBasicInfo",) === deploymentId,
				`project-deployer get-deployment did not return the owned deployment ${deploymentId}`,
			);
			const publishedStatus = await ctx.run<JsonRecord>([
				"project-deployer",
				"project-status",
				stack.publishedProjectKey,
			],);
			require(
				packageIds(publishedStatus,).includes(stack.bundleId,),
				`project-deployer project-status ${stack.publishedProjectKey} does not list the owned bundle ${stack.bundleId}`,
			);
			// GET /settings is not exposed by the SDK for the project deployer;
			// the settings document is observable through the create receipt or
			// the light status only (the identity read-only contract — no
			// partial PUT is ever sent here).
			const settings = projectDeploymentSettings([receipt, light,],);
			require(
				settings !== undefined && settings["publishedProjectKey"] === stack.publishedProjectKey
					&& settings["infraId"] === stack.infraId && settings["bundleId"] === stack.bundleId,
				`project-deployer settings of deployment ${deploymentId} do not carry the owned publishedProjectKey/infraId/bundleId`,
			);
			const status = await ctx.run<unknown>([
				"project-deployer",
				"deployment-status",
				deploymentId,
			],);
			require(
				asRecord(status,) !== undefined,
				"project-deployer deployment-status returned no object",
			);
		},);
	},);
}

// ---------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------

export async function exerciseDeployers(ctx: LiveContext,): Promise<void> {
	await ctx.check(
		"infrastructure.api-deployer" as LiveCaseId,
		[
			"api-deployer.list-stages",
			"api-deployer.create-infra",
			"api-deployer.get-infra",
			"api-deployer.list-infras",
			"api-deployer.create-service",
			"api-deployer.get-service",
			"api-deployer.list-services",
			"api-deployer.delete-service",
			"api-deployer.delete-infra",
		],
		async () => {
			await apiDeployerMetadata(ctx,);
		},
		{ capability: "infrastructure.api-deployer-metadata", },
	);
	await ctx.check(
		"infrastructure.api-deployer.publish-version" as LiveCaseId,
		[
			"api-deployer.create-infra",
			"api-deployer.create-service",
			"api-deployer.publish-version",
			"api-deployer.get-service",
			"api-deployer.delete-version",
			"api-deployer.delete-service",
			"api-deployer.delete-infra",
		],
		async () => {
			await apiDeployerPublishVersion(ctx,);
		},
		{ capability: "infrastructure.api-deployer-versions", },
	);
	await ctx.check(
		"infrastructure.api-deployer.deployment-lifecycle" as LiveCaseId,
		[
			"api-deployer.create-infra",
			"api-deployer.create-service",
			"api-deployer.publish-version",
			"api-deployer.create-deployment",
			"api-deployer.get-deployment",
			"api-deployer.list-deployments",
			"api-deployer.deployment-settings",
			"api-deployer.save-deployment-settings",
			"api-deployer.delete-deployment",
			"api-deployer.delete-service",
			"api-deployer.delete-infra",
		],
		async () => {
			await apiDeployerDeploymentLifecycle(ctx,);
		},
		{ capability: "infrastructure.api-deployer-deployments", },
	);
	await ctx.check(
		"infrastructure.api-deployer.deploy" as LiveCaseId,
		[
			"api-deployer.create-deployment",
			"api-deployer.deployment-status",
			"api-deployer.deploy",
			"api-deployer.delete-deployment",
		],
		async () => {
			await apiDeployerDeploy(ctx,);
		},
		{ capability: "infrastructure.api-deployer-node", required: false, },
	);
	await ctx.check(
		"infrastructure.project-deployer.reads" as LiveCaseId,
		[
			"project-deployer.list-projects",
			"project-deployer.list-deployments",
			"project-deployer.list-infras",
		],
		async () => {
			await projectDeployerReads(ctx,);
		},
		{ capability: "infrastructure.project-deployer-reads", required: false, },
	);
	await ctx.check(
		"infrastructure.project-deployer.infra-lifecycle" as LiveCaseId,
		["project-deployer.create-infra", "project-deployer.list-infras",],
		async () => {
			await projectDeployerInfraLifecycle(ctx,);
		},
		{ capability: "infrastructure.project-deployer-infra", required: false, },
	);
	await ctx.check(
		"infrastructure.project-deployer" as LiveCaseId,
		[
			"project-deployer.create-project",
			"project-deployer.project-status",
			"project-deployer.list-projects",
		],
		async () => {
			await projectDeployerCreateProject(ctx,);
		},
		{ capability: "infrastructure.project-deployer-publish", required: false, },
	);
	await ctx.check(
		"infrastructure.project-deployer.upload-bundle" as LiveCaseId,
		[
			"bundle.export",
			"bundle.download-exported",
			"project-deployer.create-infra",
			"project-deployer.upload-bundle",
			"project-deployer.project-status",
			"project-deployer.list-projects",
		],
		async () => {
			await projectDeployerUploadBundle(ctx,);
		},
		{ capability: "infrastructure.project-deployer-publish", required: false, },
	);
	await ctx.check(
		"infrastructure.project-deployer.deployment-lifecycle" as LiveCaseId,
		[
			"project-deployer.upload-bundle",
			"project-deployer.project-status",
			"project-deployer.create-deployment",
			"project-deployer.get-deployment",
			"project-deployer.list-deployments",
			"project-deployer.save-deployment-settings",
			"project-deployer.delete-deployment",
		],
		async () => {
			await projectDeployerDeploymentLifecycle(ctx,);
		},
		{ capability: "infrastructure.project-deployer-deployments", required: false, },
	);
	await ctx.check(
		"infrastructure.project-deployer.deploy" as LiveCaseId,
		[
			"project-deployer.create-deployment",
			"project-deployer.deployment-status",
			"project-deployer.deploy",
			"project-deployer.delete-deployment",
		],
		async () => {
			await projectDeployerDeploy(ctx,);
		},
		{ capability: "infrastructure.project-deployer-node", required: false, },
	);
}
