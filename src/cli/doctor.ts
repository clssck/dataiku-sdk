import { DataikuClient, } from "../client.js";
import { DataikuError, } from "../errors.js";
import { num, splitCsvFlag, stringField, } from "./coerce.js";
import { currentCommandContext, resolveCredentials, } from "./runtime.js";
import { UsageError, } from "./usage.js";

// ---------------------------------------------------------------------------
// Agent-facing diagnostics and introspection
// ---------------------------------------------------------------------------

export interface DoctorCheck {
	name: string;
	ok: boolean;
	message: string;
	details?: Record<string, unknown>;
}

export type PermissionStatus = "yes" | "no" | "unknown";

export type DoctorPermissionKey =
	| "canListProjects"
	| "canReadProject"
	| "canMutateProject"
	| "canCreateFolder"
	| "canRunJobs"
	| "canCreateScenario"
	| "canSaveJupyter"
	| "canMutateConnection";

export type DoctorPermissions = Record<DoctorPermissionKey, PermissionStatus>;

export interface DoctorFixtures {
	defaultDataset: string | null;
	defaultRecipe: string | null;
	defaultScenario: string | null;
	defaultFlowZone: string | null;
	defaultManagedFolder: string | null;
	defaultJupyterNotebook: string | null;
}

export interface FixtureReject {
	id?: string;
	name?: string;
	type?: string;
	reason: string;
}

export interface FixtureDiscoveryResult {
	projectKey: string;
	allowTypes: string[];
	fixtures: DoctorFixtures;
	safeDataset: Record<string, unknown> | null;
	safeManagedFolder: Record<string, unknown> | null;
	safeJupyterNotebook: Record<string, unknown> | null;
	unsafe: {
		datasets: FixtureReject[];
		managedFolders: FixtureReject[];
		jupyterNotebooks: FixtureReject[];
	};
}
export interface DoctorEnvironment {
	projectKey?: string;
	dssVersion?: string;
	instanceTime?: string;
	integrationFlags: {
		mutating: boolean;
		adminMutating: boolean;
		variables: boolean;
		sqlLive: boolean;
		bundles: boolean;
		apiServices: boolean;
	};
}

export interface DoctorResult {
	ok: boolean;
	checks: DoctorCheck[];
	context: {
		hasUrl: boolean;
		hasApiKey: boolean;
		projectKey?: string;
		tlsVerify: "strict" | "disabled";
		caCert: "default" | "custom";
	};
	permissions?: DoctorPermissions;
	permissionDetails?: Partial<Record<DoctorPermissionKey, Record<string, unknown>>>;
	fixtures?: DoctorFixtures;
	environment?: DoctorEnvironment;
}

export function errorDetails(error: unknown,): Record<string, unknown> {
	if (error instanceof DataikuError) {
		return {
			category: error.category,
			retryable: error.retryable,
			status: error.status,
			statusText: error.statusText,
		};
	}
	return { message: error instanceof Error ? error.message : String(error,), };
}

export function firstStringField(items: unknown[] | undefined, fields: string[],): string | null {
	for (const item of items ?? []) {
		if (item === null || typeof item !== "object" || Array.isArray(item,)) continue;
		const record = item as Record<string, unknown>;
		for (const field of fields) {
			const value = record[field];
			if (typeof value === "string" && value.trim().length > 0) return value;
		}
	}
	return null;
}

export function integrationFlag(name: string,): boolean {
	const value = process.env[name];
	return value === "1" || value?.toLowerCase() === "true";
}

export function doctorEnvironment(projectKey?: string,): DoctorEnvironment {
	return {
		...(projectKey ? { projectKey, } : {}),
		integrationFlags: {
			mutating: integrationFlag("RUN_DATAIKU_INTEGRATION_MUTATING",),
			adminMutating: integrationFlag("RUN_DATAIKU_ADMIN_MUTATING",),
			variables: integrationFlag("RUN_DATAIKU_INTEGRATION_VARIABLES",),
			sqlLive: integrationFlag("RUN_DATAIKU_SQL_LIVE",),
			bundles: integrationFlag("RUN_DATAIKU_INTEGRATION_BUNDLES",),
			apiServices: integrationFlag("RUN_DATAIKU_INTEGRATION_API_SERVICES",),
		},
	};
}

export function permissionStatusForError(error: unknown,): PermissionStatus {
	if (error instanceof DataikuError) {
		if (error.status === 401 || error.status === 403 || error.status === 404) return "no";
		if (error.status === 0 || error.status >= 500 || error.category === "transient") return "unknown";
		if (error.category === "forbidden" || error.category === "not_found") return "no";
		return "unknown";
	}
	return "unknown";
}

export async function probeDoctorPermission(
	probe: () => Promise<unknown>,
): Promise<{ status: PermissionStatus; details?: Record<string, unknown>; }> {
	try {
		await probe();
		return { status: "yes", };
	} catch (error) {
		return { status: permissionStatusForError(error,), details: errorDetails(error,), };
	}
}

export async function probeReadOnlyPrerequisiteForMutation(
	probe: () => Promise<unknown>,
	readAction: string,
): Promise<{ status: PermissionStatus; details?: Record<string, unknown>; }> {
	const readProbe = await probeDoctorPermission(probe,);
	if (readProbe.status !== "yes") return readProbe;
	return {
		status: "unknown",
		details: {
			reason: "mutation capability was not verified because doctor capabilities are read-only",
			readAction,
			readStatus: "yes",
		},
	};
}

export function missingProjectPermission(): {
	status: PermissionStatus;
	details: Record<string, unknown>;
} {
	return {
		status: "unknown",
		details: { reason: "projectKey is required for this probe", },
	};
}

export function recordsFromUnknownArray(items: unknown[],): Array<Record<string, unknown>> {
	return items.filter((item,) =>
		item !== null && typeof item === "object" && !Array.isArray(item,)
	) as Array<
		Record<string, unknown>
	>;
}

export function doctorFixturesFromLists(
	datasets: unknown[],
	recipes: unknown[],
	scenarios: unknown[],
	flowZones: unknown[],
	folders: unknown[],
	jupyterNotebooks: unknown[],
): DoctorFixtures {
	return {
		defaultDataset: firstStringField(datasets, ["name",],),
		defaultRecipe: firstStringField(recipes, ["name",],),
		defaultScenario: firstStringField(scenarios, ["id",],),
		defaultFlowZone: firstStringField(flowZones, ["id",],),
		defaultManagedFolder: firstStringField(folders, ["id",],),
		defaultJupyterNotebook: firstStringField(jupyterNotebooks, ["name",],),
	};
}

export async function discoverDoctorFixtures(
	client: DataikuClient,
	projectKey: string,
): Promise<DoctorFixtures> {
	const [
		datasets,
		recipes,
		scenarios,
		flowZones,
		folders,
		jupyterNotebooks,
	] = await Promise.all([
		client.datasets.list(projectKey,),
		client.recipes.list(projectKey,),
		client.scenarios.list(projectKey,),
		client.flowZones.list(projectKey,),
		client.folders.list(projectKey,),
		client.notebooks.listJupyter(projectKey,),
	],);
	return doctorFixturesFromLists(
		datasets,
		recipes,
		scenarios,
		flowZones,
		folders,
		jupyterNotebooks,
	);
}

export const DEFAULT_FIXTURE_ALLOW_TYPES = ["Filesystem", "Inline",];

export function fixtureAllowTypes(flags: Record<string, string | boolean>,): string[] {
	const configured = splitCsvFlag(flags["allow-types"],);
	return configured.length > 0 ? configured : DEFAULT_FIXTURE_ALLOW_TYPES;
}

export function isAllowedFixtureType(type: string | undefined, allowTypes: string[],): boolean {
	if (!type) return false;
	const normalized = type.trim().toLowerCase();
	return allowTypes.some((allowed,) => allowed.trim().toLowerCase() === normalized);
}

export function fixtureReject(record: Record<string, unknown>, reason: string,): FixtureReject {
	return {
		...(stringField(record, ["id",],) ? { id: stringField(record, ["id",],), } : {}),
		...(stringField(record, ["name",],) ? { name: stringField(record, ["name",],), } : {}),
		...(stringField(record, ["type",],) ? { type: stringField(record, ["type",],), } : {}),
		reason,
	};
}

export function firstSafeTypedFixture(
	items: unknown[],
	allowTypes: string[],
): { safe: Record<string, unknown> | null; unsafe: FixtureReject[]; } {
	const unsafe: FixtureReject[] = [];
	for (const record of recordsFromUnknownArray(items,)) {
		const type = stringField(record, ["type",],);
		if (isAllowedFixtureType(type, allowTypes,)) return { safe: record, unsafe, };
		unsafe.push(fixtureReject(record, `type=${type ?? "missing"}`,),);
	}
	return { safe: null, unsafe, };
}

export function firstSafeJupyterNotebook(
	items: unknown[],
): { safe: Record<string, unknown> | null; unsafe: FixtureReject[]; } {
	const unsafe: FixtureReject[] = [];
	for (const record of recordsFromUnknownArray(items,)) {
		const name = stringField(record, ["name",],);
		if (name && !name.startsWith("_",)) return { safe: record, unsafe, };
		unsafe.push(fixtureReject(record, name ? "name starts with _" : "missing name",),);
	}
	return { safe: null, unsafe, };
}

export async function discoverFixtureReport(
	client: DataikuClient,
	projectKey: string,
	flags: Record<string, string | boolean>,
): Promise<FixtureDiscoveryResult> {
	const allowTypes = fixtureAllowTypes(flags,);
	const [
		datasets,
		recipes,
		scenarios,
		flowZones,
		folders,
		jupyterNotebooks,
	] = await Promise.all([
		client.datasets.list(projectKey,),
		client.recipes.list(projectKey,),
		client.scenarios.list(projectKey,),
		client.flowZones.list(projectKey,),
		client.folders.list(projectKey,),
		client.notebooks.listJupyter(projectKey,),
	],);
	const dataset = firstSafeTypedFixture(datasets, allowTypes,);
	const folder = firstSafeTypedFixture(folders, allowTypes,);
	const notebook = firstSafeJupyterNotebook(jupyterNotebooks,);
	return {
		projectKey,
		allowTypes,
		fixtures: doctorFixturesFromLists(
			datasets,
			recipes,
			scenarios,
			flowZones,
			folders,
			jupyterNotebooks,
		),
		safeDataset: dataset.safe,
		safeManagedFolder: folder.safe,
		safeJupyterNotebook: notebook.safe,
		unsafe: {
			datasets: dataset.unsafe,
			managedFolders: folder.unsafe,
			jupyterNotebooks: notebook.unsafe,
		},
	};
}

export async function doctorCapabilities(
	client: DataikuClient,
	projectKey: string | undefined,
	accessibleProjects: unknown[] | undefined,
	flags: Record<string, string | boolean>,
): Promise<Pick<DoctorResult, "permissions" | "permissionDetails" | "fixtures" | "environment">> {
	const probeProjectKey = projectKey ?? firstStringField(accessibleProjects, ["projectKey",],)
		?? undefined;
	const probes: Record<
		DoctorPermissionKey,
		() => Promise<{ status: PermissionStatus; details?: Record<string, unknown>; }>
	> = {
		canListProjects: () =>
			probeDoctorPermission(async () => accessibleProjects ?? await client.projects.list()),
		canReadProject: () =>
			probeProjectKey
				? probeDoctorPermission(() => client.projects.get(probeProjectKey,))
				: Promise.resolve(missingProjectPermission(),),
		canMutateProject: () =>
			probeProjectKey
				? probeReadOnlyPrerequisiteForMutation(
					() => client.variables.get(probeProjectKey,),
					"variables.get",
				)
				: Promise.resolve(missingProjectPermission(),),
		canCreateFolder: () =>
			probeProjectKey
				? probeReadOnlyPrerequisiteForMutation(
					() => client.folders.list(probeProjectKey,),
					"folders.list",
				)
				: Promise.resolve(missingProjectPermission(),),
		canRunJobs: () =>
			probeProjectKey
				? probeReadOnlyPrerequisiteForMutation(
					() => client.jobs.list(probeProjectKey,),
					"jobs.list",
				)
				: Promise.resolve(missingProjectPermission(),),
		canCreateScenario: () =>
			probeProjectKey
				? probeReadOnlyPrerequisiteForMutation(
					() => client.scenarios.list(probeProjectKey,),
					"scenarios.list",
				)
				: Promise.resolve(missingProjectPermission(),),
		canSaveJupyter: () =>
			probeProjectKey
				? probeReadOnlyPrerequisiteForMutation(
					() => client.notebooks.listJupyter(probeProjectKey,),
					"notebooks.listJupyter",
				)
				: Promise.resolve(missingProjectPermission(),),
		canMutateConnection: () =>
			probeReadOnlyPrerequisiteForMutation(() => client.connections.list(), "connections.list",),
	};
	const permissions = {} as DoctorPermissions;
	const permissionDetails: Partial<Record<DoctorPermissionKey, Record<string, unknown>>> = {};
	for (const key of Object.keys(probes,) as DoctorPermissionKey[]) {
		const probe = await probes[key]();
		permissions[key] = probe.status;
		if (probe.details) permissionDetails[key] = probe.details;
	}

	const capabilityResult: Pick<
		DoctorResult,
		"permissions" | "permissionDetails" | "fixtures" | "environment"
	> = {
		permissions,
		...(Object.keys(permissionDetails,).length > 0 ? { permissionDetails, } : {}),
		environment: doctorEnvironment(projectKey,),
	};

	if (flags["fast"] !== true && probeProjectKey) {
		try {
			capabilityResult.fixtures = await discoverDoctorFixtures(client, probeProjectKey,);
		} catch (error) {
			capabilityResult.fixtures = {
				defaultDataset: null,
				defaultRecipe: null,
				defaultScenario: null,
				defaultFlowZone: null,
				defaultManagedFolder: null,
				defaultJupyterNotebook: null,
			};
			capabilityResult.permissionDetails = {
				...capabilityResult.permissionDetails,
				canReadProject: {
					...capabilityResult.permissionDetails?.canReadProject,
					fixtureDiscovery: errorDetails(error,),
				},
			};
		}
	}

	return capabilityResult;
}

export async function runDoctor(flags: Record<string, string | boolean>,): Promise<{
	result: DoctorResult;
	exitCode: number;
}> {
	const { url, apiKey, projectKey, tlsRejectUnauthorized, caCertPath, } = resolveCredentials(flags,);
	const checks: DoctorCheck[] = [];
	const context: DoctorResult["context"] = {
		hasUrl: url.trim().length > 0,
		hasApiKey: apiKey.trim().length > 0,
		...(projectKey ? { projectKey, } : {}),
		tlsVerify: tlsRejectUnauthorized === false ? "disabled" : "strict",
		caCert: caCertPath ? "custom" : "default",
	};

	const credentialsOk = context.hasUrl && context.hasApiKey;
	checks.push({
		name: "credentials_present",
		ok: credentialsOk,
		message: credentialsOk
			? "Dataiku URL and API key are configured."
			: "Missing Dataiku URL and/or API key. Set DATAIKU_URL/DATAIKU_API_KEY or pass --url/--api-key.",
	},);

	let accessibleProjects: unknown[] | undefined;

	if (credentialsOk) {
		const requestTimeoutMs = num(flags["request-timeout"],);
		const retryMaxAttempts = num(flags["retries"],);
		const client = new DataikuClient({
			url,
			apiKey,
			projectKey,
			verbose: flags["verbose"] === true,
			requestTimeoutMs,
			retryMaxAttempts,
			tlsRejectUnauthorized,
			caCertPath,
		},);

		try {
			const projects = await client.projects.list();
			accessibleProjects = projects;
			checks.push({
				name: "connectivity",
				ok: true,
				message: "Connected to DSS and listed accessible projects.",
				details: { projectCount: projects.length, },
			},);
		} catch (error) {
			checks.push({
				name: "connectivity",
				ok: false,
				message: "Could not list accessible projects.",
				details: errorDetails(error,),
			},);
		}

		if (projectKey) {
			try {
				const project = await client.projects.get(projectKey,);
				checks.push({
					name: "default_project",
					ok: true,
					message: `Project ${projectKey} is accessible.`,
					details: {
						projectKey,
						name: typeof project.name === "string" ? project.name : undefined,
					},
				},);
			} catch (error) {
				checks.push({
					name: "default_project",
					ok: false,
					message: `Project ${projectKey} is not accessible.`,
					details: errorDetails(error,),
				},);
			}
		}
	}

	const result: DoctorResult = { ok: checks.every((check,) => check.ok), checks, context, };
	if (flags["capabilities"] === true && credentialsOk) {
		const requestTimeoutMs = num(flags["request-timeout"],);
		const retryMaxAttempts = num(flags["retries"],) ?? 1;
		const client = new DataikuClient({
			url,
			apiKey,
			projectKey,
			verbose: flags["verbose"] === true,
			requestTimeoutMs,
			retryMaxAttempts,
			tlsRejectUnauthorized,
			caCertPath,
		},);
		result.environment = doctorEnvironment(projectKey,);
		Object.assign(result, await doctorCapabilities(client, projectKey, accessibleProjects, flags,),);
	}
	return { result, exitCode: result.ok ? 0 : 2, };
}

export async function runFixtures(
	flags: Record<string, string | boolean>,
): Promise<FixtureDiscoveryResult> {
	const { url, apiKey, projectKey, tlsRejectUnauthorized, caCertPath, } = resolveCredentials(flags,);
	if (!url) {
		throw new UsageError(
			"Missing Dataiku URL. Set DATAIKU_URL or pass --url.",
			"missing_required_flag",
		);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key. Set DATAIKU_API_KEY or pass --api-key.",
			"missing_required_flag",
		);
	}
	if (!projectKey) {
		throw new UsageError(
			"Missing project key. Set DATAIKU_PROJECT_KEY or pass --project-key.",
			"missing_required_flag",
		);
	}

	currentCommandContext.projectKey = projectKey;
	const requestTimeoutMs = num(flags["request-timeout"],);
	const retryMaxAttempts = num(flags["retries"],) ?? 1;
	const client = new DataikuClient({
		url,
		apiKey,
		projectKey,
		verbose: flags["verbose"] === true,
		requestTimeoutMs,
		retryMaxAttempts,
		tlsRejectUnauthorized,
		caCertPath,
	},);
	return discoverFixtureReport(client, projectKey, flags,);
}
