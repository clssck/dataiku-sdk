import { describe, expect, it, } from "bun:test";
import { buildCommandRegistry, buildMutationPlan, } from "../../src/cli/contract.js";
import { RESOURCE_NAMES, } from "../../src/cli/usage.js";
import { dss, dssFailure, } from "./_harness.js";

type Schema = Record<string, unknown>;

describe("agent contract accuracy", () => {
	it("extracts enum alternatives including uppercase forms into flag metadata", () => {
		const registry = buildCommandRegistry();
		const flagByName = (resource: string, action: string, name: string,) =>
			registry[resource]?.[action]?.flags.find((flag,) => flag.name === name);

		expect(flagByName("recipe", "create", "fuzzy-distance",)?.enumValues,).toEqual([
			"DAMERAU_LEVENSHTEIN",
			"HAMMING",
			"JACCARD",
			"COSINE",
			"EUCLIDEAN",
		],);
		expect(flagByName("job", "build", "type",)?.enumValues,).toEqual([
			"DATASET",
			"MANAGED_FOLDER",
		],);
		expect(flagByName("ml-task", "create", "task-type",)?.enumValues,).toEqual([
			"PREDICTION",
			"CLUSTERING",
		],);
		// Lowercase alternatives keep working.
		expect(flagByName("job", "build", "target-type",)?.enumValues,).toEqual([
			"dataset",
			"managed-folder",
		],);
		expect(flagByName("recipe", "create", "join-type",)?.enumValues,).toEqual([
			"LEFT",
			"INNER",
			"RIGHT",
			"FULL",
		],);
		// Single-token placeholders are not enums.
		expect(flagByName("project", "export", "output",),).not.toHaveProperty("enumValues",);
	});

	it("classifies remote exports as local-file reads, not DSS mutations", () => {
		const registry = buildCommandRegistry();
		const exports = [registry.project?.export, registry.dashboard?.export,];
		for (const entry of exports) {
			expect(entry,).toBeDefined();
			expect(entry?.sideEffect,).toBe("read",);
			expect(entry?.mutatesDss,).toBe(false,);
			expect(entry?.destructive,).toBe("none",);
			expect(entry?.producesLocalFile,).toBe(true,);
			expect(entry?.idempotency,).toBe("safe",);
			expect(entry?.flags.some((flag,) => flag.name === "plan"),).toBe(false,);
			expect(entry?.optionalFlags,).not.toContain("plan",);
			expect(entry?.requiredFlags,).toContain("output",);
		}
		// Nearby real mutations keep their classification.
		expect(registry.project?.import?.mutatesDss,).toBe(true,);
		expect(registry.project?.import?.sideEffect,).toBe("write",);
		expect(registry.bundle?.export?.mutatesDss,).toBe(true,);
	});

	it("reports agent, version, and batch among valid resources", async () => {
		expect(RESOURCE_NAMES,).toEqual(expect.arrayContaining([
			"agent",
			"version",
			"batch",
			"project",
			"commands",
			"install-skill",
		],),);

		const failure = await dssFailure(["not-a-resource",],);
		expect(failure.code,).toBe(1,);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect((report.details as Record<string, unknown>).validResources,).toEqual(
			expect.arrayContaining(["agent", "version", "batch",],),
		);
	});

	it("rejects unknown scoped discovery resources and actions", async () => {
		const resourceFailure = await dssFailure([
			"commands",
			"run",
			"--fields",
			"not-a-resource",
		],);
		expect(resourceFailure.code,).toBe(1,);
		const resourceReport = JSON.parse(resourceFailure.stderr,) as Record<string, unknown>;
		expect(resourceReport.error,).toBe("Unknown resource: not-a-resource.",);
		expect((resourceReport.details as Record<string, unknown>).validResources,).toEqual(
			expect.arrayContaining(["agent", "version", "batch",],),
		);

		const actionFailure = await dssFailure([
			"commands",
			"run",
			"--fields",
			"dataset.not-an-action",
		],);
		expect(actionFailure.code,).toBe(1,);
		const actionReport = JSON.parse(actionFailure.stderr,) as Record<string, unknown>;
		expect(actionReport.error,).toBe("Unknown action: dataset not-an-action",);
		expect((actionReport.details as Record<string, unknown>).validActions,).toContain("create",);
	});

	it("rejects scoped discovery selectors with empty path components", async () => {
		for (const selector of ["dataset.", "dataset..usage", ".dataset",]) {
			const failure = await dssFailure(["commands", "run", "--fields", selector,],);
			expect(failure.code,).toBe(1,);
			const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
			expect(report.error,).toBe(
				`Invalid --fields selector: ${selector}. Expected RESOURCE or RESOURCE.ACTION[.FIELD...].`,
			);
			expect(report.details,).toEqual({ selector, },);
		}
	});

	it("rejects empty or comma-only --fields without emitting the registry", async () => {
		for (const value of ["", ",,", " , ",]) {
			const failure = await dssFailure(["commands", "run", "--fields", value, "--json",],);
			expect(failure.code,).toBe(1,);
			expect(failure.stdout,).toBe("",);
			const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				ok: false,
				error:
					"--fields requires at least one selector. Expected RESOURCE or RESOURCE.ACTION[.FIELD...].",
				code: "usage_error",
				category: "usage",
				exitCode: 1,
				hint:
					"Use `dss commands run --fields RESOURCE.ACTION --json` for scoped, compact command discovery; omit --fields only when you need the full registry.",
				details: { fields: value, },
				resource: "commands",
				action: "run",
			},);
		}
	});

	it("omitted --fields still returns the full registry", async () => {
		const { stdout, stderr, } = await dss(["commands", "run",],);
		expect(stderr,).toBe("",);
		const registry = JSON.parse(stdout,) as Record<string, unknown>;
		expect(Object.keys(registry,),).toEqual(
			expect.arrayContaining(["project", "recipe", "dataset", "commands",],),
		);
	});

	it("unknown-flag recovery points at scoped compact discovery", async () => {
		const failure = await dssFailure(["project", "list", "--name", "X",],);
		expect(failure.code,).toBe(1,);
		expect(failure.stdout,).toBe("",);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			error: "Unknown flag --name for project list",
			code: "unknown_flag",
			hint:
				"Use `dss commands run --fields project.list --json` to list the flags this command supports.",
		},);
	});

	it("preserves nested field projection after validating the scoped action", async () => {
		const { stdout, stderr, } = await dss([
			"commands",
			"run",
			"--fields",
			"dataset.update.payloadSchema",
		],);
		expect(stderr,).toBe("",);
		expect(JSON.parse(stdout,),).toEqual({
			"dataset.update.payloadSchema": {
				stdin: true,
				dataFlag: true,
				dataFileFlag: true,
				jsonShape: "object",
			},
		},);
	});

	it("emits argv schemas whose head pins resource/action and required positionals", async () => {
		const { stdout, stderr, } = await dss(
			[
				"commands",
				"run",
				"--fields",
				"project.export,recipe.create,insight.update,sql.query,project.delete",
			],
		);
		expect(stderr,).toBe("",);
		const scoped = JSON.parse(stdout,) as Record<string, Record<string, Schema>>;
		const argvSchemaOf = (key: string,) => scoped[key]?.schemas?.argv as Schema | undefined;

		const exportSchema = argvSchemaOf("project.export",);
		expect(exportSchema,).toBeDefined();
		expect(exportSchema?.$schema,).toBe("https://json-schema.org/draft/2020-12/schema",);
		// Original envelope is preserved: required argv, additive constraints.
		expect(exportSchema?.required,).toEqual(["argv",],);
		expect(exportSchema?.additionalProperties,).toBe(true,);
		expect(exportSchema?.properties?.resource,).toEqual({ const: "project", },);
		expect(exportSchema?.properties?.action,).toEqual({ const: "export", },);
		const exportArgv = exportSchema?.properties?.argv as Schema;
		expect(exportArgv?.minItems,).toBe(3,); // resource + action + <projectKey>
		expect(exportArgv?.prefixItems,).toEqual([
			{ const: "project", },
			{ const: "export", },
			{ type: "string", title: "projectKey", not: { pattern: "^-", }, },
		],);
		expect(exportArgv?.allOf,).toEqual([
			{ contains: { pattern: "^--(output)=[\\s\\S]+$", }, minContains: 1, },
		],);

		const createSchema = argvSchemaOf("recipe.create",);
		expect(createSchema,).toBeDefined();
		const createArgv = createSchema?.properties?.argv as Schema;
		expect(createArgv?.minItems,).toBe(2,); // recipe create has no positionals
		expect(createArgv?.prefixItems,).toEqual([
			{ const: "recipe", },
			{ const: "create", },
		],);
		expect(createArgv?.allOf,).toEqual([
			{ contains: { pattern: "^--(type)=[\\s\\S]+$", }, minContains: 1, },
			{ contains: { pattern: "^--(input)=[\\s\\S]+$", }, minContains: 1, },
			{
				anyOf: [
					{ contains: { pattern: "^--(output)=[\\s\\S]+$", }, minContains: 1, },
					{ contains: { pattern: "^--(output-folder)=[\\s\\S]+$", }, minContains: 1, },
				],
			},
		],);

		const insightArgv = argvSchemaOf("insight.update",)?.properties?.argv as Schema;
		expect(insightArgv?.allOf,).toContainEqual({
			anyOf: expect.arrayContaining([
				{ contains: { pattern: "^--(file)=[\\s\\S]+$", }, minContains: 1, },
				{ contains: { pattern: "^--(content-type)=[\\s\\S]+$", }, minContains: 1, },
			],),
		},);

		const deleteArgv = argvSchemaOf("project.delete",)?.properties?.argv as Schema;
		expect(deleteArgv?.minItems,).toBe(3,);
		expect(deleteArgv?.prefixItems,).toEqual([
			{ const: "project", },
			{ const: "delete", },
			{ type: "string", title: "projectKey", not: { pattern: "^-", }, },
		],);
		// Tail tokens: plain values, the `-` stdin marker, the `--` separator,
		// boolean flags standalone, value flags only in `--flag=VALUE` form, and
		// boolean short flags. Unknown flags match none of these branches and are
		// rejected.
		const exportItems = exportArgv?.items as Schema;
		expect(exportItems?.anyOf,).toEqual([
			{ not: { pattern: "^-", }, },
			{ const: "-", },
			{ pattern: "^--$", },
			{ pattern: "^--(stdin|json|verbose|insecure)$", },
			{
				pattern:
					"^--(output)=[\\s\\S]+$|^--(data)=[\\s\\S]+$|^--(data-file)=[\\s\\S]+$|^--(fields)=[\\s\\S]+$|^--(url)=[\\s\\S]+$|^--(api-key)=[\\s\\S]+$|^--(request-timeout)=[\\s\\S]+$|^--(retries)=[\\s\\S]+$|^--(ca-cert)=[\\s\\S]+$",
			},
			{ pattern: "^-(v)$", },
		],);
	});

	it("keeps independent update flags and required variable choices machine-readable", () => {
		const registry = buildCommandRegistry();
		expect(registry.insight?.update?.requiredOneOf,).toContainEqual({
			oneOf: expect.arrayContaining([["file",], ["content-type",],],),
		},);
		expect(registry.variable?.set?.requiredOneOf,).toEqual([
			{ oneOf: [["standard",], ["local",],], },
		],);
		expect(registry.variable?.set?.usage,).toContain(
			`(--standard '{"k":"v"}'|--local '{"k":"v"}')`,
		);
	});

	it("does not require a --sql flag when positional SQL is a valid alternative", () => {
		const registry = buildCommandRegistry();
		expect(registry.sql?.query?.requiredOneOf,).toEqual([
			{ oneOf: [["connection",], ["dataset",],], },
		],);
		expect(registry.sql?.query?.requiredFlags,).toEqual([],);

		const argv = registry.sql?.query?.schemas?.argv?.properties?.argv as Schema | undefined;
		expect(argv,).toBeDefined();
		// The usage spells the SQL input as a bare `SQL` placeholder (not <sql>),
		// so it has no prefix slot; the SQL-input requirement is handler-enforced.
		expect(argv?.minItems,).toBe(2,); // resource + action
		// Only the --connection/--dataset choice is required; --sql, --sql-file,
		// and --stdin must not appear in the flag contract.
		expect(argv?.allOf,).toEqual([
			{
				anyOf: [
					{ contains: { pattern: "^--(connection)=[\\s\\S]+$", }, minContains: 1, },
					{ contains: { pattern: "^--(dataset)=[\\s\\S]+$", }, minContains: 1, },
				],
			},
		],);
		expect(registry.sql?.query?.optionalFlags,).toEqual(expect.arrayContaining([
			"sql",
			"sql-file",
			"stdin",
		],),);
	});

	it("exposes scoped discovery guidance in the agent contract", async () => {
		const { stdout, stderr, } = await dss(["agent", "contract",],);
		expect(stderr,).toBe("",);
		const contract = JSON.parse(stdout,) as Record<string, Record<string, unknown>>;
		const commands = contract.commands as Record<string, unknown>;
		expect(commands.scopedDiscoveryCommand,).toBe(
			"dss commands run --fields RESOURCE[.ACTION[.FIELD...]] --json",
		);
		expect(commands.compactOutputFlag,).toBe("--json",);
		expect(commands.compactOutputHint,).toContain("reduce agent context usage",);
		expect(commands.scopedDiscoveryExamples,).toEqual(
			expect.arrayContaining(["dss commands run --fields dataset.create --json",],),
		);
		expect(commands.scopedDiscoveryHint,).toContain("append .FIELD paths",);
		expect(commands.discoveryCommand,).toBe("dss commands run",);
		expect(commands.actionIndexCommand,).toBe(
			"dss agent contract --fields commands.actions --json",
		);
	});

	it("advertises compact scoped bootstrap and per-action projections in meta metadata", async () => {
		const registry = buildCommandRegistry();
		const commandsRun = registry.commands?.run;
		const agentContract = registry.agent?.contract;
		expect(commandsRun?.usage,).toContain("[--fields PATHS]",);
		expect(commandsRun?.usage,).toContain("[--json]",);
		expect(commandsRun?.examples,).toContain(
			"dss commands run --fields dataset.create.usage,dataset.create.description,dataset.create.flags,dataset.create.examples --json",
		);
		expect(commandsRun?.description,).toContain("RESOURCE.ACTION.FIELD",);
		expect(agentContract?.usage,).toContain("[--fields PATHS]",);
		expect(agentContract?.examples,).toContain(
			"dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility --json",
		);
		expect(agentContract?.examples,).toContain(
			"dss agent contract --fields commands.actions --json",
		);
		expect(agentContract?.description,).toContain("commands.actions",);
		const { stdout, stderr, } = await dss(["agent", "contract", "--fields", "planning",],);
		expect(stderr,).toBe("",);
		const contract = JSON.parse(stdout,) as Record<string, Record<string, unknown>>;
		expect(contract.planning?.contractCommand,).toBe("dss agent contract",);
		expect(contract.planning?.bootstrapCommand,).toContain(
			"--fields protocol,agentContractVersion,cli,stdio,planning,compatibility --json",
		);
		expect(contract.planning?.preferredDiscoveryCommand,).toBe(
			"dss commands run --fields RESOURCE.ACTION --json",
		);
		expect(contract.planning?.actionIndexCommand,).toBe(
			"dss agent contract --fields commands.actions --json",
		);
	});

	it("projects the six-field bootstrap and four-field action metadata as compact JSON", async () => {
		const bootstrap = await dss([
			"agent",
			"contract",
			"--fields",
			"protocol,agentContractVersion,cli,stdio,planning,compatibility",
			"--json",
		],);
		expect(bootstrap.stderr,).toBe("",);
		const contract = JSON.parse(bootstrap.stdout,) as Record<string, unknown>;
		expect(Object.keys(contract,).sort(),).toEqual([
			"agentContractVersion",
			"cli",
			"compatibility",
			"planning",
			"protocol",
			"stdio",
		],);
		expect(contract.commands,).toBeUndefined();
		expect(contract.schemas,).toBeUndefined();

		const projection = await dss([
			"commands",
			"run",
			"--fields",
			"dataset.create.usage,dataset.create.description,dataset.create.flags,dataset.create.examples",
			"--json",
		],);
		expect(projection.stderr,).toBe("",);
		const entry = JSON.parse(projection.stdout,) as Record<string, unknown>;
		expect(Object.keys(entry,).sort(),).toEqual([
			"dataset.create.description",
			"dataset.create.examples",
			"dataset.create.flags",
			"dataset.create.usage",
		],);
	});

	it("wires version, successor, and verification metadata with local redacted plans", () => {
		const registry = buildCommandRegistry();
		const successor = registry.app?.["create-successor-instance"];
		expect(successor,).toBeDefined();
		expect(successor?.async,).toBe("future",);
		expect(successor?.sideEffect,).toBe("write",);
		expect(successor?.dryRun,).toBe(true,);
		expect(successor?.cleanupCommand,).toBeUndefined();
		expect(successor?.cleanupHint ?? "",).toContain("--record-cleanup",);
		expect(successor?.cleanupHint ?? "",).toContain("dss cleanup --file",);
		expect(successor?.cleanupHint ?? "",).toContain("--apply",);
		expect(successor?.cleanupHint ?? "",).not.toContain("dss app delete-instance",);
		expect(successor?.exitCodes,).toHaveProperty("longRunningFailure", 4,);
		expect(successor?.flags.some((flag,) => flag.name === "wait"),).toBe(false,);
		expect(
			successor?.flags.some((flag,) => flag.name === "copy-permissions" && flag.kind === "boolean"),
		).toBe(true,);
		expect(registry.app?.["manifest-version"]?.sideEffect,).toBe("read",);
		expect(registry.app?.["verify-instance"]?.sideEffect,).toBe("read",);
		expect(registry.app?.["set-manifest-version"]?.sideEffect,).toBe("write",);
		expect(registry.app?.["set-manifest-version"]?.idempotency,).toBe("none",);

		const successorPlan = buildMutationPlan(
			"app",
			"create-successor-instance",
			{
				usage:
					"dss app create-successor-instance <appId> --from KEY --to KEY [--name NAME] [--copy-permissions] [--timeout MS] [--poll-interval MS] [--dry-run] [--record-cleanup PATH]",
				handler: async () => undefined,
			},
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", },
		);
		expect(successorPlan,).toMatchObject({
			plan: true,
			method: "POST",
			endpoint: "/public/api/apps/MYAPP/instances",
			appId: "MYAPP",
			sourceProjectKey: "FROMKEY",
			targetProjectKey: "TOKEY",
			payload: { targetProjectKey: "TOKEY", targetProjectName: "TOKEY", },
			wait: true,
			async: "future",
			copyPermissions: false,
		},);
		expect(successorPlan.incarnationControl,).toBe(
			"client-side-non-atomic-future-target-and-creation-tag-join",
		);
		expect(successorPlan.postFutureRequests,).toEqual([
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/TOKEY/",
			},),
		],);
		expect(successorPlan,).not.toHaveProperty("permissionRequests",);
		expect(successorPlan,).toHaveProperty("note",);
		expect(JSON.stringify(successorPlan,),).not.toContain("api-key",);
		expect(JSON.stringify(successorPlan,),).not.toContain("apiKey",);

		const permissionPlan = buildMutationPlan(
			"app",
			"create-successor-instance",
			{
				usage:
					"dss app create-successor-instance <appId> --from KEY --to KEY [--name NAME] [--copy-permissions] [--timeout MS] [--poll-interval MS] [--dry-run] [--record-cleanup PATH]",
				handler: async () => undefined,
			},
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", "copy-permissions": true, },
		);
		expect(permissionPlan,).toMatchObject({
			plan: true,
			method: "POST",
			endpoint: "/public/api/apps/MYAPP/instances",
			copyPermissions: true,
			payload: { targetProjectKey: "TOKEY", targetProjectName: "TOKEY", },
			wait: true,
		},);
		expect(permissionPlan.permissionConcurrencyControl,).toBe(
			"client-side-non-atomic-stale-identity-and-hash-checks",
		);
		expect(
			(permissionPlan.permissionRequests as Array<Record<string, unknown>>).map(
				({ method, endpoint, },) => `${String(method,)} ${String(endpoint,)}`,
			),
		).toEqual([
			"GET /public/api/projects/FROMKEY/permissions",
			"GET /public/api/projects/TOKEY/permissions",
			"GET /public/api/projects/TOKEY/",
			"GET /public/api/projects/FROMKEY/permissions",
			"GET /public/api/projects/TOKEY/",
			"PUT /public/api/projects/TOKEY/permissions",
			"GET /public/api/projects/TOKEY/permissions",
			"GET /public/api/projects/TOKEY/",
		],);
		expect(permissionPlan.permissionRequests,).toHaveLength(8,);

		const versionPlan = buildMutationPlan(
			"app",
			"set-manifest-version",
			{
				usage:
					"dss app set-manifest-version (--manifest-version V|--version-notes NOTES) [--expect-hash SHA256] [--dry-run] [--project-key KEY]",
				handler: async () => undefined,
			},
			[],
			{ "project-key": "TEMPLATE", "manifest-version": "1.4.0", },
		);
		expect(versionPlan,).toMatchObject({
			plan: true,
			method: "PUT",
			endpoint: "/public/api/projects/TEMPLATE/app-manifest",
			projectKey: "TEMPLATE",
			payloadPatch: { version: "1.4.0", },
			concurrencyControl: "client-side-non-atomic-stale-read-check",
			staleReadCheck: "none",
		},);
		expect(JSON.stringify(versionPlan,).toLowerCase(),).not.toContain("optimistic",);

		const guardedPlan = buildMutationPlan(
			"app",
			"set-manifest-version",
			{
				usage:
					"dss app set-manifest-version (--manifest-version V|--version-notes NOTES) [--expect-hash SHA256] [--dry-run] [--project-key KEY]",
				handler: async () => undefined,
			},
			[],
			{
				"project-key": "TEMPLATE",
				"manifest-version": "1.4.0",
				"expect-hash": "A".repeat(64,),
			},
		);
		expect(guardedPlan,).toMatchObject({
			expectHash: "a".repeat(64,),
			concurrencyControl: "client-side-non-atomic-stale-read-check",
			staleReadCheck: "client-side-expect-hash-compare-before-put",
		},);
		expect(JSON.stringify(versionPlan,),).not.toContain("api-key",);
		expect(JSON.stringify(versionPlan,),).not.toContain("credentials",);
	});

	it("pins delete-instance plans to trimmed explicit keys and read-only future gates", () => {
		const meta = {
			usage:
				"dss app delete-instance --project-key KEY [--future-id ID] [--unconfirmed-creation] [--timeout MS] [--poll-interval MS]",
			handler: async () => undefined,
		};

		const barePlan = buildMutationPlan("app", "delete-instance", meta, [], {
			"project-key": "  INST  ",
		},);
		expect(barePlan,).toMatchObject({
			plan: true,
			method: "DELETE",
			endpoint: "/public/api/projects/INST",
			projectKey: "INST",
			idempotency: "convergent",
		},);
		expect(barePlan,).not.toHaveProperty("futureGate",);
		expect(barePlan.preflightRequests,).toEqual([
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/INST/app-manifest",
			},),
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/INST/",
			},),
		],);

		const incarnationHash = "0".repeat(64,);
		const boundDirectPlan = buildMutationPlan("app", "delete-instance", meta, [], {
			"project-key": "INST",
			"expect-project-incarnation": incarnationHash,
		},);
		expect(boundDirectPlan.incarnationControl,).toBe(
			"client-side-non-atomic-stale-identity-check",
		);
		expect(String(boundDirectPlan.note,),).toContain("unconditional DELETE",);
		const futurePlan = buildMutationPlan("app", "delete-instance", meta, [], {
			"project-key": "INST",
			"future-id": "  fut-1  ",
			"expect-project-incarnation": incarnationHash,
		},);
		expect(futurePlan.futureId,).toBe("fut-1",);
		expect(futurePlan.futureGate,).toHaveLength(1,);
		expect(futurePlan.futureGate,).toEqual([
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/futures/fut-1?peek=false",
			},),
		],);
		expect(JSON.stringify(futurePlan.futureGate,),).not.toContain("DELETE",);
		expect(JSON.stringify(futurePlan.futureGate,),).not.toContain("peek=true",);
		expect(String(futurePlan.note,),).toContain("non-atomic observations",);
		expect(futurePlan.preflightRequests,).toEqual([
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/INST/app-manifest",
				when: "before-future-wait",
			},),
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/INST/",
				when: "conditional-before-wait",
			},),
		],);
		expect(futurePlan.projectIncarnationGate,).toEqual({
			required: true,
			provided: true,
			expectedHash: incarnationHash,
		},);
		expect(futurePlan.incarnationControl,).toBe(
			"client-side-non-atomic-future-target-and-creation-tag-join",
		);
		expect(futurePlan.postFutureValidationRequests,).toEqual([
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/INST/app-manifest",
				when: "before-delete",
			},),
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/INST/",
				when: "incarnation-and-conditional-type-check-before-delete",
			},),
		],);

		const unconfirmedPlan = buildMutationPlan("app", "delete-instance", meta, [], {
			"project-key": "INST",
			"future-id": "fut-1",
			"unconfirmed-creation": "true",
		},);
		expect(unconfirmedPlan.unconfirmedCreation,).toBe(true,);
		expect(unconfirmedPlan,).not.toHaveProperty("method",);
		expect(unconfirmedPlan,).not.toHaveProperty("endpoint",);
		expect(unconfirmedPlan,).not.toHaveProperty("futureGate",);
		expect(unconfirmedPlan,).not.toHaveProperty("preflightRequests",);
		expect(unconfirmedPlan,).not.toHaveProperty("postFutureValidationRequests",);
		const previousAmbient = process.env.DATAIKU_PROJECT_KEY;
		process.env.DATAIKU_PROJECT_KEY = "AMBIENTKEY";
		try {
			expect(() => buildMutationPlan("app", "delete-instance", meta, [], {},)).toThrow(
				"--project-key is required",
			);
		} finally {
			if (previousAmbient === undefined) delete process.env.DATAIKU_PROJECT_KEY;
			else process.env.DATAIKU_PROJECT_KEY = previousAmbient;
		}
	});

	it("advertises batch exit 4 and convergent delete-instance metadata", () => {
		const registry = buildCommandRegistry();
		expect(registry.batch?.run?.async,).toBe("none",);
		expect(registry.batch?.run?.exitCodes,).toMatchObject({ longRunningFailure: 4, },);
		expect(registry.app?.["delete-instance"]?.idempotency,).toBe("convergent",);
		expect(
			registry.app?.["delete-instance"]?.flags.some((flag,) => flag.name === "if-exists"),
		).toBe(false,);
		expect(registry.app?.["delete-instance"]?.optionalFlags,).toContain("unconfirmed-creation",);
		expect(registry.app?.["delete-instance"]?.requiredFlags,).not.toContain("unconfirmed-creation",);
	});

	it("advertises strict absence preflight with normalized targets on create and successor plans", () => {
		const createMeta = {
			usage:
				"dss app create-instance <appId> --data JSON [--wait] [--timeout MS] [--poll-interval MS]",
			handler: async () => undefined,
		};
		const createPlan = buildMutationPlan("app", "create-instance", createMeta, ["MYAPP",], {
			data: JSON.stringify({ targetProjectKey: "  NEWPROJ  ", },),
		},);
		expect(createPlan.targetProjectKey,).toBe("NEWPROJ",);
		expect(createPlan.payload,).toEqual({ targetProjectKey: "NEWPROJ", },);
		expect(createPlan.preflightRequests,).toEqual([
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/NEWPROJ/",
				when: "before-create",
			},),
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/",
				when: "conditional",
			},),
		],);
		expect(
			() =>
				buildMutationPlan("app", "create-instance", createMeta, ["MYAPP",], {
					data: JSON.stringify({ targetProjectKey: "   ", },),
				},),
		).toThrow("non-empty targetProjectKey",);

		const successorMeta = {
			usage:
				"dss app create-successor-instance <appId> --from KEY --to KEY [--name NAME] [--copy-permissions] [--timeout MS] [--poll-interval MS] [--dry-run] [--record-cleanup PATH]",
			handler: async () => undefined,
		};
		const successorPlan = buildMutationPlan("app", "create-successor-instance", successorMeta, [
			"MYAPP",
		], { from: "FROMKEY", to: "TOKEY", },);
		expect(successorPlan.preflightRequests,).toEqual([
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/apps/MYAPP/instances/",
			},),
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/FROMKEY/app-manifest",
			},),
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/TOKEY/",
			},),
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/",
				when: "conditional",
			},),
		],);
		expect(successorPlan.payload,).toEqual({
			targetProjectKey: "TOKEY",
			targetProjectName: "TOKEY",
		},);
	});
});
