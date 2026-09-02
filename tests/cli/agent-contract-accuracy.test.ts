import { describe, expect, it, } from "bun:test";
import { commands, } from "../../src/cli/commands/index.js";
import {
	buildAgentContract,
	buildCommandRegistry,
	buildMutationPlan,
} from "../../src/cli/contract.js";
import { RESOURCE_NAMES, } from "../../src/cli/usage.js";
import {
	dss,
	dssFailure,
	join,
	mkdirSync,
	readFileSync,
	rmSync,
	tmpdir,
	writeFileSync,
} from "./_harness.js";

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

	it("classifies opaque SQL and ML training as planned mutations", () => {
		const registry = buildCommandRegistry();
		expect(registry.sql?.query,).toMatchObject({
			sideEffect: "write",
			mutatesDss: true,
			destructive: "destructive",
			idempotency: "none",
			requiresProject: false,
		},);
		expect(registry.mlTask?.train ?? registry["ml-task"]?.train,).toMatchObject({
			sideEffect: "write",
			mutatesDss: true,
			async: "job",
			idempotency: "none",
		},);
		expect(registry["install-skill"]?.run?.idempotency,).toBe("convergent",);

		const sqlPlan = buildMutationPlan(
			"sql",
			"query",
			commands.sql!.query!,
			["DROP TABLE obsolete",],
			{ connection: "WAREHOUSE", },
		);
		expect(sqlPlan,).toMatchObject({
			plan: true,
			method: "POST",
			endpoint: "/public/api/sql/queries/",
			connection: "WAREHOUSE",
			payload: { query: "DROP TABLE obsolete", connection: "WAREHOUSE", type: "sql", },
			idempotency: "none",
		},);
		const trainPlan = buildMutationPlan(
			"ml-task",
			"train",
			commands["ml-task"]!.train!,
			["analysis", "task",],
			{ "project-key": "PROJECT", "session-name": "baseline", wait: true, },
		);
		expect(trainPlan,).toMatchObject({
			plan: true,
			method: "POST",
			endpoint: "/public/api/projects/PROJECT/models/lab/analysis/task/train",
			analysisId: "analysis",
			mlTaskId: "task",
			payload: { sessionName: "baseline", runQueue: false, },
			wait: true,
			async: "job",
			idempotency: "none",
		},);
	});

	it("exposes complete positional, value, and output schemas", () => {
		const registry = buildCommandRegistry();
		expect(registry.sql?.query?.positionalArguments,).toContainEqual({
			name: "SQL",
			required: false,
		},);
		expect(registry.sql?.query?.requiredInputGroups,).toEqual([
			{
				oneOf: [
					{ positionals: ["SQL",], },
					{ flags: ["sql",], },
					{ flags: ["sql-file",], },
					{ flags: ["stdin",], },
				],
			},
			{ oneOf: [{ flags: ["connection",], }, { flags: ["dataset",], },], },
		],);
		expect(registry["flow-zone"]?.move?.positionalArguments,).toContainEqual({
			name: "id",
			required: false,
		},);
		expect(registry["flow-zone"]?.move?.requiredInputGroups,).toHaveLength(2,);

		for (const actions of Object.values(registry,)) {
			for (const entry of Object.values(actions,)) {
				for (const flag of entry.flags.filter((candidate,) => candidate.kind === "value")) {
					expect(flag.valueType, `${entry.resource}.${entry.action} --${flag.name}`,).toEqual(
						expect.any(String,),
					);
				}
			}
		}

		expect(registry.project?.list?.schemas.output,).toMatchObject({
			type: "array",
			items: { type: "object", required: ["projectKey", "name",], },
		},);
		expect(registry.dataset?.get?.schemas.output,).toMatchObject({
			type: "object",
			required: ["name",],
		},);
		expect(registry["flow-zone"]?.get?.schemas.output,).toMatchObject({
			type: "object",
			required: ["id", "name",],
		},);
	});

	it("publishes an error schema that accepts every emitted field", () => {
		const contract = buildAgentContract();
		const schema = (contract.schemas as Record<string, Schema>).errorEnvelope;
		const properties = schema.properties as Record<string, Schema>;
		expect(properties.category.enum,).toEqual([
			"usage",
			"permission_or_environment",
			"dss",
			"internal",
		],);
		for (const field of ["hint", "status", "retryable", "requestId", "details",]) {
			expect(properties,).toHaveProperty(field,);
		}
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
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
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
		expect(resourceFailure.stderr,).toBe("",);
		const resourceReport = JSON.parse(resourceFailure.stdout,) as Record<string, unknown>;
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
		expect(actionFailure.stderr,).toBe("",);
		const actionReport = JSON.parse(actionFailure.stdout,) as Record<string, unknown>;
		expect(actionReport.error,).toBe("Unknown action: dataset not-an-action",);
		expect((actionReport.details as Record<string, unknown>).validActions,).toContain("create",);
	});

	it("rejects scoped discovery selectors with empty path components", async () => {
		for (const selector of ["dataset.", "dataset..usage", ".dataset",]) {
			const failure = await dssFailure(["commands", "run", "--fields", selector,],);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report.error,).toBe(
				`Invalid --fields selector: ${selector}. Expected RESOURCE or RESOURCE.ACTION[.FIELD...].`,
			);
			expect(report.details,).toEqual({ selector, },);
		}
	});

	it("rejects empty or comma-only --fields without emitting the registry", async () => {
		for (const value of ["", ",,", " , ",]) {
			const failure = await dssFailure(["commands", "run", "--fields", value,],);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				ok: false,
				error:
					"--fields requires at least one selector. Expected RESOURCE or RESOURCE.ACTION[.FIELD...].",
				code: "usage_error",
				category: "usage",
				exitCode: 1,
				hint:
					"Use `dss commands run --fields RESOURCE.ACTION\` for scoped command discovery; use `dss commands run\` for the action summary or `--output PATH\` to export the full registry.",
				details: { fields: value, },
				resource: "commands",
				action: "run",
			},);
		}
	});

	it("omitted --fields returns a compact per-resource action summary", async () => {
		const { stdout, stderr, } = await dss(["commands", "run",],);
		expect(stderr,).toBe("",);
		const summary = JSON.parse(stdout,) as Record<string, string[]>;
		expect(Object.keys(summary,),).toEqual(
			expect.arrayContaining(["project", "recipe", "dataset", "commands",],),
		);
		for (const [resource, actions,] of Object.entries(summary,)) {
			expect(Array.isArray(actions,), `${resource} must map to an action list`,).toBe(true,);
			expect(actions.length, `${resource} must expose at least one action`,).toBeGreaterThan(0,);
			expect(actions.every((action,) => typeof action === "string"),).toBe(true,);
		}
		// The heavy per-action metadata must never travel over the default path.
		expect(stdout,).not.toContain('"usage"',);
		expect(stdout,).not.toContain('"description"',);
		expect(stdout,).not.toContain('"flags"',);
	});

	it("exports the full registry to --output and prints only the path", async () => {
		const exportDir = join(tmpdir(), `dss-contract-registry-${Date.now()}`,);
		mkdirSync(exportDir, { recursive: true, },);
		const exportPath = join(exportDir, "registry.json",);
		try {
			const { stdout, stderr, } = await dss(["commands", "run", "--output", exportPath,],);
			expect(stderr,).toBe("",);
			// Compact envelope on stdout: only the file path, never the registry.
			expect(JSON.parse(stdout,),).toEqual({ path: exportPath, },);
			expect(stdout.trim(),).not.toContain("\n",);

			const registry = JSON.parse(readFileSync(exportPath, "utf-8",),) as Record<
				string,
				Record<string, unknown>
			>;
			expect(Object.keys(registry,),).toEqual(
				expect.arrayContaining(["project", "recipe", "dataset", "commands",],),
			);
			const datasetCreate = registry.dataset?.create as Record<string, unknown>;
			expect(datasetCreate,).toMatchObject({
				usage: expect.any(String,),
				description: expect.any(String,),
				examples: expect.any(Array,),
				flags: expect.any(Array,),
			},);
		} finally {
			rmSync(exportDir, { recursive: true, force: true, },);
		}
	});

	it("unknown-flag recovery points at scoped compact discovery", async () => {
		const failure = await dssFailure(["project", "list", "--name", "X",],);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			error: "Unknown flag --name for project list",
			code: "unknown_flag",
			hint: "Use `dss commands run --fields project.list\` to list the flags this command supports.",
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
			{ pattern: "^--(stdin|verbose|insecure)$", },
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
			"dss commands run --fields RESOURCE[.ACTION[.FIELD...]]",
		);
		expect(commands,).not.toHaveProperty("compactOutputFlag",);
		expect(commands,).not.toHaveProperty("compactOutputHint",);
		expect(commands.scopedDiscoveryExamples,).toEqual(
			expect.arrayContaining([
				"dss commands run --fields dataset",
				"dss commands run --fields dataset.create",
			],),
		);
		expect(commands.scopedDiscoveryHint,).toContain("append .FIELD paths",);
		expect(commands.fullRegistryExportCommand,).toBe("dss commands run --output PATH",);
		expect(commands.discoveryCommand,).toBe("dss commands run",);
		expect(commands.actionIndexCommand,).toBe(
			"dss agent contract --fields commands.actions",
		);
	});

	it("advertises compact scoped bootstrap and per-action projections in meta metadata", async () => {
		const registry = buildCommandRegistry();
		const commandsRun = registry.commands?.run;
		const agentContract = registry.agent?.contract;
		expect(commandsRun?.usage,).toContain("[--fields PATHS]",);
		expect(commandsRun?.usage,).not.toContain("[--json]",);
		expect(commandsRun?.examples,).toContain(
			"dss commands run --fields dataset.create.usage,dataset.create.description,dataset.create.flags,dataset.create.examples",
		);
		expect(commandsRun?.description,).toContain("--output PATH",);
		expect(agentContract?.usage,).toContain("[--fields PATHS]",);
		expect(agentContract?.examples,).toContain(
			"dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility",
		);
		expect(agentContract?.examples,).toContain(
			"dss agent contract --fields commands.actions",
		);
		expect(agentContract?.description,).toContain("commands.actions",);
		const { stdout, stderr, } = await dss(["agent", "contract", "--fields", "planning",],);
		expect(stderr,).toBe("",);
		const contract = JSON.parse(stdout,) as Record<string, Record<string, unknown>>;
		expect(contract.planning?.contractCommand,).toBe("dss agent contract",);
		expect(contract.planning?.bootstrapCommand,).toContain(
			"--fields protocol,agentContractVersion,cli,stdio,planning,compatibility",
		);
		expect(contract.planning?.preferredDiscoveryCommand,).toBe(
			"dss commands run --fields RESOURCE.ACTION",
		);
		expect(contract.planning?.actionIndexCommand,).toBe(
			"dss agent contract --fields commands.actions",
		);
	});

	it("projects the six-field bootstrap and four-field action metadata as compact JSON", async () => {
		const bootstrap = await dss([
			"agent",
			"contract",
			"--fields",
			"protocol,agentContractVersion,cli,stdio,planning,compatibility",
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
		expect(createPlan.preflightExecuted,).toBe(false,);
		expect(createPlan.preflightWillRunDuringApply,).toBe(true,);
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
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/apps/MYAPP/instances/",
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
		expect(successorPlan.preflightExecuted,).toBe(false,);
		expect(successorPlan.preflightWillRunDuringApply,).toBe(true,);
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

describe("agent contract accuracy: coding plans", () => {
	it("never guesses endpoints for uncovered mutations", () => {
		const streaming = commands["streaming-endpoint"]!;
		const plan = buildMutationPlan(
			"streaming-endpoint",
			"create",
			streaming.create!,
			["stream", "kafka",],
			{ "project-key": "TEST", },
		);
		expect(plan,).toMatchObject({
			plan: true,
			resource: "streaming-endpoint",
			action: "create",
			exact: false,
		},);
		expect(plan.reason,).toContain("no endpoint was guessed",);
		expect(plan.endpoint,).toBeUndefined();
		expect(plan.method,).toBeUndefined();
	});

	it("describes code run lifecycle without exposing Python source", () => {
		const dir = join(tmpdir(), `dss-code-plan-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const sourcePath = join(dir, "script.py",);
		writeFileSync(sourcePath, "print('contract_secret_marker')\n",);
		try {
			const plan = buildMutationPlan("code", "run", commands.code!.run!, [], {
				file: sourcePath,
				"project-key": "TEST",
			},);
			expect(plan,).toMatchObject({
				endpoint: "/public/api/projects/TEST/scenarios/",
				method: "POST",
				source: { kind: "file", path: sourcePath, },
			},);
			expect(plan.sourceSha256,).toMatch(/^[a-f0-9]{64}$/,);
			expect(plan.requests,).toHaveLength(6,);
			expect(JSON.stringify(plan,),).not.toContain("contract_secret_marker",);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("marks recipe output resolution as unavailable offline", () => {
		const plan = buildMutationPlan("recipe", "run", commands.recipe!.run!, ["prepare",], {
			"project-key": "TEST",
		},);
		expect(plan,).toMatchObject({
			exact: false,
			method: "POST",
			endpoint: "/public/api/projects/TEST/jobs/",
			recipe: "prepare",
		},);
		expect(plan.payload,).toBeUndefined();
		expect(plan.reason,).toContain("recipe run --dry-run",);
	});

	it("publishes raw-text stdin for code run", () => {
		const codeRun = buildCommandRegistry().code!.run!;
		expect(codeRun.payloadSchema,).toEqual({ stdin: true, contentType: "text/plain", },);
		expect(codeRun.schemas.input,).toEqual({ type: "string", contentMediaType: "text/plain", },);
		expect(codeRun.schemas.output,).toMatchObject({
			type: "object",
			additionalProperties: false,
			required: expect.arrayContaining(["outcome", "success", "cleanup",],),
		},);
	});

	it("guards project-library paths and content with hashes", () => {
		expect(() =>
			buildMutationPlan(
				"project-library",
				"create-file",
				commands["project-library"]!["create-file"]!,
				[
					"../escape.py",
				],
				{ "project-key": "TEST", },
			)
		).toThrow();

		const expectSha256 = "a".repeat(64,);
		const plan = buildMutationPlan("project-library", "put", commands["project-library"]!.put!, [
			"python/lib.py",
		], {
			content: "library_secret_marker",
			"expect-sha256": expectSha256,
			"project-key": "TEST",
		},);
		expect(plan,).toMatchObject({
			method: "POST",
			endpoint: "/public/api/projects/TEST/libraries/contents/python/lib.py",
			payload: {
				contentSource: "flag",
				bytes: 21,
				expectSha256,
			},
		},);
		expect((plan.payload as Record<string, unknown>).sha256,).toMatch(/^[a-f0-9]{64}$/,);
		expect(plan.requests,).toHaveLength(2,);
		expect(JSON.stringify(plan,),).not.toContain("library_secret_marker",);
	});

	it("uses documented notebook output and composed unload requests", () => {
		const clear = buildMutationPlan(
			"notebook",
			"clear-jupyter-outputs",
			commands.notebook!["clear-jupyter-outputs"]!,
			["analysis.ipynb",],
			{ "project-key": "TEST", },
		);
		expect(clear,).toMatchObject({
			method: "DELETE",
			endpoint: "/public/api/projects/TEST/jupyter-notebooks/analysis.ipynb/outputs",
		},);

		const unloadAll = buildMutationPlan(
			"notebook",
			"unload-jupyter",
			commands.notebook!["unload-jupyter"]!,
			[],
			{ all: true, "project-key": "TEST", },
		);
		expect(unloadAll,).toMatchObject({ exact: false, all: true, },);
		expect(unloadAll.endpoint,).toBeUndefined();
		expect(unloadAll.requests,).toEqual([
			expect.objectContaining({
				method: "GET",
				endpoint: "/public/api/projects/TEST/jupyter-notebooks/?active=true",
			},),
			expect.objectContaining({ method: "GET", forEach: "active notebook", },),
			expect.objectContaining({ method: "DELETE", forEach: "listed session", },),
		],);
	});

	it("describes code-env entity, merge, image, and delete requests", () => {
		const create = buildMutationPlan("code-env", "create", commands["code-env"]!.create!, [
			"PYTHON",
			"audit_env",
		], { "deployment-mode": "DESIGN_MANAGED", },);
		expect(create,).toMatchObject({
			method: "POST",
			endpoint: "/public/api/admin/code-envs/PYTHON/audit_env?wait=true",
			payload: { deploymentMode: "DESIGN_MANAGED", },
		},);

		const expectHash = "c".repeat(64,);
		const definition = buildMutationPlan(
			"code-env",
			"set-definition",
			commands["code-env"]!["set-definition"]!,
			["PYTHON", "audit_env",],
			{
				data: '{"desc":{"secret":"definition_secret_marker"}}',
				"expect-hash": expectHash,
			},
		);
		expect(definition,).toMatchObject({
			exact: false,
			method: "PUT",
			endpoint: "/public/api/admin/code-envs/PYTHON/audit_env",
			lang: "PYTHON",
			name: "audit_env",
			expectHash,
			payload: "<omitted>",
		},);
		expect(definition.reason,).toContain("omits that body",);
		expect(definition.definitionHash,).toMatch(/^[a-f0-9]{64}$/,);
		expect(definition.requests,).toEqual([
			{ method: "GET", endpoint: definition.endpoint, purpose: "verify expectHash", },
			{
				method: "PUT",
				endpoint: definition.endpoint,
				condition: "hash matched",
				payload: "<omitted>",
				redactedFields: ["payload",],
			},
		],);
		expect(JSON.stringify(definition,),).not.toContain("definition_secret_marker",);

		const packages = buildMutationPlan(
			"code-env",
			"set-packages",
			commands["code-env"]!["set-packages"]!,
			[
				"PYTHON",
				"audit_env",
			],
			{ packages: "pandas==2.2", },
		);
		expect(packages,).toMatchObject({
			exact: false,
			method: "PUT",
			endpoint: "/public/api/admin/code-envs/PYTHON/audit_env",
		},);
		expect(packages.requests,).toHaveLength(2,);

		const images = buildMutationPlan(
			"code-env",
			"update-images",
			commands["code-env"]!["update-images"]!,
			[
				"PYTHON",
				"audit_env",
			],
			{ "env-version": "v1", "no-wait": true, },
		);
		expect(images.endpoint,).toBe(
			"/public/api/admin/code-envs/PYTHON/audit_env/images?envVersion=v1&wait=false",
		);

		const remove = buildMutationPlan("code-env", "delete", commands["code-env"]!.delete!, [
			"PYTHON",
			"audit_env",
		], { "no-wait": true, },);
		expect(remove,).toMatchObject({
			method: "DELETE",
			endpoint: "/public/api/admin/code-envs/PYTHON/audit_env?wait=false",
		},);
	});

	it("models guarded webapp and API service settings without exposing definitions", () => {
		const expectHash = "b".repeat(64,);
		const webapp = buildMutationPlan(
			"webapp",
			"update-settings",
			commands.webapp!["update-settings"]!,
			[
				"web-1",
			],
			{
				data: '{"params":{"secret":"webapp_secret_marker"}}',
				"expect-hash": expectHash,
				"project-key": "TEST",
			},
		);
		expect(webapp,).toMatchObject({
			exact: false,
			method: "PUT",
			endpoint: "/public/api/projects/TEST/webapps/web-1",
		},);
		expect(webapp.requests,).toHaveLength(2,);
		expect(JSON.stringify(webapp,),).not.toContain("webapp_secret_marker",);
		expect(buildCommandRegistry().webapp!["restart-backend"]!.async,).toBe("future",);

		const service = buildMutationPlan(
			"api-service",
			"save-settings",
			commands["api-service"]!["save-settings"]!,
			["svc",],
			{
				data: '{"secret":"api_service_secret_marker"}',
				"expect-hash": expectHash,
				"project-key": "TEST",
			},
		);
		expect(service,).toMatchObject({
			method: "PUT",
			endpoint: "/public/api/projects/TEST/apiservices/svc/settings",
			payload: { expectHash, settings: "<omitted>", },
		},);
		expect(service.requests,).toHaveLength(2,);
		expect(JSON.stringify(service,),).not.toContain("api_service_secret_marker",);
	});

	it("forwards deployable package and bundle parameters in exact plans", () => {
		const publishPackage = buildMutationPlan(
			"api-service",
			"publish-package",
			commands["api-service"]!["publish-package"]!,
			["svc", "v1",],
			{ "project-key": "TEST", "published-service-id": "prod svc", },
		);
		expect(publishPackage.endpoint,).toBe(
			"/public/api/projects/TEST/apiservices/svc/packages/v1/publish?publishedServiceId=prod+svc",
		);

		const exported = buildMutationPlan("bundle", "export", commands.bundle!.export!, ["v1",], {
			"project-key": "TEST",
			"release-notes": "ready now",
			"evaluate-standards-checks": "false",
		},);
		expect(exported,).toMatchObject({ method: "PUT", payload: {}, },);
		expect(exported.endpoint,).toBe(
			"/public/api/projects/TEST/bundles/exported/v1?releaseNotes=ready+now&evaluateProjectStandardsChecks=false",
		);

		const activated = buildMutationPlan("bundle", "activate", commands.bundle!.activate!, ["v1",], {
			"project-key": "TEST",
			scenarios: '{"daily":true,"hourly":false}',
		},);
		expect(activated,).toMatchObject({
			method: "POST",
			endpoint: "/public/api/projects/TEST/bundles/imported/v1/actions/activate",
			payload: { scenariosActiveOnActivation: { daily: true, hourly: false, }, },
		},);
	});
});

describe("agent contract accuracy: project-git surface", () => {
	it("exposes the exact project-git action list through the agent contract", () => {
		const registry = buildCommandRegistry();
		const gitActions = Object.keys(registry["project-git"] ?? {},).sort();
		expect(gitActions,).toEqual(
			[
				"add-library",
				"branches",
				"commit",
				"create-branch",
				"create-tag",
				"current-branch",
				"delete-branch",
				"delete-tag",
				"diff",
				"drop-and-rebuild",
				"fetch",
				"future-abort",
				"future-status",
				"future-wait",
				"get-remote",
				"list-libraries",
				"log",
				"pull",
				"push",
				"push-all-libraries",
				"push-library",
				"remove-library",
				"remove-remote",
				"reset-all-libraries",
				"reset-library",
				"reset-to-head",
				"reset-to-upstream",
				"revert-commit",
				"revert-to-revision",
				"set-library",
				"set-remote",
				"status",
				"switch",
				"tags",
			].sort(),
		);
		const contract = buildAgentContract();
		expect(contract.commands,).toHaveProperty(["actions", "project-git",], gitActions,);
	});

	it("exposes project-git acknowledgements and password flags safely", () => {
		const registry = buildCommandRegistry();
		const drop = registry["project-git"]?.["drop-and-rebuild"];
		expect(drop?.requiredFlags,).toContain("i-know-what-i-am-doing",);
		expect(drop?.destructive,).toBe("destructive",);

		const add = registry["project-git"]?.["add-library"];
		expect(add?.flags,).toEqual(
			expect.arrayContaining([
				expect.objectContaining({ name: "password-env", kind: "value", },),
				expect.objectContaining({
					name: "no-add-to-python-path",
					kind: "boolean",
				},),
			],),
		);
		// The plan never resolves the named environment variable; only the flag.
		expect(add?.requiredFlags,).not.toContain("password-env",);
	});

	it("refuses planning for project-git reads like status and log", () => {
		const git = commands["project-git"];
		expect(
			() => buildMutationPlan("project-git", "status", git.status!, [], { "project-key": "P", },),
		).toThrow(/only supported for mutating|mutating commands/i,);
		expect(
			() => buildMutationPlan("project-git", "log", git.log!, [], { "project-key": "P", },),
		).toThrow(/only supported for mutating|mutating commands/i,);
	});
});
