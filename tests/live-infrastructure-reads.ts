// Live infrastructure module: connection/code-env/plugin metadata reads,
// project-scoped LLM / knowledge-bank / streaming / continuous-activity /
// deployer coverage. Every case self-obtains its prerequisites inside its own
// ctx.check callback so a selected subcase never depends on an unselected
// discovery case, and one unavailable export never hides independent
// successful behaviors. Missing but genuinely configured prerequisites
// (SQL connection, knowledge bank, cost-bearing LLM) produce explicit blocked
// CaseResults via LiveCapabilityError — never mock passes, never fictitious
// defaults. Streaming endpoints and continuous activities provision their own
// owned HTTPSSE fixture, so no broker prerequisite exists for the default
// path. Real command defects stay failures.
import { LiveCapabilityError, LiveCommandError, type LiveContext, } from "./live-context.js";
import { exerciseOwnedDeployerDetails, } from "./live-deployers.js";
import { withOwnedSqlConnection, } from "./live-infrastructure-disposable.js";

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

function capabilityBlocked(message: string,): never {
	throw new LiveCapabilityError(message, "blocked",);
}

/**
 * Typed fixture boundary: the infra module records cross-case ids
 * (codeEnvName, knowledgeBankId, deployer ids) on the shared fixtures object,
 * whose declared shape does not yet carry these fields. One unknown-routed
 * boundary keeps the rest of the module free of chained assertions; the
 * returned reference mutates the manifest's fixtures in place.
 */
function fixturesBag(ctx: LiveContext,): Record<string, unknown> {
	const bag: Record<string, unknown> = {};
	for (const [key, value,] of Object.entries(ctx.fixtures,)) {
		bag[key] = value;
	}
	return bag;
}

/** Read one optional string fixture recorded by provisioning or a sibling case. */
function fixtureString(ctx: LiveContext, field: string,): string | undefined {
	return asString(fixturesBag(ctx,)[field],);
}

/** Record a cross-case id in fixtures so later iterations can reuse it. */
function rememberFixture(ctx: LiveContext, field: string, value: string | undefined,): void {
	if (value !== undefined) {
		fixturesBag(ctx,)[field] = value;
	}
}

/**
 * First string field of a list item, used to discover identifiers from live
 * list responses whose documented shapes vary across DSS versions.
 */
function firstListField(items: unknown[], fields: readonly string[],): string | undefined {
	for (const item of items) {
		const record = asRecord(item,);
		if (!record) continue;
		for (const field of fields) {
			const id = asString(record[field],);
			if (id !== undefined) return id;
		}
	}
	return undefined;
}

/** List connection names visible to this key via the CLI. */
async function listConnectionNames(ctx: LiveContext,): Promise<string[]> {
	const names = asArray(await ctx.run<unknown>(["connection", "list",],),)
		.map((name,) => asString(name,))
		.filter((name,): name is string => name !== undefined);
	return names;
}

/**
 * Resolve the connection to inspect: the run's configured connection when
 * visible, else the first listed one; with neither, an explicit blocker.
 */
function pickReadableConnection(names: string[], fallback: string,): string {
	if (names.includes(fallback,)) return fallback;
	const first = names[0];
	if (first !== undefined) return first;
	if (fallback !== "") return fallback;
	capabilityBlocked("No connection is visible to this key: connection list returned zero names.",);
}

/** Read a connection's admin definition and return its declared type. */
async function resolveConnectionType(
	ctx: LiveContext,
	name: string,
): Promise<string | undefined> {
	const details = asRecord(await ctx.run<unknown>(["connection", "get", name,],),);
	if (details === undefined) {
		throw new Error(`connection get ${name} returned no object.`,);
	}
	if (asString(details["name"],) === undefined && asString(details["type"],) === undefined) {
		throw new Error(`connection get ${name} returned neither name nor type.`,);
	}
	return asString(details["type"],);
}

// ---------------------------------------------------------------------------
// Connections: discovery, admin get, connectivity test, SQL catalog surface
// ---------------------------------------------------------------------------

/** Determine SQL-ness of a connection from its declared type. */
function isSqlType(type: string | undefined,): boolean {
	const normalized = (type ?? "").toLowerCase();
	return normalized.includes("sql",) && !normalized.includes("nosql",);
}

async function exerciseConnectionReads(ctx: LiveContext,): Promise<void> {
	const names = await listConnectionNames(ctx,);
	const target = pickReadableConnection(names, ctx.connection,);
	const inferred = asArray(
		await ctx.run<unknown>([
			"connection",
			"infer",
			"--mode",
			"rich",
			"--project-key",
			ctx.projectKey,
		],),
	);
	if (!Array.isArray(inferred,)) {
		throw new Error("connection infer --mode rich did not return an array.",);
	}
	await resolveConnectionType(ctx, target,);
}

/**
 * Connectivity assertion on the same live-discovered connection. DSS returns
 * connectionOK plus per-type notes; testing is unsupported for some types and
 * the endpoint reports that as a structured result rather than an error.
 */
async function exerciseConnectionTest(ctx: LiveContext,): Promise<void> {
	const names = await listConnectionNames(ctx,);
	const target = pickReadableConnection(names, ctx.connection,);
	const result = asRecord(await ctx.run<unknown>(["connection", "test", target,],),);
	if (result === undefined) {
		throw new Error(`connection test ${target} returned no object.`,);
	}
}

/**
 * Resolve a genuinely SQL-typed connection for import coverage: the explicit
 * DATAIKU_SQL_CONNECTION prerequisite wins; otherwise the case self-provisions
 * a disposable in-memory SQLite JDBC connection through the canonical
 * withOwnedSqlConnection helper (created and deleted inside the helper per
 * the owned-global ledger). Callers pass their body to the helper directly.
 */

async function discoverExistingSqlConnection(ctx: LiveContext,): Promise<string | undefined> {
	for (const name of await listConnectionNames(ctx,)) {
		if (isSqlType(await resolveConnectionType(ctx, name,),)) return name;
	}
	return undefined;
}

/** Prefer an explicit SQL connection; otherwise provision an owned persistent catalog. */
async function exerciseConnectionImportSurface(ctx: LiveContext,): Promise<void> {
	const explicit = process.env["DATAIKU_SQL_CONNECTION"]?.trim();
	if (explicit) {
		// An operator-designated SQL connection takes precedence; the helper's
		// disposable SQLite path is only for instances without one.
		await runImportLifecycle(ctx, explicit, false,);
		return;
	}
	const discovered = await discoverExistingSqlConnection(ctx,);
	if (discovered !== undefined) {
		await runImportLifecycle(ctx, discovered, false,);
		return;
	}
	// Catalog discovery and import run in different JDBC sessions.
	await withOwnedSqlConnection(ctx, "sqlimport", async (connectionName,) => {
		await runImportLifecycle(ctx, connectionName, true,);
	}, { persistent: true, },);
}

/** Import the actual prepared candidates; checked:false would silently import nothing. */
async function runImportLifecycle(
	ctx: LiveContext,
	connection: string,
	ownedFixture: boolean,
): Promise<void> {
	const projectKey = await ctx.createProject("sqlimport",);
	try {
		const schemas = asArray(
			await ctx.run<unknown>([
				"connection",
				"schemas",
				"--connection",
				connection,
				"--project-key",
				projectKey,
			],),
		);
		if (!Array.isArray(schemas,)) {
			throw new Error(`connection schemas ${connection} did not return an array.`,);
		}
		const tables = asRecord(
			await ctx.run<unknown>([
				"connection",
				"tables",
				"--connection",
				connection,
				"--project-key",
				projectKey,
			],),
		);
		if (tables === undefined) {
			throw new Error(`connection tables ${connection} returned no object.`,);
		}
		// The completed catalog nests the table list at result.tables (older
		// DSS builds returned result as a bare array); settle a still-unsettled
		// jobId first so the emptiness judgment always reads a completed
		// response.
		let catalog = tables;
		if (tables["hasResult"] !== true) {
			const jobId = asString(tables["jobId"],);
			if (jobId !== undefined) {
				const waited = asRecord(
					await ctx.run<unknown>([
						"future",
						"wait",
						jobId,
						"--timeout",
						"180000",
						"--poll-interval",
						"2000",
					],),
				);
				if (waited === undefined) {
					throw new Error(`future wait ${jobId} for connection tables returned no object.`,);
				}
				catalog = waited;
			}
		}
		const tableNames = catalogTableNames(catalog,);
		if (tableNames.length === 0) {
			const completedCatalog = JSON.stringify(catalog,).slice(0, 300,);
			const message = `No importable table in ${connection}: completed catalog ${completedCatalog}`;
			if (ownedFixture) throw new Error(message,);
			capabilityBlocked(message,);
		}
		const tableName = tableNames[0]!;

		// --- prepare-import: returns a future whose RESULT carries the
		// authoritative import candidates for this connection/table.
		const prepared = asRecord(
			await ctx.run<unknown>([
				"connection",
				"prepare-import",
				"--data",
				JSON.stringify({ keys: [{ connectionName: connection, name: tableName, },], },),
				"--project-key",
				projectKey,
			],),
		);
		if (prepared === undefined) {
			throw new Error(`connection prepare-import ${tableName} returned no object.`,);
		}
		const preparedResult = await waitImportFuture(ctx, "prepare-import", prepared,);

		// --- collect the actual candidates from the prepare result. DSS
		// documents the fields per connection kind; the objects are forwarded
		// verbatim except for the fields this case must control.
		const candidates = extractImportCandidates(preparedResult,);
		if (candidates.length === 0) {
			capabilityBlocked(
				`connection prepare-import returned no import candidates for ${connection}/${tableName} (result: ${
					JSON.stringify(preparedResult,).slice(0, 300,)
				}); the driver catalog may not expose the table for import.`,
			);
		}
		const datasetName = `live_imported_sqlite_i${ctx.iteration}`;
		const sqlCandidates = candidates.map((candidate,) => ({
			...candidate,
			checked: true,
			datasetName,
			existingDatasetsNames: [],
		}));
		const executed = asRecord(
			await ctx.run<unknown>([
				"connection",
				"execute-import",
				"--data",
				JSON.stringify({ sqlImportCandidates: sqlCandidates, },),
				"--project-key",
				projectKey,
			],),
		);
		if (executed === undefined) {
			throw new Error(`connection execute-import ${tableName} returned no object.`,);
		}
		await waitImportFuture(ctx, "execute-import", executed,);

		// --- assert the imported dataset materialized with a readable schema.
		const datasetDetails = asRecord(
			await ctx.run<unknown>([
				"dataset",
				"get",
				datasetName,
				"--project-key",
				projectKey,
			],),
		);
		if (datasetDetails === undefined) {
			throw new Error(`execute-import did not materialize dataset ${datasetName}.`,);
		}
		const schema = asRecord(
			await ctx.run<unknown>([
				"dataset",
				"schema",
				datasetName,
				"--project-key",
				projectKey,
			],),
		);
		if (schema === undefined) {
			throw new Error(`dataset schema for ${datasetName} returned no object.`,);
		}
		if (ownedFixture) {
			const preview = await ctx.run<
				{ columns: { name: string; }[]; rows: string[][]; rowCount: number; }
			>([
				"dataset",
				"preview",
				datasetName,
				"--max-rows",
				"2",
			], { projectKey, },);
			const id = preview.columns.findIndex(column => column.name === "id");
			const value = preview.columns.findIndex(column => column.name === "value");
			if (
				id < 0 || value < 0 || preview.rowCount !== 1
				|| preview.rows[0]?.[id] !== "1" || preview.rows[0]?.[value] !== "owned"
			) {
				throw new Error("Imported SQL fixture did not preserve its seeded row",);
			}
		}
	} finally {
		await ctx.deleteProject(projectKey,);
	}
}

/**
 * Wait a tables-import future and return its RESULT payload (the prepare
 * candidates / execute receipt). Inline results (hasResult already true on
 * the returned state) are honored without a wait round-trip.
 */
async function waitImportFuture(
	ctx: LiveContext,
	label: string,
	envelope: JsonRecord,
): Promise<JsonRecord> {
	const inlineResult = asRecord(envelope["result"],);
	if (inlineResult !== undefined) return inlineResult;
	const jobId = asString(envelope["jobId"],) ?? asString(envelope["id"],)
		?? asString(envelope["futureId"],);
	if (jobId === undefined) {
		throw new Error(
			`connection ${label} returned no future id to await: ${
				JSON.stringify(envelope,).slice(0, 200,)
			}.`,
		);
	}
	const waited = asRecord(
		await ctx.run<unknown>([
			"future",
			"wait",
			jobId,
			"--timeout",
			"180000",
			"--poll-interval",
			"2000",
		],),
	);
	if (waited === undefined) {
		throw new Error(`future wait ${jobId} for ${label} returned no object.`,);
	}
	const result = asRecord(waited["result"],);
	if (result === undefined) {
		throw new Error(`future wait ${jobId} for ${label} completed without a result payload.`,);
	}
	return result;
}

/**
 * Pull the SQL import candidate objects out of a prepare-import result.
 * DSS returns the candidates under a kind-specific key; every array of
 * objects inside the result is inspected for the documented candidate
 * markers (connectionName + table / name) so no server shape is fabricated.
 */
function extractImportCandidates(result: JsonRecord,): JsonRecord[] {
	const found: JsonRecord[] = [];
	const walk = (value: unknown,): void => {
		if (Array.isArray(value,)) {
			for (const item of value) walk(item,);
			return;
		}
		const record = asRecord(value,);
		if (record === undefined) return;
		const connectionName = asString(record["connectionName"],);
		const table = asString(record["table"],) ?? asString(record["tableName"],)
			?? asString(record["name"],);
		if (connectionName !== undefined && table !== undefined) {
			found.push(record,);
			return;
		}
		for (const nested of Object.values(record,)) walk(nested,);
	};
	for (const value of Object.values(result,)) walk(value,);
	return found;
}

/**
 * Table names out of a completed list-tables catalog envelope. The DSS 15
 * response nests the list at result.tables (result may also be a bare array
 * on older builds); both shapes are descended, and an entry counts as a
 * table only when it carries name / tableName / table.
 */
function catalogTableNames(catalog: JsonRecord,): string[] {
	const names: string[] = [];
	const collect = (value: unknown,): void => {
		if (Array.isArray(value,)) {
			for (const item of value) collect(item,);
			return;
		}
		const record = asRecord(value,);
		if (record === undefined) return;
		const name = asString(record["name"],) ?? asString(record["tableName"],)
			?? asString(record["table"],);
		if (name !== undefined) {
			names.push(name,);
			return;
		}
		for (const nested of Object.values(record,)) collect(nested,);
	};
	collect(asRecord(catalog["result"],) ?? catalog,);
	return names;
}

// ---------------------------------------------------------------------------
// Code environments: read-only surface of a live-discovered env
// ---------------------------------------------------------------------------

/**
 * Resolve a readable PYTHON code env: safety-agent fixture when recorded,
 * else the live list; an instance without any PYTHON env is a factual
 * blocker because the infrastructure profile never creates globals.
 */
async function resolvePythonCodeEnv(ctx: LiveContext,): Promise<string> {
	const remembered = fixtureString(ctx, "codeEnvName",);
	if (remembered) return remembered;
	const envs = asArray(
		await ctx.run<unknown>(["code-env", "list", "--lang", "PYTHON",],),
	)
		.map((item,) => asRecord(item,))
		.filter((item,): item is JsonRecord => item !== undefined)
		.map((item,) => asString(item["envName"],) ?? asString(item["name"],))
		.filter((name,): name is string => name !== undefined);
	if (envs.length === 0) {
		capabilityBlocked(
			"No PYTHON code environment exists on this instance (code-env list --lang PYTHON returned zero); get/get-definition/logs need an existing env and the infrastructure profile never creates globals.",
		);
	}
	return envs[0]!;
}

async function exerciseCodeEnvReads(ctx: LiveContext,): Promise<void> {
	const envName = await resolvePythonCodeEnv(ctx,);
	const details = asRecord(
		await ctx.run<unknown>(["code-env", "get", "PYTHON", envName,],),
	);
	if (details === undefined) {
		throw new Error(`code-env get PYTHON ${envName} returned no object.`,);
	}
	const definition = asRecord(
		await ctx.run<unknown>(["code-env", "get-definition", "PYTHON", envName,],),
	);
	if (definition === undefined) {
		throw new Error(`code-env get-definition PYTHON ${envName} returned no object.`,);
	}
	// code-env version is a read-only probe keyed to this run's project; the
	// list payload does not always carry envVersion, so the call is
	// unconditional — a DSS rejection here is genuine behavior, not masked.
	await ctx.run<unknown>(["code-env", "version", "PYTHON", envName, ctx.projectKey,],);
	const logs = asArray(
		await ctx.run<unknown>(["code-env", "list-logs", "PYTHON", envName,],),
	);
	const logName = firstListField(logs, ["name", "logName", "id",],);
	if (logName !== undefined) {
		await ctx.run<unknown>([
			"code-env",
			"get-log",
			"PYTHON",
			envName,
			logName,
			"--max-lines",
			"20",
		],);
	}
	await ctx.run<unknown>(["code-env", "usages", "PYTHON", envName,],);
}

// ---------------------------------------------------------------------------
// Plugins: list + usage surface of a live-discovered plugin
// ---------------------------------------------------------------------------

async function exercisePluginReads(ctx: LiveContext,): Promise<void> {
	const plugins = asArray(await ctx.run<unknown>(["plugin", "list",],),)
		.map((item,) => asRecord(item,))
		.filter((item,): item is JsonRecord => item !== undefined);
	const pluginId = plugins
		.map((item,) => asString(item["pluginId"],) ?? asString(item["id"],))
		.find((id,): id is string => id !== undefined);
	if (pluginId === undefined) {
		capabilityBlocked(
			"No plugin is installed on this instance (plugin list returned zero usable ids); plugin usages needs an existing plugin and the infrastructure profile never installs one.",
		);
	}
	const usages = asRecord(await ctx.run<unknown>(["plugin", "usages", pluginId,],),);
	if (usages === undefined) {
		throw new Error(`plugin usages ${pluginId} returned no object.`,);
	}
}

// ---------------------------------------------------------------------------
// LLM Mesh: catalog + cost-bearing probes behind explicit authorization
// ---------------------------------------------------------------------------

interface LlmCatalogEntry {
	id: string;
	purpose?: string;
}

/**
 * Discover the live LLM catalog of a project. The documented id field varies
 * by DSS version (llmId on newer, id on older), so both are accepted.
 */
async function discoverLlmCatalog(
	ctx: LiveContext,
	projectKey: string,
): Promise<LlmCatalogEntry[]> {
	const llms = asArray(
		await ctx.run<unknown>(["llm", "list", "--project-key", projectKey,],),
	)
		.map((item,) => asRecord(item,))
		.filter((item,): item is JsonRecord => item !== undefined);
	return llms
		.map((item,) => ({
			id: asString(item["llmId"],) ?? asString(item["id"],) ?? "",
			purpose: asString(item["purpose"],) ?? asString(item["type"],),
		}))
		.filter((entry,) => entry.id !== "");
}

async function exerciseLlmCatalog(ctx: LiveContext,): Promise<void> {
	const projectKey = await ctx.createProject("llmcatalog",);
	try {
		const catalog = await discoverLlmCatalog(ctx, projectKey,);
		if (catalog.length === 0) {
			rememberFixture(ctx, "llmCatalogEmpty", "true",);
		}
	} finally {
		await ctx.deleteProject(projectKey,);
	}
}

/**
 * Cost-bearing LLM probes. Listing models does NOT authorize billing: the
 * probe runs only when the operator explicitly designates a free/local LLM
 * id via DATAIKU_LIVE_FREE_LLM_ID; otherwise the probe is a precise billing
 * blocker even when the catalog lists candidates.
 */
function freeLlmId(): string {
	const explicit = process.env["DATAIKU_LIVE_FREE_LLM_ID"]?.trim();
	if (explicit) return explicit;
	capabilityBlocked(
		"LLM completions/embeddings are cost-bearing and the catalog listing is not billing authorization: set DATAIKU_LIVE_FREE_LLM_ID to an explicitly user-authorized free/local LLM id to run the probe. No LLM is invoked without that operator designation.",
	);
}

async function exerciseLlmProbes(ctx: LiveContext,): Promise<void> {
	const llmId = freeLlmId();
	const projectKey = await ctx.createProject("llmprobe",);
	try {
		const completions = asRecord(
			await ctx.run<unknown>([
				"llm",
				"completions",
				"--data",
				JSON.stringify({
					llmId,
					queries: [{ messages: [{ role: "user", content: "Reply with the single word: ok", },], },],
				},),
				"--project-key",
				projectKey,
			],),
		);
		if (asArray(completions?.["responses"],).length === 0) {
			throw new Error("llm completions returned no responses.",);
		}
		const embeddings = asRecord(
			await ctx.run<unknown>([
				"llm",
				"embeddings",
				"--data",
				JSON.stringify({ llmId, queries: [{ text: "knowledge base smoke test", },], },),
				"--project-key",
				projectKey,
			],),
		);
		if (asArray(embeddings?.["responses"],).length === 0) {
			throw new Error("llm embeddings returned no responses.",);
		}
	} finally {
		await ctx.deleteProject(projectKey,);
	}
}

// ---------------------------------------------------------------------------
// Knowledge banks: search + clear against an explicitly provided bank
// ---------------------------------------------------------------------------

/**
 * Knowledge bank prerequisite. The CLI exposes no knowledge-bank list or
 * create action, so a bank id must arrive explicitly. Two provenance
 * classes are kept apart on purpose:
 *   - `owned`: a bank created by THIS lab (fixtures.knowledgeBankId with
 *     fixtures.knowledgeBankOwned === true) — search AND clear are allowed.
 *   - `external`: an operator-supplied pre-existing bank
 *     (DATAIKU_LIVE_KB_ID) — search only; clear is withheld because the
 *     user authorized only new lab-created global resources for mutation.
 * Without either, the case is a factual blocker naming the gap.
 */
interface KnowledgeBankTarget {
	id: string;
	owned: boolean;
}

function resolveKnowledgeBank(ctx: LiveContext,): KnowledgeBankTarget {
	const ownedId = fixtureString(ctx, "knowledgeBankId",);
	if (ownedId && fixturesBag(ctx,)["knowledgeBankOwned"] === true) {
		return { id: ownedId, owned: true, };
	}
	const external = process.env["DATAIKU_LIVE_KB_ID"]?.trim() || ownedId;
	if (external) return { id: external, owned: false, };
	capabilityBlocked(
		"No knowledge bank is available: the CLI has no knowledge-bank list/create action and neither DATAIKU_LIVE_KB_ID nor a lab-owned fixtures.knowledgeBankId was provided. Configure an existing bank id to exercise search (clear runs only on lab-created banks).",
	);
}

async function exerciseKnowledgeBank(ctx: LiveContext,): Promise<void> {
	const bank = resolveKnowledgeBank(ctx,);
	const projectKey = await ctx.createProject("kbprobe",);
	try {
		const search = asRecord(
			await ctx.run<unknown>([
				"knowledge-bank",
				"search",
				bank.id,
				"--data",
				JSON.stringify({ query: "smoke probe", },),
				"--project-key",
				projectKey,
			],),
		);
		if (search === undefined) {
			throw new Error(`knowledge-bank search ${bank.id} returned no object.`,);
		}
		if (!bank.owned) {
			// Search executed and counts; clear is DESTRUCTIVE and the user only
			// authorized mutation of NEW lab-created globals, never a supplied
			// pre-existing bank. Reported as a precise per-action blocker.
			capabilityBlocked(
				`knowledge-bank clear withheld for ${bank.id}: the bank was supplied externally (DATAIKU_LIVE_KB_ID / non-owned fixture), not created by this lab, and the CLI has no knowledge-bank create action to provision a disposable one. Search executed successfully.`,
			);
		}
		const cleared = asRecord(
			await ctx.run<unknown>(["knowledge-bank", "clear", bank.id, "--project-key", projectKey,],),
		);
		if (cleared === undefined) {
			throw new Error(`knowledge-bank clear ${bank.id} returned no object.`,);
		}
	} finally {
		await ctx.deleteProject(projectKey,);
	}
}

// ---------------------------------------------------------------------------
// Streaming endpoints: full project-scoped lifecycle
// ---------------------------------------------------------------------------

/**
 * Streaming fixture resolution. HTTP-SSE endpoints need NO broker connection
 * at all — the official client documents the endpoint connection as "None for
 * HTTP SSE endpoints" and creates it with body {id, projectKey,
 * type:"httpsse", params:{url}} — so the default fixture is a case-owned SSE
 * daemon served from the owned host directory at
 * http://127.0.0.1:<port>/events (owned-sse-host contract). An explicitly
 * user-configured broker connection (DATAIKU_LIVE_STREAMING_CONNECTION) is
 * still honored and switches the case to a broker (kafka) endpoint created
 * through that connection. That is an opt-in, never a prerequisite: no broker
 * is required for the default path.
 */
type StreamingFixture =
	| { mode: "httpsse"; url: string; }
	| { mode: "broker"; connection: string; };

/**
 * Run `body` with a resolved streaming fixture. The owned SSE daemon is
 * started inside the case-owned host directory and torn down by
 * withOwnedHostDirectory's own receipt-verified cleanup (pid + startTime +
 * process group), so the default path needs no .env broker entry and leaks no
 * process.
 */
async function withStreamingFixture<T,>(
	ctx: LiveContext,
	label: string,
	body: (fixture: StreamingFixture,) => Promise<T>,
): Promise<T> {
	const explicit = process.env["DATAIKU_LIVE_STREAMING_CONNECTION"]?.trim();
	if (explicit) return await body({ mode: "broker", connection: explicit, },);
	return await ctx.withOwnedHostDirectory(label, async directory => {
		const daemon = await ctx.startOwnedSseServer(directory,);
		return await body({
			mode: "httpsse",
			url: `http://127.0.0.1:${daemon.port}/events`,
		},);
	},);
}

/**
 * Create one streaming endpoint with the officially documented body shape and
 * return its creation receipt.
 */
async function createStreamingEndpoint(
	ctx: LiveContext,
	endpointId: string,
	fixture: StreamingFixture,
	projectKey: string,
): Promise<JsonRecord> {
	const params = fixture.mode === "httpsse"
		? { url: fixture.url, }
		: { connection: fixture.connection, topic: `live-suite-${ctx.iteration}`, };
	let created: JsonRecord | undefined;
	try {
		created = asRecord(
			await ctx.run<unknown>([
				"streaming-endpoint",
				"create",
				endpointId,
				fixture.mode === "httpsse" ? "httpsse" : "kafka",
				"--data",
				JSON.stringify(params,),
				"--project-key",
				projectKey,
			],),
		);
	} catch (error) {
		const evidence = streamingDisabledEvidence(error,);
		if (evidence !== undefined) {
			capabilityBlocked(
				`This DSS instance reports streaming as unavailable while creating the ${
					fixture.mode === "httpsse" ? "http-sse" : "broker"
				} streaming endpoint: ${evidence}`,
			);
		}
		throw error;
	}
	if (created === undefined) {
		throw new Error(`streaming-endpoint create ${endpointId} returned no object.`,);
	}
	return created;
}

async function exerciseStreamingEndpoints(ctx: LiveContext,): Promise<void> {
	await withStreamingFixture(ctx, "streaming_sse", async fixture => {
		const projectKey = await ctx.createProject("streaming",);
		try {
			const endpointId = `live_stream_i${ctx.iteration}`;
			await createStreamingEndpoint(ctx, endpointId, fixture, projectKey,);
			try {
				const settings = asRecord(
					await ctx.run<unknown>([
						"streaming-endpoint",
						"get",
						endpointId,
						"--project-key",
						projectKey,
					],),
				);
				if (settings === undefined) {
					throw new Error(`streaming-endpoint get ${endpointId} returned no object.`,);
				}
				if (asString(settings["id"],) !== endpointId) {
					throw new Error(
						`streaming-endpoint get returned a different id: ${JSON.stringify(settings["id"],)}.`,
					);
				}
				if (fixture.mode === "httpsse") {
					// The created settings must reflect the official httpsse shape:
					// no broker connection anywhere (connection is None for HTTP SSE
					// endpoints) and the params carry the owned daemon URL.
					if (settings["connection"] !== undefined && settings["connection"] !== null) {
						throw new Error(
							`httpsse streaming endpoint unexpectedly declares a connection: ${
								JSON.stringify(settings["connection"],)
							}.`,
						);
					}
					const params = asRecord(settings["params"],);
					if (asString(params?.["url"],) !== fixture.url) {
						throw new Error(
							`httpsse streaming endpoint params.url is not the owned daemon URL: ${
								JSON.stringify(params?.["url"],)
							}.`,
						);
					}
				}
				const updated = await ctx.run<unknown>([
					"streaming-endpoint",
					"update-settings",
					endpointId,
					"--data",
					JSON.stringify(settings,),
					"--project-key",
					projectKey,
				],);
				if (asRecord(updated,) === undefined) {
					throw new Error("streaming-endpoint update-settings returned no object.",);
				}
			} finally {
				await ctx.run<unknown>([
					"streaming-endpoint",
					"delete",
					endpointId,
					"--project-key",
					projectKey,
				],);
			}
			// The list after cleanup must not still contain the throwaway endpoint.
			const remaining = asArray(
				await ctx.run<unknown>(["streaming-endpoint", "list", "--project-key", projectKey,],),
			);
			if (remaining.some((item,) => asString(asRecord(item,)?.["id"],) === endpointId)) {
				throw new Error(`streaming-endpoint delete left ${endpointId} behind.`,);
			}
		} finally {
			await ctx.deleteProject(projectKey,);
		}
	},);
}

// ---------------------------------------------------------------------------
// Continuous activities: full streaming chain provisioned inside the same case
// ---------------------------------------------------------------------------

/** Bounded wait between live activity/output polls (never an unbounded loop). */
const CONTINUOUS_POLL_INTERVAL_MS = 4_000;
/** Bounded attempts for reaching desiredState STARTED and for output rows. */
const CONTINUOUS_POLL_ATTEMPTS = 30;
/** Bounded attempts for confirming a stop before the chain is retained. */
const CONTINUOUS_STOP_POLL_ATTEMPTS = 10;

/**
 * Schema of the owned SSE daemon's bounded record stream (contract locked with
 * the owned host worker): data events {"n":1,"value":"one"} ..
 * {"n":5,"value":"five"} at ~0.2s cadence followed by ": heartbeat" comments;
 * n is an integer and maps to the DSS bigint storage type, value a string.
 */
const OWNED_SSE_COLUMNS = [
	{ name: "n", type: "bigint", },
	{ name: "value", type: "string", },
];
/** Stream values that prove rows flowed from the owned daemon into DSS. */
const OWNED_SSE_VALUES = ["one", "two", "three", "four", "five",];

/** Bounded sleep between live polls (same helper shape as sibling live modules). */
function sleep(ms: number,): Promise<void> {
	const { promise, resolve, } = Promise.withResolvers<void>();
	setTimeout(resolve, ms,);
	return promise;
}

/**
 * Narrow classifier for an instance-level streaming capability gap: only an
 * error whose own text states streaming is not enabled/available on this DSS
 * instance is reported as a blocked prerequisite. Payload-shape, permission,
 * and worker-reachability errors stay real failures — a recipe or command
 * defect is never masked as "streaming disabled".
 */
function streamingDisabledEvidence(error: unknown,): string | undefined {
	const result = error instanceof LiveCommandError ? JSON.stringify(error.result,) : "";
	const evidence = `${error instanceof Error ? error.message : String(error,)} ${result}`.trim();
	const text = evidence.toLowerCase();
	if (
		text.includes("streaming",)
		&& (text.includes("not enabled",) || text.includes("disabled",)
			|| text.includes("not available",) || text.includes("not activated",))
	) {
		return evidence.slice(0, 500,);
	}
	return undefined;
}

/** Case-owned continuous-chain ids plus the existence flags cleanup needs. */
interface ContinuousChain {
	endpointId: string;
	outputName: string;
	recipeName: string;
	recipeId?: string;
	endpointCreated: boolean;
	outputCreated: boolean;
	recipeCreated: boolean;
	activityStarted: boolean;
	activityConfirmedStopped: boolean;
}

/**
 * Provision the continuous chain in creation order: streaming endpoint →
 * managed file output dataset (schema declared from the owned stream) → csync
 * recipe through the generic recipe-create route. Official single-output
 * creator semantics: the streaming endpoint is the csync `main` input
 * ({"items":[{"ref":"<endpoint>","deps":[]}]}) and the managed dataset the
 * `main` output. The endpoint schema is declared with the documented GET →
 * modify → PUT settings flow so the chain consumes a typed stream.
 */
async function provisionContinuousChain(
	ctx: LiveContext,
	projectKey: string,
	chain: ContinuousChain,
	fixture: StreamingFixture,
): Promise<void> {
	await createStreamingEndpoint(ctx, chain.endpointId, fixture, projectKey,);
	chain.endpointCreated = true;
	const settings = asRecord(
		await ctx.run<unknown>([
			"streaming-endpoint",
			"get",
			chain.endpointId,
			"--project-key",
			projectKey,
		],),
	);
	if (settings === undefined) {
		throw new Error(`streaming-endpoint get ${chain.endpointId} returned no object.`,);
	}
	await ctx.run<unknown>([
		"streaming-endpoint",
		"update-settings",
		chain.endpointId,
		"--data",
		JSON.stringify({ ...settings, schema: { columns: OWNED_SSE_COLUMNS, }, },),
		"--project-key",
		projectKey,
	],);
	await ctx.run<unknown>([
		"dataset",
		"create-managed",
		"--name",
		chain.outputName,
		"--connection",
		ctx.connection,
		"--project-key",
		projectKey,
	],);
	chain.outputCreated = true;
	await ctx.run<unknown>([
		"dataset",
		"refresh-schema",
		chain.outputName,
		"--data",
		JSON.stringify({ columns: OWNED_SSE_COLUMNS, },),
		"--project-key",
		projectKey,
	],);
	await ctx.run<unknown>([
		"recipe",
		"create",
		"--type",
		"csync",
		"--input",
		chain.endpointId,
		"--output",
		chain.outputName,
		"--name",
		chain.recipeName,
		"--project-key",
		projectKey,
	],);
	chain.recipeCreated = true;
}

/**
 * Start the continuous activity and prove the loop consumed the owned stream
 * by polling status to desiredState STARTED and reading the daemon's records
 * back from the output dataset — status alone is never accepted as success.
 * The stop is always attempted exactly once and confirmed with bounded status
 * polls; an unconfirmed stop leaves activityConfirmedStopped false so the
 * caller retains the chain instead of deleting it underneath a live loop.
 */
async function runContinuousActivityLifecycle(
	ctx: LiveContext,
	projectKey: string,
	chain: ContinuousChain,
): Promise<void> {
	const statusArgv = (recipeId: string,): string[] => [
		"continuous-activity",
		"status",
		recipeId,
		"--project-key",
		projectKey,
	];
	let failure: unknown;
	try {
		const listed = asArray(
			await ctx.run<unknown>(["continuous-activity", "list", "--project-key", projectKey,],),
		);
		const exact = listed.find(item => asString(asRecord(item,)?.["recipeId"],) === chain.recipeName);
		// The throwaway project holds exactly one continuous recipe, so a single
		// listed activity is provably this recipe's; anything else is ambiguous.
		const activity = asRecord(exact,) ?? (listed.length === 1 ? asRecord(listed[0],) : undefined);
		const recipeId = asString(activity?.["recipeId"],) ?? asString(activity?.["id"],);
		if (recipeId === undefined) {
			throw new Error(
				`continuous-activity list found no activity for the freshly created csync recipe ${chain.recipeName}: ${
					JSON.stringify(listed,).slice(0, 300,)
				}.`,
			);
		}
		chain.recipeId = recipeId;
		// Verify the created recipe really is the documented csync chain.
		const recipeDoc = asRecord(
			await ctx.run<unknown>(["recipe", "get", chain.recipeName, "--project-key", projectKey,],),
		);
		const recipeBody = asRecord(recipeDoc?.["recipe"],);
		if (asString(recipeBody?.["type"],)?.toLowerCase() !== "csync") {
			throw new Error(
				`recipe get ${chain.recipeName} did not report a csync recipe: ${
					JSON.stringify(recipeBody?.["type"],)
				}.`,
			);
		}
		const inputRefs = asArray(asRecord(asRecord(recipeBody?.["inputs"],)?.["main"],)?.["items"],)
			.map(item => asString(asRecord(item,)?.["ref"],))
			.filter((ref,): ref is string => ref !== undefined);
		if (!inputRefs.includes(chain.endpointId,)) {
			throw new Error(
				`csync recipe ${chain.recipeName} does not declare the streaming endpoint ${chain.endpointId} as a main input: ${
					JSON.stringify(inputRefs,)
				}.`,
			);
		}
		const initial = asRecord(await ctx.run<unknown>(statusArgv(recipeId,),),);
		if (initial === undefined) {
			throw new Error(`continuous-activity status ${recipeId} returned no object.`,);
		}
		await ctx.run<unknown>([
			"continuous-activity",
			"start",
			recipeId,
			"--project-key",
			projectKey,
		],);
		chain.activityStarted = true;
		let status: JsonRecord | undefined;
		for (let attempt = 0; attempt < CONTINUOUS_POLL_ATTEMPTS; attempt++) {
			status = asRecord(await ctx.run<unknown>(statusArgv(recipeId,),),);
			if (asString(status?.["desiredState"],) === "STARTED") break;
			await sleep(CONTINUOUS_POLL_INTERVAL_MS,);
		}
		if (asString(status?.["desiredState"],) !== "STARTED") {
			throw new Error(
				`continuous activity ${recipeId} never reached desiredState STARTED within the bounded poll (${
					(CONTINUOUS_POLL_ATTEMPTS * CONTINUOUS_POLL_INTERVAL_MS) / 1000
				}s): ${JSON.stringify(status,).slice(0, 300,)}.`,
			);
		}
		let preview: JsonRecord | undefined;
		for (let attempt = 0; attempt < CONTINUOUS_POLL_ATTEMPTS; attempt++) {
			preview = asRecord(
				await ctx.run<unknown>([
					"dataset",
					"preview",
					chain.outputName,
					"--max-rows",
					"20",
					"--project-key",
					projectKey,
				],),
			);
			if (asArray(preview?.["rows"],).length > 0) break;
			await sleep(CONTINUOUS_POLL_INTERVAL_MS,);
		}
		const rows = asArray(preview?.["rows"],);
		if (
			rows.length === 0
			|| !OWNED_SSE_VALUES.some(value => JSON.stringify(rows,).includes(`"${value}"`,))
		) {
			throw new Error(
				`continuous activity ${recipeId} started but the output dataset ${chain.outputName} never showed the owned stream records within the bounded poll (last preview: ${
					JSON.stringify(preview,).slice(0, 300,)
				}).`,
			);
		}
	} catch (error) {
		failure = error;
	}
	const recipeId = chain.recipeId;
	if (recipeId === undefined || !chain.activityStarted) {
		if (failure !== undefined) throw failure;
		return;
	}
	try {
		await ctx.run<unknown>([
			"continuous-activity",
			"stop",
			recipeId,
			"--project-key",
			projectKey,
		],);
	} catch (error) {
		failure = failure === undefined
			? error
			: new Error(`${String(failure,)}; stop failed: ${String(error,)}`,);
	}
	let stopped: JsonRecord | undefined;
	for (let attempt = 0; attempt < CONTINUOUS_STOP_POLL_ATTEMPTS; attempt++) {
		try {
			stopped = asRecord(await ctx.run<unknown>(statusArgv(recipeId,),),);
		} catch (error) {
			failure = failure === undefined ? error : failure;
			break;
		}
		if (asString(stopped?.["desiredState"],) !== "STARTED") break;
		await sleep(CONTINUOUS_POLL_INTERVAL_MS,);
	}
	chain.activityConfirmedStopped = asString(stopped?.["desiredState"],) !== "STARTED";
	if (!chain.activityConfirmedStopped && failure === undefined) {
		failure = new Error(
			`continuous activity ${recipeId} still reports desiredState STARTED after stop and bounded polling: ${
				JSON.stringify(stopped,).slice(0, 300,)
			}.`,
		);
	}
	if (failure !== undefined) throw failure;
}

/**
 * Ordered teardown once the activity is stopped (or never started): recipe →
 * endpoint → output dataset → project, best-effort per step so one failed
 * delete never hides the others, with every cleanup error reported.
 */
async function releaseContinuousChain(
	ctx: LiveContext,
	projectKey: string,
	chain: ContinuousChain,
): Promise<void> {
	const cleanupErrors: string[] = [];
	if (chain.recipeCreated) {
		try {
			await ctx.run<unknown>([
				"recipe",
				"delete",
				chain.recipeName,
				"--if-exists",
				"--project-key",
				projectKey,
			],);
		} catch (error) {
			cleanupErrors.push(`recipe delete ${chain.recipeName}: ${String(error,)}`,);
		}
	}
	if (chain.endpointCreated) {
		try {
			await ctx.run<unknown>([
				"streaming-endpoint",
				"delete",
				chain.endpointId,
				"--project-key",
				projectKey,
			],);
		} catch (error) {
			cleanupErrors.push(`streaming-endpoint delete ${chain.endpointId}: ${String(error,)}`,);
		}
	}
	if (chain.outputCreated) {
		try {
			await ctx.run<unknown>([
				"dataset",
				"delete",
				chain.outputName,
				"--if-exists",
				"--project-key",
				projectKey,
			],);
		} catch (error) {
			cleanupErrors.push(`dataset delete ${chain.outputName}: ${String(error,)}`,);
		}
	}
	try {
		await ctx.deleteProject(projectKey,);
	} catch (error) {
		cleanupErrors.push(`project delete ${projectKey}: ${String(error,)}`,);
	}
	if (cleanupErrors.length > 0) {
		throw new Error(`Continuous-chain cleanup failed: ${cleanupErrors.join("; ",)}.`,);
	}
}

/**
 * Continuous activities: the lifecycle provisions the whole chain in a
 * throwaway project — owned HTTPSSE endpoint (or the explicitly
 * user-configured broker connection when DATAIKU_LIVE_STREAMING_CONNECTION is
 * set), managed file output dataset declared with the owned stream schema, and
 * a csync recipe created through the generic recipe-create route — then starts
 * the activity, proves rows from the owned stream landed in the output dataset,
 * and stops it. HTTP-SSE endpoints are read-only by product design, so no
 * producer/push coverage is claimed here. Any DSS rejection of this documented
 * chain is a genuine failure: only an instance error whose own text states
 * streaming is unavailable is classified as a blocker (see
 * streamingDisabledEvidence on the endpoint create). Cleanup order: stop the
 * activity, delete the recipe and endpoint, then the dataset and project; an
 * unconfirmed stop retains the case-owned chain for diagnosis.
 */
async function exerciseContinuousActivities(ctx: LiveContext,): Promise<void> {
	await withStreamingFixture(ctx, "continuous_sse", async fixture => {
		const projectKey = await ctx.createProject("contact",);
		const chain: ContinuousChain = {
			endpointId: `live_stream_i${ctx.iteration}`,
			outputName: `live_stream_output_${ctx.iteration}`,
			recipeName: `live_cont_sync_${ctx.iteration}`,
			endpointCreated: false,
			outputCreated: false,
			recipeCreated: false,
			activityStarted: false,
			activityConfirmedStopped: false,
		};
		let failure: unknown;
		try {
			await provisionContinuousChain(ctx, projectKey, chain, fixture,);
			await runContinuousActivityLifecycle(ctx, projectKey, chain,);
		} catch (error) {
			failure = error;
		}
		if (chain.activityStarted && !chain.activityConfirmedStopped) {
			throw new Error(
				`Retaining the case-owned continuous chain in project ${projectKey} (recipe ${chain.recipeName}, endpoint ${chain.endpointId}, dataset ${chain.outputName}) because the activity could not be confirmed stopped${
					failure === undefined ? "" : `; original failure: ${String(failure,)}`
				}.`,
			);
		}
		try {
			await releaseContinuousChain(ctx, projectKey, chain,);
		} catch (error) {
			if (failure !== undefined) {
				throw new Error(`${String(failure,)}; ${String(error,)}`, { cause: error, },);
			}
			throw error;
		}
		if (failure !== undefined) throw failure;
	},);
}

// ---------------------------------------------------------------------------
// Deployers: instance-scoped lists + follow-up details for discovered ids
// ---------------------------------------------------------------------------

interface DeployerDiscovered {
	infraId?: string;
	deploymentId?: string;
}

/**
 * Instance-scoped deployer lists, verified as arrays, with the first infra
 * and deployment id extracted for the deployer-lists fixtures.
 */
async function discoverDeployers(ctx: LiveContext,): Promise<DeployerDiscovered> {
	const infras = asArray(await ctx.run<unknown>(["api-deployer", "list-infras",],),);
	const stages = asArray(await ctx.run<unknown>(["api-deployer", "list-stages",],),);
	const services = asArray(await ctx.run<unknown>(["api-deployer", "list-services",],),);
	const deployments = asArray(await ctx.run<unknown>(["api-deployer", "list-deployments",],),);
	const pdProjects = asArray(await ctx.run<unknown>(["project-deployer", "list-projects",],),);
	const pdDeployments = asArray(await ctx.run<unknown>(["project-deployer", "list-deployments",],),);
	const pdInfras = asArray(await ctx.run<unknown>(["project-deployer", "list-infras",],),);
	for (
		const [name, value,] of Object.entries({
			"api-deployer list-infras": infras,
			"api-deployer list-stages": stages,
			"api-deployer list-services": services,
			"api-deployer list-deployments": deployments,
			"project-deployer list-projects": pdProjects,
			"project-deployer list-deployments": pdDeployments,
			"project-deployer list-infras": pdInfras,
		},)
	) {
		if (!Array.isArray(value,)) throw new Error(`${name} did not return an array.`,);
	}
	return {
		infraId: firstListField(infras, ["id", "infraId",],),
		deploymentId: firstListField(deployments, ["id", "deploymentId",],),
	};
}

async function exerciseDeployerLists(ctx: LiveContext,): Promise<void> {
	const discovered = await discoverDeployers(ctx,);
	rememberFixture(ctx, "deployerDeploymentId", discovered.deploymentId,);
	rememberFixture(ctx, "deployerInfraId", discovered.infraId,);
}

/**
 * Follow-up details for deployer objects are covered by
 * exerciseOwnedDeployerDetails (tests/live-deployers.ts), which provisions the
 * owned API/project-deployer metadata stack and asserts the detail gets
 * against it; the instance-scoped list discovery below stays read-only.
 */

// ---------------------------------------------------------------------------
// Instance directories: user/group/meaning/workspace/data-collection/folders
// ---------------------------------------------------------------------------

/**
 * Directory reads with discovered-id follow-ups inside the same callback:
 * lists always run; per-id gets run only when their list produced an id, so
 * an empty directory hides nothing and blocks nothing independently.
 */
async function exerciseDirectoryReads(ctx: LiveContext,): Promise<void> {
	const users = asArray(await ctx.run<unknown>(["user", "list",],),);
	const login = firstListField(users, ["login",],);
	if (login !== undefined) {
		await ctx.run<unknown>(["user", "get", login,],);
		await ctx.run<unknown>(["user", "activity-get", login,],);
	}
	await ctx.run<unknown>(["user", "activity",],);
	const groups = asArray(await ctx.run<unknown>(["group", "list",],),);
	const group = firstListField(groups, ["name", "groupName",],);
	if (group !== undefined) await ctx.run<unknown>(["group", "get", group,],);
	const meanings = asArray(await ctx.run<unknown>(["meaning", "list",],),);
	const meaningId = firstListField(meanings, ["id", "meaningId",],);
	if (meaningId !== undefined) await ctx.run<unknown>(["meaning", "get", meaningId,],);
	const workspaces = asArray(await ctx.run<unknown>(["workspace", "list",],),);
	const workspaceKey = firstListField(workspaces, ["workspaceKey", "id",],);
	if (workspaceKey !== undefined) {
		await ctx.run<unknown>(["workspace", "get", workspaceKey,],);
		await ctx.run<unknown>(["workspace", "list-objects", workspaceKey,],);
	}
	const collections = asArray(await ctx.run<unknown>(["data-collection", "list",],),);
	const collectionId = firstListField(collections, ["id", "dataCollectionId",],);
	if (collectionId !== undefined) {
		await ctx.run<unknown>(["data-collection", "get", collectionId,],);
		await ctx.run<unknown>(["data-collection", "list-objects", collectionId,],);
	}
	const root = asRecord(await ctx.run<unknown>(["project-folder", "root",],),);
	const rootId = asString(root?.["id"],) ?? asString(root?.["rootFolderId"],);
	if (rootId !== undefined) {
		await ctx.run<unknown>(["project-folder", "get", rootId,],);
		// DSS root folders have no settings object: GET /project-folders/ROOT/settings
		// answers 500 "Root project folder does not have settings" (live-verified),
		// so settings-get targets a discovered non-root child instead.
		const childId = asArray(root?.["childrenIds"],)
			.map((id,) => asString(id,))
			.find((id,): id is string => id !== undefined);
		if (childId !== undefined) {
			await ctx.run<unknown>(["project-folder", "settings-get", childId,],);
		}
	}
	if (
		users.length === 0 && groups.length === 0 && workspaces.length === 0 && collections.length === 0
		&& root === undefined
	) {
		throw new Error(
			"Every directory surface returned empty and project-folder root returned no object; at minimum the root project folder must exist.",
		);
	}
}

// ---------------------------------------------------------------------------
// Module entry point
// ---------------------------------------------------------------------------

/**
 * Exercise the infrastructure profile: read-only instance metadata plus
 * project-scoped mutation surfaces (streaming endpoints, continuous
 * activities, knowledge banks, connection imports) that create and tear down
 * their own case-owned resources inside selected check callbacks.
 *
 * Phase contract (parent LiveContext): no persistent fixtures in either
 * phase; every mutating scenario runs on a case-owned project deleted in
 * finally, so repeated selected runs stay idempotent. ctx.selection filtering
 * is applied automatically by ctx.check.
 *
 * Cost-bearing LLM probes never auto-execute: the live catalog listing is
 * not billing authorization, so a probe runs only against an explicitly
 * user-designated free/local LLM id (DATAIKU_LIVE_FREE_LLM_ID) and is a
 * precise billing blocker otherwise. Genuine environment gaps (no SQL
 * connection, no knowledge bank, no code env, no plugin) are explicit blocked
 * results naming the exact gap — never mock passes and never blanket blockers
 * hiding sibling behaviors. Streaming endpoints and continuous activities
 * need no broker: they self-provision the owned HTTPSSE fixture, and only an
 * instance error whose own text reports streaming unavailable blocks them.
 */
export async function exerciseInfrastructureReads(ctx: LiveContext,): Promise<void> {
	if (!ctx.profiles.includes("infrastructure",)) return;

	await ctx.check(
		"infrastructure.connections-reads",
		["connection.list", "connection.infer", "connection.get", "connection.test",],
		async () => {
			await exerciseConnectionReads(ctx,);
			await exerciseConnectionTest(ctx,);
		},
		{ capability: "infrastructure.connections-read", required: false, },
	);
	await ctx.check(
		"infrastructure.connection-import-surface",
		[
			"connection.schemas",
			"connection.tables",
			"connection.prepare-import",
			"connection.execute-import",
			"future.wait",
		],
		async () => {
			await exerciseConnectionImportSurface(ctx,);
		},
		{ capability: "infrastructure.connection-sql-catalog", required: false, },
	);
	await ctx.check(
		"infrastructure.code-env-reads",
		[
			"code-env.list",
			"code-env.get",
			"code-env.get-definition",
			"code-env.list-logs",
			"code-env.get-log",
			"code-env.version",
			"code-env.usages",
		],
		async () => {
			await exerciseCodeEnvReads(ctx,);
		},
		{ capability: "infrastructure.code-env-read", required: false, },
	);
	await ctx.check(
		"infrastructure.plugin-reads",
		["plugin.list", "plugin.usages",],
		async () => {
			await exercisePluginReads(ctx,);
		},
		{ capability: "infrastructure.plugin-read", required: false, },
	);
	await ctx.check(
		"infrastructure.llm-catalog",
		["llm.list",],
		async () => {
			await exerciseLlmCatalog(ctx,);
		},
		{ capability: "infrastructure.llm-catalog", required: false, },
	);
	await ctx.check(
		"infrastructure.llm-probes",
		["llm.completions", "llm.embeddings",],
		async () => {
			await exerciseLlmProbes(ctx,);
		},
		{ capability: "infrastructure.llm-cost-bearing", required: false, },
	);
	await ctx.check(
		"infrastructure.knowledge-bank",
		["knowledge-bank.search", "knowledge-bank.clear",],
		async () => {
			await exerciseKnowledgeBank(ctx,);
		},
		{ capability: "infrastructure.knowledge-bank", required: false, },
	);
	await ctx.check(
		"infrastructure.streaming-endpoints",
		[
			"streaming-endpoint.create",
			"streaming-endpoint.get",
			"streaming-endpoint.update-settings",
			"streaming-endpoint.delete",
			"streaming-endpoint.list",
		],
		async () => {
			await exerciseStreamingEndpoints(ctx,);
		},
		{ capability: "infrastructure.streaming-lifecycle", required: false, },
	);
	await ctx.check(
		"infrastructure.continuous-activities",
		[
			"streaming-endpoint.create",
			"streaming-endpoint.get",
			"streaming-endpoint.update-settings",
			"streaming-endpoint.delete",
			"dataset.create-managed",
			"dataset.refresh-schema",
			"dataset.preview",
			"dataset.delete",
			"recipe.create",
			"recipe.get",
			"recipe.delete",
			"continuous-activity.list",
			"continuous-activity.status",
			"continuous-activity.start",
			"continuous-activity.stop",
			"project.delete",
		],
		async () => {
			await exerciseContinuousActivities(ctx,);
		},
		{ capability: "infrastructure.continuous-lifecycle", required: false, },
	);
	await ctx.check(
		"infrastructure.deployer-lists",
		[
			"api-deployer.list-infras",
			"api-deployer.list-stages",
			"api-deployer.list-services",
			"api-deployer.list-deployments",
			"project-deployer.list-projects",
			"project-deployer.list-deployments",
			"project-deployer.list-infras",
		],
		async () => {
			await exerciseDeployerLists(ctx,);
		},
		{ capability: "infrastructure.deployer-lists", required: false, },
	);
	await ctx.check(
		"infrastructure.deployer-details",
		[
			"api-deployer.list-infras",
			"api-deployer.list-stages",
			"api-deployer.list-services",
			"api-deployer.list-deployments",
			"api-deployer.get-infra",
			"api-deployer.get-service",
			"api-deployer.get-deployment",
			"api-deployer.deployment-status",
			"api-deployer.deployment-settings",
			"api-deployer.create-infra",
			"api-deployer.create-service",
			"api-deployer.publish-version",
			"api-deployer.create-deployment",
			"api-deployer.save-deployment-settings",
			"api-deployer.delete-deployment",
			"api-deployer.delete-service",
			"api-deployer.delete-infra",
			"bundle.export",
			"bundle.download-exported",
			"project-deployer.list-projects",
			"project-deployer.list-deployments",
			"project-deployer.get-deployment",
			"project-deployer.deployment-status",
			"project-deployer.project-status",
			"project-deployer.create-infra",
			"project-deployer.upload-bundle",
			"project-deployer.create-deployment",
			"project-deployer.delete-deployment",
		],
		async () => {
			await exerciseOwnedDeployerDetails(ctx,);
		},
		{ capability: "infrastructure.deployer-details", required: false, },
	);
	await ctx.check(
		"infrastructure.directory-reads",
		[
			"user.list",
			"user.get",
			"user.activity",
			"user.activity-get",
			"group.list",
			"group.get",
			"meaning.list",
			"meaning.get",
			"workspace.list",
			"workspace.get",
			"workspace.list-objects",
			"data-collection.list",
			"data-collection.get",
			"data-collection.list-objects",
			"project-folder.root",
			"project-folder.get",
			"project-folder.settings-get",
		],
		async () => {
			await exerciseDirectoryReads(ctx,);
		},
		{ capability: "infrastructure.directory-reads", required: false, },
	);
}
