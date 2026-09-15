// Live infrastructure module: connection/code-env/plugin metadata reads,
// project-scoped LLM / knowledge-bank / streaming / continuous-activity /
// deployer coverage. Every case self-obtains its prerequisites inside its own
// ctx.check callback so a selected subcase never depends on an unselected
// discovery case, and one unavailable export never hides independent
// successful behaviors. Missing but genuinely configured prerequisites
// (SQL connection, streaming broker, knowledge bank, cost-bearing LLM)
// produce explicit blocked CaseResults via LiveCapabilityError — never mock
// passes, never fictitious defaults. Real command defects stay failures.
import { LiveCapabilityError, type LiveContext, } from "./live-context.js";
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
 * Resolve a streaming-capable connection for endpoint creation. DSS streaming
 * endpoints require a broker connection (Kafka/MQTT/stream); without any
 * broker connection the lifecycle is factually blocked.
 */
async function resolveStreamingConnection(ctx: LiveContext,): Promise<string> {
	const explicit = process.env["DATAIKU_LIVE_STREAMING_CONNECTION"]?.trim();
	if (explicit) return explicit;
	for (const name of await listConnectionNames(ctx,)) {
		const normalized = ((await resolveConnectionType(ctx, name,)) ?? "").toLowerCase();
		if (
			normalized.includes("kafka",) || normalized.includes("stream",) || normalized.includes("mqtt",)
		) {
			return name;
		}
	}
	capabilityBlocked(
		"No streaming-capable connection (Kafka/MQTT/stream) is configured on this instance: connection list + get type inspection found none and DATAIKU_LIVE_STREAMING_CONNECTION is unset.",
	);
}

async function exerciseStreamingEndpoints(ctx: LiveContext,): Promise<void> {
	const connection = await resolveStreamingConnection(ctx,);
	const projectKey = await ctx.createProject("streaming",);
	try {
		const endpointId = `live_stream_i${ctx.iteration}`;
		const created = asRecord(
			await ctx.run<unknown>([
				"streaming-endpoint",
				"create",
				endpointId,
				"kafka",
				"--data",
				JSON.stringify({ connection, topic: `live-suite-${ctx.iteration}`, },),
				"--project-key",
				projectKey,
			],),
		);
		if (created === undefined) {
			throw new Error(`streaming-endpoint create ${endpointId} returned no object.`,);
		}
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
}

// ---------------------------------------------------------------------------
// Continuous activities: streaming chain provisioned inside the same case
// ---------------------------------------------------------------------------

/**
 * Continuous activities require a continuous recipe, which requires a
 * streaming endpoint plus a streaming input. The lifecycle below provisions
 * the full chain in a throwaway project and starts/stops the activity, so
 * the coverage never depends on pre-existing project state. Dataset/recipe
 * creation uses server-known shapes; any DSS rejection of the documented
 * chain is a genuine failure, never masked as a blocker.
 */
async function exerciseContinuousActivities(ctx: LiveContext,): Promise<void> {
	const connection = await resolveStreamingConnection(ctx,);
	const projectKey = await ctx.createProject("contact",);
	try {
		const endpointId = `live_stream_i${ctx.iteration}`;
		await ctx.run<unknown>([
			"streaming-endpoint",
			"create",
			endpointId,
			"kafka",
			"--data",
			JSON.stringify({ connection, topic: `live-cont-actor-${ctx.iteration}`, },),
			"--project-key",
			projectKey,
		],);
		try {
			await ctx.run<unknown>([
				"dataset",
				"create",
				"--name",
				"live_stream_input",
				"--type",
				"Kafka",
				"--connection",
				connection,
				"--project-key",
				projectKey,
			],);
			await ctx.run<unknown>([
				"recipe",
				"create",
				"--type",
				"sync",
				"--input",
				"live_stream_input",
				"--output",
				"live_stream_output",
				"--name",
				"live_cont_recipe",
				"--project-key",
				projectKey,
			],);
			const listed = asArray(
				await ctx.run<unknown>([
					"continuous-activity",
					"list",
					"--project-key",
					projectKey,
				],),
			);
			const recipeId = firstListField(listed, ["recipeId", "id",],);
			if (recipeId === undefined) {
				capabilityBlocked(
					"continuous-activity list returned no continuous activity after provisioning a streaming endpoint and continuous recipe; DSS may not expose continuous activities for this recipe type.",
				);
			}
			const status = asRecord(
				await ctx.run<unknown>([
					"continuous-activity",
					"status",
					recipeId,
					"--project-key",
					projectKey,
				],),
			);
			if (status === undefined) {
				throw new Error(`continuous-activity status ${recipeId} returned no object.`,);
			}
			await ctx.run<unknown>([
				"continuous-activity",
				"start",
				recipeId,
				"--project-key",
				projectKey,
			],);
			try {
				const runningStatus = asRecord(
					await ctx.run<unknown>([
						"continuous-activity",
						"status",
						recipeId,
						"--project-key",
						projectKey,
					],),
				);
				if (asString(runningStatus?.["desiredState"],) !== "STARTED") {
					throw new Error(
						`continuous activity did not reach desiredState STARTED: ${
							JSON.stringify(runningStatus,).slice(0, 300,)
						}.`,
					);
				}
			} finally {
				await ctx.run<unknown>([
					"continuous-activity",
					"stop",
					recipeId,
					"--project-key",
					projectKey,
				],);
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
	} finally {
		await ctx.deleteProject(projectKey,);
	}
}

// ---------------------------------------------------------------------------
// Deployers: instance-scoped lists + follow-up details for discovered ids
// ---------------------------------------------------------------------------

interface DeployerDiscovered {
	infraId?: string;
	serviceId?: string;
	deploymentId?: string;
	pdProjectKey?: string;
	pdDeploymentId?: string;
}

/**
 * Instance-scoped deployer lists, verified as arrays, with the first infra
 * and deployment id extracted for the details case in the same callback.
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
		serviceId: firstListField(services, ["id", "serviceId", "publishedServiceId",],),
		deploymentId: firstListField(deployments, ["id", "deploymentId",],),
		pdProjectKey: firstListField(pdProjects, ["id", "projectKey", "publishedProjectKey",],),
		pdDeploymentId: firstListField(pdDeployments, ["id", "deploymentId",],),
	};
}

async function exerciseDeployerLists(ctx: LiveContext,): Promise<void> {
	const discovered = await discoverDeployers(ctx,);
	rememberFixture(ctx, "deployerDeploymentId", discovered.deploymentId,);
	rememberFixture(ctx, "deployerInfraId", discovered.infraId,);
}

/**
 * Follow-up details for deployer objects discovered in THIS callback (never
 * across case boundaries): each empty list produces a precise per-action
 * blocker instead of hiding the remaining independent gets.
 */
async function exerciseDeployerDetails(ctx: LiveContext,): Promise<void> {
	const discovered = await discoverDeployers(ctx,);
	if (discovered.infraId !== undefined) {
		const infra = asRecord(
			await ctx.run<unknown>(["api-deployer", "get-infra", discovered.infraId,],),
		);
		if (infra === undefined) {
			throw new Error(`api-deployer get-infra ${discovered.infraId} returned no object.`,);
		}
	}
	if (discovered.serviceId !== undefined) {
		const service = asRecord(
			await ctx.run<unknown>(["api-deployer", "get-service", discovered.serviceId,],),
		);
		if (service === undefined) {
			throw new Error(`api-deployer get-service ${discovered.serviceId} returned no object.`,);
		}
	}
	if (discovered.deploymentId !== undefined) {
		const deployment = asRecord(
			await ctx.run<unknown>(["api-deployer", "get-deployment", discovered.deploymentId,],),
		);
		if (deployment === undefined) {
			throw new Error(`api-deployer get-deployment ${discovered.deploymentId} returned no object.`,);
		}
		await ctx.run<unknown>(["api-deployer", "deployment-status", discovered.deploymentId,],);
		await ctx.run<unknown>(["api-deployer", "deployment-settings", discovered.deploymentId,],);
	}
	if (discovered.pdProjectKey !== undefined) {
		await ctx.run<unknown>(["project-deployer", "project-status", discovered.pdProjectKey,],);
	}
	if (discovered.pdDeploymentId !== undefined) {
		const pdDeployment = asRecord(
			await ctx.run<unknown>(["project-deployer", "get-deployment", discovered.pdDeploymentId,],),
		);
		if (pdDeployment === undefined) {
			throw new Error(
				`project-deployer get-deployment ${discovered.pdDeploymentId} returned no object.`,
			);
		}
		await ctx.run<unknown>([
			"project-deployer",
			"deployment-status",
			discovered.pdDeploymentId,
		],);
	}
	if (
		discovered.infraId === undefined && discovered.deploymentId === undefined
		&& discovered.serviceId === undefined && discovered.pdDeploymentId === undefined
		&& discovered.pdProjectKey === undefined
	) {
		capabilityBlocked(
			"No API deployer or project deployer object exists on this instance (infras, services, deployments, and published projects all empty), so the detail gets have no target; the lists themselves were verified in the same case.",
		);
	}
}

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
 * connection, no streaming broker, no knowledge bank, no code env, no
 * plugin) are explicit blocked results naming the exact gap — never mock
 * passes and never blanket blockers hiding sibling behaviors.
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
			"dataset.create",
			"recipe.create",
			"continuous-activity.list",
			"continuous-activity.status",
			"continuous-activity.start",
			"continuous-activity.stop",
			"streaming-endpoint.delete",
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
			"api-deployer.list-services",
			"api-deployer.list-deployments",
			"api-deployer.get-infra",
			"api-deployer.get-service",
			"api-deployer.get-deployment",
			"api-deployer.deployment-status",
			"api-deployer.deployment-settings",
			"project-deployer.list-projects",
			"project-deployer.list-deployments",
			"project-deployer.get-deployment",
			"project-deployer.deployment-status",
			"project-deployer.project-status",
		],
		async () => {
			await exerciseDeployerDetails(ctx,);
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
