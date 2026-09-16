// Disposable global-resource lifecycle for the infrastructure profile. Every
// case reserves an exact run-scoped identity through the owned-global ledger
// (manifest.globals), drives real CLI create/get/update/list verbs, and tears
// the resource down in a finally block through ctx.deleteGlobal. Steps that
// need genuinely external dependencies (external auth backends, plugin-store
// access, external Git remotes, API-node deployment targets) are separate
// cases reported as explicit LiveCapabilityError blockers — never successes,
// never silent skips.
// Case ids requested from and registered by Main; nothing here runs without
// the serialized live-suite dispatcher.
import { randomBytes, } from "node:crypto";
import type { FutureState, FutureWaitResult, } from "../src/schemas.js";
import type { LiveCaseId, } from "./live-cases.js";
import type { LiveContext, OwnedGlobal, } from "./live-context.js";
import { LiveCapabilityError, LiveCommandError, } from "./live-context.js";
import type { HostDirectory, } from "./live-host.js";

type JsonRecord = Record<string, unknown>;

/** Upper bound for waiting on the code-env creation build future. */
const CODE_ENV_WAIT_TIMEOUT_MS = 420_000;
/** Upper bound for waiting on a code-env mutation build future. */
const CODE_ENV_MUTATION_WAIT_TIMEOUT_MS = 180_000;

function asRecord(value: unknown,): JsonRecord | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value,)
		? value as JsonRecord
		: undefined;
}

function asString(value: unknown,): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

function recordId(result: unknown, keys: string[],): string | undefined {
	const record = asRecord(result,);
	if (!record) return undefined;
	for (const key of keys) {
		const id = asString(record[key],);
		if (id) return id;
	}
	return undefined;
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

/**
 * Guaranteed teardown for one owned global: runs body, then deletes the bound
 * resource in finally — including on assertion failures. Deletion only ever
 * targets a bound, identity-matched ledger entry (never unconfirmed entries,
 * never the root project). `clearFixture` runs in finally so no fixture id
 * ever points at a deleted resource, whether or not body succeeded. Errors
 * from body and teardown are both surfaced — a successful body with a failed
 * cleanup is a failure, never a silent pass. An UnsettledOwnedFutureError
 * from body preserves the resource (no deletion) and is surfaced too.
 */
async function withOwnedGlobal<T,>(
	ctx: LiveContext,
	kind: Parameters<LiveContext["deleteGlobal"]>[0],
	entry: OwnedGlobal,
	body: (id: string,) => Promise<T>,
): Promise<T> {
	const bound = requireBound(entry, kind,);
	const errors: unknown[] = [];
	let preserve = false;
	let result: T | undefined;
	let succeeded = false;
	try {
		result = await body(bound.id!,);
		succeeded = true;
	} catch (error) {
		errors.push(error,);
		if (error instanceof UnsettledOwnedFutureError) preserve = true;
	} finally {
		clearTransientFixture(ctx, kind,);
		if (preserve) {
			// Never delete an owned resource whose build future is not
			// confirmed terminal; surface the preserved state explicitly.
			errors.push(
				new LiveCapabilityError(
					`owned ${kind} ${bound.id} preserved for inspection: an owned build future could not be confirmed terminal`,
				),
			);
		} else {
			try {
				await ctx.deleteGlobal(kind, bound.id!,);
			} catch (error) {
				errors.push(error,);
			}
		}
	}
	if (errors.length === 1) throw errors[0];
	if (errors.length) {
		throw new AggregateError(
			errors,
			errors.map(e => e instanceof Error ? e.message : String(e,)).join("; ",),
		);
	}
	if (!succeeded) throw new Error("unreachable: withOwnedGlobal body did not produce a result",);
	return result as T;
}

/**
 * Clears this module's transient fixture id for a kind at teardown time so a
 * deleted resource never leaves a dangling reference. Only ids set by this
 * module's own cases are touched; caller-owned fixtures are never modified.
 */
function clearTransientFixture(
	ctx: LiveContext,
	kind: Parameters<LiveContext["deleteGlobal"]>[0],
): void {
	if (kind === "project-folder") ctx.fixtures.projectFolderId = undefined;
	if (kind === "code-env") ctx.fixtures.codeEnvName = undefined;
	if (kind === "plugin") ctx.fixtures.pluginId = undefined;
}

/**
 * Terminal only when the server proves the build ended: a retrieved result,
 * an abort, or a dead process. `unknown === true` is NEVER terminal (Main's
 * ruling, and the schema accepts {unknown:true, alive:false} together): an
 * unknown state after a failed wait means terminality was not confirmed, so
 * it must fall to the preserve path.
 */
function isTerminalFutureState(state: FutureState | undefined,): boolean {
	if (state === undefined || state.unknown === true) return false;
	return state.hasResult === true || state.aborted === true || state.alive === false;
}

/**
 * Raised when an owned build future could not be confirmed terminal after a
 * failed wait: the owning resource must be preserved for inspection instead
 * of being deleted mid-build.
 */
class UnsettledOwnedFutureError extends Error {
	constructor(message: string,) {
		super(message,);
		this.name = "UnsettledOwnedFutureError";
	}
}

/**
 * Settle a lab-owned future after a failed or timed-out wait: peek, abort if
 * still alive (only recorded lab futures ever reach here), then peek again.
 * Returns true only when a terminal state is confirmed — deletion of the
 * owning resource must never proceed on an unconfirmed future.
 */
async function settleOwnedFuture(ctx: LiveContext, futureId: string,): Promise<boolean> {
	const first = await ctx.run<FutureState>(["future", "peek", futureId,],).catch(() => undefined);
	if (isTerminalFutureState(first,)) return true;
	await ctx.run(["future", "abort", futureId,],).catch(() => undefined);
	const second = await ctx.run<FutureState>(["future", "peek", futureId,],).catch(() => undefined);
	return isTerminalFutureState(second,);
}

/**
 * Await the build future of a mutation that returned a jobId (--no-wait
 * form). `expectFuture` marks commands whose registry async kind is `future`:
 * a missing jobId on their receipt is fail-closed (UnsettledOwnedFutureError
 * → preserve), never a silent pass. A recordFuture failure is fail-closed
 * too: without abort rights terminality cannot be confirmed. On a
 * failed/timed-out wait the future is settled (peek → abort → peek): only
 * server-proven terminal states (result/aborted/dead) rethrow the real error;
 * anything unconfirmed preserves the resource.
 */
async function awaitMutationFuture(
	ctx: LiveContext,
	result: unknown,
	label: string,
	options: { expectFuture?: boolean; timeoutMs?: number; } = {},
): Promise<void> {
	const jobId = asString(asRecord(result,)?.["jobId"],);
	if (jobId === undefined) {
		if (options.expectFuture !== true) return;
		throw new UnsettledOwnedFutureError(
			`${label} is an asynchronous command but its receipt carried no jobId; cannot confirm the build terminal`,
		);
	}
	try {
		await ctx.recordFuture(jobId,);
	} catch (error) {
		throw new UnsettledOwnedFutureError(
			`${label} future ${jobId} could not be recorded for abort rights: ${
				error instanceof Error ? error.message : String(error,)
			}`,
		);
	}
	let waitError: unknown;
	try {
		const waited = await ctx.run<FutureWaitResult>([
			"future",
			"wait",
			jobId,
			"--timeout",
			String(options.timeoutMs ?? CODE_ENV_MUTATION_WAIT_TIMEOUT_MS,),
		],);
		if (waited.timedOut === true || waited.success !== true) {
			waitError = new LiveCapabilityError(
				`${label} future ${jobId} ended ${waited.state ?? "unknown"} without success`,
			);
		}
	} catch (error) {
		waitError = error;
	}
	if (waitError === undefined) return;
	const terminal = await settleOwnedFuture(ctx, jobId,);
	if (!terminal) {
		throw new UnsettledOwnedFutureError(
			`${label} future ${jobId} could not be confirmed terminal after a failed wait: ${
				waitError instanceof Error ? waitError.message : String(waitError,)
			}`,
		);
	}
	throw waitError;
}

/**
 * Attempt a command whose success depends on an external prerequisite or a
 * global-mutation authorization. DSS's own refusal (400/403/404/501) or the
 * live sandbox's refusal of an unowned global mutation is reported as an
 * exact LiveCapabilityError prerequisite block — never an assumed block,
 * never a silent skip, never a future-TODO message. Any other error
 * propagates unchanged.
 */
async function attemptOrBlock<T,>(
	ctx: LiveContext,
	argv: string[],
	label: string,
	context?: string,
): Promise<T | undefined> {
	try {
		return await ctx.run<T>(argv,);
	} catch (error) {
		if (error instanceof LiveCommandError) {
			const payload = error.result as { status?: number; error?: string; } | null;
			const status = payload?.status;
			if (status === 400 || status === 403 || status === 404 || status === 501) {
				throw new LiveCapabilityError(
					`${label}${context !== undefined ? ` — ${context}` : ""} (DSS's own response: ${
						payload?.error ?? "refused"
					})`,
					status === 501 ? "unsupported" : "blocked",
				);
			}
			throw error;
		}
		if (
			error instanceof Error
			&& /not authorized by a project sandbox|Unregistered live command|Refusing foreign/.test(
				error.message,
			)
		) {
			throw new LiveCapabilityError(
				`${label} is an instance-global operation with no owned target; the live project sandbox intentionally refuses global mutations${
					context !== undefined ? ` — ${context}` : ""
				} (guard's own response: ${error.message})`,
			);
		}
		throw error;
	}
}

function listArgv(kind: string, id: string,): string[] {
	switch (kind) {
		case "code-env":
			return ["code-env", "list", "--lang", id.split("/",)[0] ?? "PYTHON",];
		default:
			return [kind, "list",];
	}
}

function listExists(list: unknown, id: string,): boolean {
	const needle = id.includes("/",) ? id.split("/",)[1]! : id;
	if (Array.isArray(list,)) {
		return list.some(item => {
			if (typeof item === "string") return item === id || item === needle;
			const record = asRecord(item,);
			return record !== undefined
				&& Object.values(record,).some(v => v === id || v === needle);
		},);
	}
	return Object.keys(asRecord(list,) ?? {},).some(key => key === id || key === needle);
}

async function assertListed(ctx: LiveContext, kind: string, id: string,): Promise<void> {
	const list = await ctx.run<unknown>(listArgv(kind, id,),);
	if (!listExists(list, id,)) {
		throw new Error(`${kind} ${id} missing from list output after creation`,);
	}
}

async function userLifecycle(ctx: LiveContext,): Promise<void> {
	// Per-user random password: never derived from public run identifiers,
	// never printed or persisted (the guard redacts it from argv receipts).
	const password = `L!${randomBytes(18,).toString("base64url",)}aB9`;
	const entry = await ctx.createGlobal("user", "user", (name, marker,) => [
		"user",
		"create",
		"--data",
		JSON.stringify({
			login: name,
			sourceType: "LOCAL",
			displayName: `Live lab ${ctx.iteration}`,
			email: `${marker}@sdk-live.invalid`,
			groups: [],
			userProfile: "DATA_ANALYST",
			password,
		},),
	],);
	await withOwnedGlobal(ctx, "user", entry, async id => {
		await assertListed(ctx, "user", id,);
		await ctx.run(["user", "get", id,],);
		await ctx.run([
			"user",
			"update",
			id,
			"--data",
			JSON.stringify({
				login: id,
				displayName: `Live lab ${ctx.iteration} updated`,
				email: `${ctx.markerFor("user", entry.name,)}@sdk-live.invalid`,
			},),
		],);
		await ctx.run(["user", "activity-get", id,],);
	},);
}

async function groupLifecycle(ctx: LiveContext,): Promise<void> {
	const entry = await ctx.createGlobal("group", "group", (name, marker,) => [
		"group",
		"create",
		"--data",
		JSON.stringify({ name, type: "NORMAL", admin: false, description: marker, },),
	],);
	await withOwnedGlobal(ctx, "group", entry, async id => {
		await assertListed(ctx, "group", id,);
		await ctx.run(["group", "get", id,],);
		await ctx.run([
			"group",
			"update",
			id,
			"--data",
			JSON.stringify({
				name: id,
				type: "NORMAL",
				description: ctx.markerFor("group", entry.name,),
			},),
		],);
	},);
}

async function meaningLifecycle(ctx: LiveContext,): Promise<void> {
	// DSS exposes no public deletion route for meanings (DELETE /meanings/{id}
	// answers 405; the DSS 15 REST reference and the official Python client
	// document list/create/get/update only). Creating a new meaning would leak
	// an uncleanable owned global, so this case reuses the already-bound owned
	// meaning from an earlier run for list/get/update readback, then reports
	// the cleanup block with the exact evidence.
	const bound = ctx.globals.find(global => global.kind === "meaning" && global.state === "bound");
	if (bound === undefined) {
		throw new LiveCapabilityError(
			"no bound owned meaning is available and creating one would leak an uncleanable global: DSS exposes no public meaning deletion route (DELETE /meanings/{id} answers 405)",
		);
	}
	const id = bound.id ?? bound.name;
	await assertListed(ctx, "meaning", id,);
	const fetched = await ctx.run<JsonRecord>(["meaning", "get", id,],);
	if (asString(fetched["id"],) !== id) {
		throw new Error(`meaning get ${id} did not echo the requested id`,);
	}
	const updatedLabel = `Live lab meaning ${id} v${String(ctx.iteration,)}`;
	const updated = await attemptOrBlock<JsonRecord>(ctx, [
		"meaning",
		"update",
		id,
		"--data",
		JSON.stringify({ ...fetched, label: updatedLabel, },),
	], "meaning update",);
	if (updated !== undefined) {
		const readback = await ctx.run<JsonRecord>(["meaning", "get", id,],);
		if (asString(readback["label"],) !== updatedLabel) {
			throw new Error("meaning update did not persist the changed label",);
		}
	}
	throw new LiveCapabilityError(
		`meaning lifecycle cleanup is blocked: DSS exposes no public deletion route for meanings (DELETE /meanings/{id} answers 405; the DSS 15 REST reference and the official Python client document list/create/get/update only). Owned meaning ${id} is retained from an earlier run; new creates are deliberately skipped so no uncleanable global is added.`,
	);
}

async function workspaceLifecycle(ctx: LiveContext,): Promise<void> {
	const entry = await ctx.createGlobal("workspace", "workspace", (name, marker,) => [
		"workspace",
		"create",
		"--data",
		JSON.stringify({
			workspaceKey: name,
			displayName: `Live lab workspace ${ctx.iteration}`,
			description: marker,
		},),
	],);
	await withOwnedGlobal(ctx, "workspace", entry, async id => {
		await assertListed(ctx, "workspace", id,);
		await ctx.run(["workspace", "get", id,],);
		await ctx.run([
			"workspace",
			"add-object",
			id,
			"--data",
			JSON.stringify({
				htmlLink: { name: `Live lab link ${ctx.iteration}`, url: "https://example.com/live-lab", },
			},),
		],);
		const objects = await ctx.run<Array<JsonRecord>>(["workspace", "list-objects", id,],);
		if (!objects.some(object => asString(asRecord(object["htmlLink"],)?.["url"],))) {
			throw new Error("workspace add-object did not persist the html link",);
		}
		// Full GET/merge/PUT with a CHANGED documented field, then read-back:
		// the description moves to a new marker-bearing value (distinct from the
		// creation value), proving the write took effect rather than echoing.
		const settings = await ctx.run<JsonRecord>(["workspace", "get", id,],);
		const updatedDescription = `${ctx.markerFor("workspace", entry.name,)} updated`;
		await ctx.run([
			"workspace",
			"update-settings",
			id,
			"--data",
			JSON.stringify({ ...settings, description: updatedDescription, },),
		],);
		const readback = await ctx.run<JsonRecord>(["workspace", "get", id,],);
		if (asString(readback["description"],) !== updatedDescription) {
			throw new Error("workspace update-settings did not persist the changed description",);
		}
		if (asString(readback["displayName"],) !== asString(settings["displayName"],)) {
			throw new Error("workspace update-settings did not preserve displayName across the merge",);
		}
		await ctx.run(["workspace", "list-objects", id,],);
	},);
}

async function dataCollectionLifecycle(ctx: LiveContext,): Promise<void> {
	const entry = await ctx.createGlobal("data-collection", "collection", (name, marker,) => [
		"data-collection",
		"create",
		"--data",
		JSON.stringify({ displayName: name, description: marker, },),
	], { id: result => recordId(result, ["id", "dataCollectionId", "name",],), },);
	const dataset = Object.keys(ctx.fixtures.datasets,)[0];
	await withOwnedGlobal(ctx, "data-collection", entry, async id => {
		await assertListed(ctx, "data-collection", id,);
		await ctx.run(["data-collection", "get", id,],);
		if (dataset) {
			await ctx.run([
				"data-collection",
				"add-object",
				id,
				"--data",
				JSON.stringify({ type: "DATASET", projectKey: ctx.projectKey, id: dataset, },),
			],);
		}
		// settings-set replaces the whole settings object; fetch-merge-put with a
		// CHANGED marker-bearing description, then read-back verification.
		const settings = await ctx.run<JsonRecord>(["data-collection", "get", id,],);
		const updatedDescription = `${ctx.markerFor("data-collection", entry.name,)} updated`;
		await ctx.run([
			"data-collection",
			"settings-set",
			id,
			"--data",
			JSON.stringify({ ...settings, description: updatedDescription, },),
		],);
		const updated = await ctx.run<JsonRecord>(["data-collection", "get", id,],);
		if (asString(updated["description"],) !== updatedDescription) {
			throw new Error("data-collection settings-set did not persist the changed description",);
		}
		const objects = await ctx.run<Array<JsonRecord>>(["data-collection", "list-objects", id,],);
		if (dataset && !objects.some(object => asString(object["id"],) === dataset)) {
			throw new Error("data-collection add-object did not persist the dataset reference",);
		}
		if (dataset) {
			await ctx.run([
				"data-collection",
				"remove-dataset",
				id,
				ctx.projectKey,
				dataset,
			],);
			const after = await ctx.run<Array<JsonRecord>>(["data-collection", "list-objects", id,],);
			if (after.some(object => asString(object["id"],) === dataset)) {
				throw new Error("data-collection remove-dataset did not remove the dataset reference",);
			}
		}
	},);
}

async function projectFolderLifecycle(ctx: LiveContext,): Promise<void> {
	const root = await ctx.run<JsonRecord>(["project-folder", "root",],);
	const rootId = asString(root["id"],) ?? asString(root["folderId"],);
	if (!rootId) throw new Error("Project-folder root did not expose an id",);
	const entry = await ctx.createGlobal("project-folder", "folder", name => [
		"project-folder",
		"create-child",
		rootId,
		"--name",
		name,
	], { id: result => recordId(result, ["id", "folderId",],), },);
	await withOwnedGlobal(ctx, "project-folder", entry, async id => {
		// There is no project-folder list action; existence is proven by the
		// root folder's childrenIds carrying the created id.
		const rootAfterCreate = await ctx.run<JsonRecord>(["project-folder", "root",],);
		const rootChildren = rootAfterCreate["childrenIds"];
		if (!Array.isArray(rootChildren,) || !rootChildren.some(childId => childId === id)) {
			throw new Error(`project-folder ${id} missing from the root folder childrenIds`,);
		}
		ctx.fixtures.projectFolderId = id;
		await ctx.run(["project-folder", "get", id,],);
		// Documented settings change without ACL risk: rename the folder to its
		// existing nonce-bearing name plus a short suffix (the full nonce stays
		// a substring of the new name), verify via read-back, then restore the
		// original settings exactly. The guard's folder rule verifies the
		// generated id plus the nonce preserved inside the name.
		const current = await ctx.run<JsonRecord>(["project-folder", "settings-get", id,],);
		const originalName = asString(current["name"],);
		if (originalName === undefined) {
			throw new Error("project-folder settings-get did not expose the folder name",);
		}
		const renamedValue = `${originalName} renamed`;
		await ctx.run([
			"project-folder",
			"settings-set",
			id,
			"--data",
			JSON.stringify({ ...current, name: renamedValue, },),
		],);
		const after = await ctx.run<JsonRecord>(["project-folder", "settings-get", id,],);
		if (asString(after["name"],) !== renamedValue) {
			throw new Error("project-folder settings-set did not persist the renamed value",);
		}
		await ctx.run([
			"project-folder",
			"settings-set",
			id,
			"--data",
			JSON.stringify(current,),
		],);
		const restored = await ctx.run<JsonRecord>(["project-folder", "settings-get", id,],);
		if (asString(restored["name"],) !== originalName) {
			throw new Error("project-folder settings-set did not restore the original name",);
		}
		// Real folder move: create a second owned child, move it INTO this
		// folder, verify the child appears in this folder's childrenIds, then
		// remove it (empty-folder delete) before this folder's own teardown.
		const rootForMove = await ctx.run<JsonRecord>(["project-folder", "root",],);
		const rootForMoveId = asString(rootForMove["id"],) ?? asString(rootForMove["folderId"],);
		if (!rootForMoveId) throw new Error("Project-folder root did not expose an id",);
		const child = await ctx.createGlobal("project-folder", "movechild", name => [
			"project-folder",
			"create-child",
			rootForMoveId,
			"--name",
			name,
		], { id: result => recordId(result, ["id", "folderId",],), },);
		await withOwnedGlobal(ctx, "project-folder", child, async childId => {
			await ctx.run(["project-folder", "move", childId, id,],);
			const parent = await ctx.run<JsonRecord>(["project-folder", "get", id,],);
			const children = parent["childrenIds"];
			if (!Array.isArray(children,) || !children.some(entryId => entryId === childId)) {
				throw new Error("project-folder move did not list the child under its new parent",);
			}
		},);
	},);
}

async function connectionLifecycle(ctx: LiveContext,): Promise<void> {
	// Filesystem covers storage settings; SQL covers the type-limited test endpoint.
	await withOwnedFilesystemConnection(ctx, "connection", async connectionName => {
		const before = await ctx.run<JsonRecord>(["connection", "get", connectionName,],);
		const params = asRecord(before["params"],);
		const root = asString(params?.["root"],);
		if (root === undefined || !root.startsWith("/",)) {
			throw new Error("connection get did not expose the absolute Filesystem root",);
		}
		await ctx.run([
			"connection",
			"update",
			connectionName,
			"--data",
			JSON.stringify({
				name: connectionName,
				type: "Filesystem",
				usableBy: "ALLOWED",
				allowedGroups: [],
				allowWrite: false,
				allowManagedDatasets: true,
				allowManagedFolders: false,
				params: { ...params, root, },
			},),
		],);
		const after = await ctx.run<JsonRecord>(["connection", "get", connectionName,],);
		if (
			after["allowWrite"] !== false
			|| asString(asRecord(after["params"],)?.["root"],) !== root
		) {
			throw new Error("connection update did not persist the changed Filesystem settings",);
		}
	},);
	await withOwnedSqlConnection(ctx, "connection_test", async connectionName => {
		const tested = await ctx.run<JsonRecord>(["connection", "test", connectionName,],);
		if (tested["connectionOK"] !== true) {
			throw new Error("Owned SQL connection test did not report success",);
		}
	},);
}

/**
 * Owned Filesystem connection: reserves a ledger identity, creates the
 * connection through the real admin CLI, materializes the marker-bearing
 * absolute root inside this case's owned host directory, and deletes the
 * connection before the host directory itself is removed. Callers must delete
 * every project/dataset that uses the connection before returning from body —
 * the wrapper's teardown order is body → connection delete → host dir removal.
 */
export async function withOwnedFilesystemConnection<T,>(
	ctx: LiveContext,
	label: string,
	body: (connectionName: string, directory: HostDirectory,) => Promise<T>,
): Promise<T> {
	return await ctx.withOwnedHostDirectory(label, async directory => {
		let root = "";
		const entry = await ctx.createGlobal("connection", label, (name, marker,) => {
			// Filesystem identity lives in params.root: absolute and marker-bearing
			// (globalMarkerVerified), under this case's owned host directory.
			root = `${directory.path}/${marker}`;
			return [
				"connection",
				"create",
				"--data",
				JSON.stringify({
					name,
					type: "Filesystem",
					usableBy: "ALLOWED",
					allowedGroups: [],
					allowWrite: true,
					allowManagedDatasets: true,
					allowManagedFolders: false,
					params: { root, },
				},),
			];
		},);
		const bound = requireBound(entry, "connection",);
		try {
			// Materialize the root on the DSS host (the same code-run path the SQL
			// catalog fixture uses) so managed datasets can write
			// under it. The whole tree is removed with the owned host directory.
			const script = await ctx.writeFile(
				`fs-root-${entry.name}.py`,
				[
					"import json, os, subprocess",
					`root = json.loads(${JSON.stringify(JSON.stringify(root,),)})`,
					"os.makedirs(root, mode=0o700)",
					// Code execution and DSS jobs use different Unix users. Inherit both
					// grants so either can write and clean up, without granting others access.
					'uids = {os.getuid(), os.stat(os.environ["DIP_HOME"]).st_uid}',
					'entries = ["u::rwx", "g::---", "m::rwx", "o::---"] + ["u:%s:rwx" % uid for uid in sorted(uids)]',
					'subprocess.run(["setfacl", "-m", ",".join(entries + ["d:" + entry for entry in entries]), root], check=True)',
				].join("\n",),
			);
			const created = await ctx.run<{ success: boolean; }>([
				"code",
				"run",
				"--file",
				script,
				"--timeout",
				"120000",
			], { projectKey: directory.projectKey, },);
			if (!created.success) throw new Error("Filesystem connection root was not created",);
			await assertListed(ctx, "connection", bound.id!,);
			return await body(bound.id!, directory,);
		} finally {
			await ctx.deleteGlobal("connection", bound.id!,);
		}
	},);
}
/** Disposable SQLite: memory for simple queries, a seeded owned file for catalog imports. */
export async function withOwnedSqlConnection<T,>(
	ctx: LiveContext,
	label: string,
	body: (connectionName: string,) => Promise<T>,
	options: { persistent?: boolean; } = {},
): Promise<T> {
	const run = async (directory?: { path: string; projectKey: string; },): Promise<T> => {
		let database = "";
		const entry = await ctx.createGlobal("connection", label, (name, marker,) => {
			database = directory ? `${directory.path}/${marker}.sqlite` : "";
			return [
				"connection",
				"create",
				"--data",
				JSON.stringify({
					name,
					type: "JDBC",
					usableBy: "ALLOWED",
					allowedGroups: [],
					allowWrite: false,
					allowManagedDatasets: false,
					params: {
						driver: "org.sqlite.JDBC",
						jdbcurl: directory
							? `jdbc:sqlite:file:${database}?mode=ro`
							: `jdbc:sqlite:file:${marker}?mode=memory&cache=shared`,
						properties: [],
					},
				},),
			];
		},);
		requireBound(entry, "connection",);
		try {
			if (directory) {
				const script = await ctx.writeFile(
					`sql-catalog-${entry.name}.py`,
					[
						"import json, os, sqlite3",
						`database = json.loads(${JSON.stringify(JSON.stringify(database,),)})`,
						"fd = os.open(database, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)",
						"os.close(fd)",
						"with sqlite3.connect(database) as connection:",
						"    connection.execute('CREATE TABLE live_fixture (id INTEGER PRIMARY KEY, value TEXT)')",
						"    connection.execute(\"INSERT INTO live_fixture VALUES (1, 'owned')\")",
						"connection.close()",
						"os.chmod(database, 0o644)",
					].join("\n",),
				);
				const result = await ctx.run<{ success: boolean; }>([
					"code",
					"run",
					"--file",
					script,
					"--timeout",
					"120000",
				], { projectKey: directory.projectKey, },);
				if (!result.success) throw new Error("SQLite catalog fixture creation failed",);
			}
			return await body(entry.id!,);
		} finally {
			await ctx.deleteGlobal("connection", entry.id!,);
		}
	};
	return options.persistent ? ctx.withOwnedHostDirectory(label, run,) : run();
}

export interface OwnedCodeEnvHandle {
	envLang: string;
	envName: string;
}

export interface OwnedCodeEnvOptions {
	/** Extra package specs installed (real build awaited) before body runs. */
	packages?: readonly string[];
	/** Enable Jupyter support on the created env (MLflow runtime needs true). */
	installJupyterSupport?: boolean;
	/**
	 * Pin the DSS core packages set (e.g. "PANDAS22") on the env definition
	 * before any requested packages resolve, so a pinned spec such as
	 * pandas==2.2.3 never conflicts with the server-default core set.
	 * Omitted → definition untouched, exactly as before.
	 */
	corePackagesSet?: string;
}

/**
 * Canonical owned code-env provisioning shared by live modules (MLflow runtime
 * consumers included). Creation is asynchronous on DSS: reserve → CLI create
 * (--no-wait) → persist the receipt's jobId via ctx.recordFuture immediately →
 * terminal-wait the build future → only then bind. A failed wait that cannot
 * be proven terminal preserves the reservation (never deleted mid-build).
 * Optional `packages` are set and really built (set-packages merge + awaited
 * update-packages future) with requestedPackages read-back verification.
 * Optional `corePackagesSet` (e.g. PANDAS22) is merged into the definition's
 * desc first, via the get-definition → set-definition --expect-hash path with
 * desc read-back, so pip resolution never fights the server-default core set.
 * Teardown always deletes the bound env in finally; callers never mutate
 * foreign environments.
 *
 * The lifecycle case builds on this: real design-managed creation (DSS-
 * documented body, empty spec package list, so no conda/pip resolution
 * beyond the core venv), then set-definition round-trip, set-packages
 * pin-and-restore, update-packages reconcile, and update-images where DSS
 * accepts it — each awaited before the next mutator.
 */
export async function withOwnedCodeEnv<T,>(
	ctx: LiveContext,
	label: string,
	options: OwnedCodeEnvOptions,
	body: (env: OwnedCodeEnvHandle,) => Promise<T>,
): Promise<T> {
	// Mirror an existing design-managed env's interpreter instead of assuming
	// one: the live inventory (e.g. default_v1) proves which interpreter DSS
	// actually supports on this instance.
	const template = await ctx.run<{ pythonInterpreter?: string; deploymentMode?: string; }>([
		"code-env",
		"get",
		"PYTHON",
		"default_v1",
	],);
	if (template.deploymentMode !== "DESIGN_MANAGED") {
		throw new LiveCapabilityError(
			`no design-managed template env found to mirror (default_v1 reports ${
				template.deploymentMode ?? "unknown"
			})`,
		);
	}
	const name = await ctx.reserveGlobal("code-env", label,);
	const created = await ctx.run<JsonRecord>([
		"code-env",
		"create",
		"PYTHON",
		name,
		"--deployment-mode",
		"DESIGN_MANAGED",
		"--params",
		JSON.stringify({
			...(template.pythonInterpreter !== undefined
				? { pythonInterpreter: template.pythonInterpreter, }
				: {}),
			installCorePackages: true,
			installJupyterSupport: options.installJupyterSupport === true,
		},),
		"--no-wait",
	],);
	const jobId = asString(created["jobId"],);
	if (jobId === undefined) {
		throw new LiveCapabilityError(
			`code-env create receipt carried no future jobId; reservation ${name} left pending`,
		);
	}
	// Persist the receipt immediately so the future is abortable and durable
	// across interruptions.
	await ctx.recordFuture(jobId,);
	// The creation build must reach a terminal, successful state before any
	// mutation or deletion. On a failed/timed-out wait, abort the owned future
	// and confirm terminality; only a proven-terminal env may be deleted.
	// Unsettled futures preserve the reservation for inspection instead of
	// deleting it mid-build (a mid-build delete can reappear as a live
	// artifact).
	let waitError: unknown;
	try {
		const waited = await ctx.run<FutureWaitResult>([
			"future",
			"wait",
			jobId,
			"--timeout",
			String(CODE_ENV_WAIT_TIMEOUT_MS,),
		],);
		if (waited.timedOut === true || waited.success !== true) {
			waitError = new LiveCapabilityError(
				`code-env create future ${jobId} ended ${waited.state ?? "unknown"} without success`,
			);
		}
	} catch (error) {
		waitError = error;
	}
	if (waitError !== undefined) {
		const terminal = await settleOwnedFuture(ctx, jobId,);
		if (!terminal) {
			throw new LiveCapabilityError(
				`code-env create future ${jobId} could not be confirmed terminal after a failed wait; reservation ${name} preserved for inspection instead of deleted mid-build: ${
					waitError instanceof Error ? waitError.message : String(waitError,)
				}`,
			);
		}
		// Proven terminal: bind only if the server actually exposes the env,
		// then delete that bound env. A missing env leaves the reservation
		// pending (never deleted blind).
		const failed = await ctx.bindGlobal("code-env", name, `${"PYTHON"}/${name}`,).catch(() =>
			undefined
		);
		if (failed !== undefined && failed.state === "bound") {
			await ctx.deleteGlobal("code-env", failed.id!,).catch(() => undefined);
		}
		throw waitError;
	}
	const entry = await ctx.bindGlobal("code-env", name, `${"PYTHON"}/${name}`,);
	requireBound(entry, "code-env",);
	const handle: OwnedCodeEnvHandle = { envLang: "PYTHON", envName: name, };
	return await withOwnedGlobal(ctx, "code-env", entry, async () => {
		// Core packages set (e.g. PANDAS22) is merged into the definition's
		// desc BEFORE requested packages resolve, via the same get-definition →
		// set-definition --expect-hash round-trip the lifecycle case uses (the
		// SDK schemas do not model this DSS field, so the creation params are
		// not an option). The read-back proves the server persisted it.
		if (options.corePackagesSet !== undefined) {
			const details = await ctx.run<{ definitionHash?: string; }>([
				"code-env",
				"get",
				handle.envLang,
				handle.envName,
			],);
			const definition = await ctx.run<JsonRecord>([
				"code-env",
				"get-definition",
				handle.envLang,
				handle.envName,
			],);
			const desc = {
				...asRecord(definition["desc"],),
				corePackagesSet: options.corePackagesSet,
			};
			const updated = await ctx.run<JsonRecord>([
				"code-env",
				"set-definition",
				handle.envLang,
				handle.envName,
				"--data",
				JSON.stringify({ ...definition, desc, },),
				...(details.definitionHash ? ["--expect-hash", details.definitionHash,] : []),
			],);
			await awaitMutationFuture(ctx, updated, `${label} set-definition (corePackagesSet)`,);
			const readback = await ctx.run<JsonRecord>([
				"code-env",
				"get-definition",
				handle.envLang,
				handle.envName,
			],);
			if (
				asString(asRecord(readback["desc"],)?.["corePackagesSet"],) !== options.corePackagesSet
			) {
				throw new Error(
					`code-env ${handle.envName} corePackagesSet did not persist ${options.corePackagesSet}`,
				);
			}
		}
		if (options.packages !== undefined && options.packages.length > 0) {
			const requested = options.packages.join("\n",);
			const set = await ctx.run<JsonRecord>([
				"code-env",
				"set-packages",
				handle.envLang,
				handle.envName,
				"--packages",
				requested,
			],);
			await awaitMutationFuture(ctx, set, `${label} set-packages`,);
			const update = await ctx.run<JsonRecord>([
				"code-env",
				"update-packages",
				handle.envLang,
				handle.envName,
				"--no-wait",
			],);
			await awaitMutationFuture(ctx, update, `${label} update-packages`, { expectFuture: true, },);
			const after = await ctx.run<{ requestedPackages: string[]; }>([
				"code-env",
				"get",
				handle.envLang,
				handle.envName,
			],);
			for (const spec of options.packages) {
				const base = spec.split("=",)[0] ?? spec;
				if (
					!after.requestedPackages.some(line => line === spec || line.split("=",)[0] === base)
				) {
					throw new Error(
						`code-env ${handle.envName} requested packages missing ${spec} after the build`,
					);
				}
			}
		}
		return await body(handle,);
	},);
}

async function codeEnvLifecycle(ctx: LiveContext,): Promise<void> {
	await withOwnedCodeEnv(ctx, "env", {}, async ({ envLang, envName, },) => {
		const lang = envLang;
		ctx.fixtures.codeEnvName = envName;
		await assertListed(ctx, "code-env", `${lang}/${envName}`,);
		await ctx.run(["code-env", "get", lang, envName,],);
		await ctx.run(["code-env", "get-definition", lang, envName,],);
		// set-definition round-trip: replace the definition with itself under the
		// hash captured from code-env get; the pre-flight compare refuses the
		// PUT when DSS changed the definition before the hash was captured
		// (best-effort compare-and-write, not an atomic CAS — a change landing
		// between that GET and the PUT is not covered).
		const details = await ctx.run<{ definitionHash?: string; }>(["code-env", "get", lang, envName,],);
		const definition = await ctx.run<JsonRecord>(["code-env", "get-definition", lang, envName,],);
		// set-definition is synchronous (async: none, no --no-wait flag): the
		// PUT returns no future, so no wait is possible or needed here.
		const defined = await ctx.run<JsonRecord>([
			"code-env",
			"set-definition",
			lang,
			envName,
			"--data",
			JSON.stringify(definition,),
			...(details.definitionHash ? ["--expect-hash", details.definitionHash,] : []),
		],);
		await awaitMutationFuture(ctx, defined, "code-env set-definition",);
		// set-packages for real: pin an installed package at its current version
		// (spec == actual, so the build is a no-op reconcile), verify, then restore.
		// Also synchronous (async: none): merge-PUT with no future.
		const got = await ctx.run<{ installedPackages: string[]; requestedPackages: string[]; }>([
			"code-env",
			"get",
			lang,
			envName,
		],);
		const pinned = got.installedPackages[0];
		if (pinned !== undefined) {
			const pinnedResult = await ctx.run<JsonRecord>([
				"code-env",
				"set-packages",
				lang,
				envName,
				"--packages",
				pinned,
			],);
			await awaitMutationFuture(ctx, pinnedResult, "code-env set-packages",);
			const afterPin = await ctx.run<{ requestedPackages: string[]; }>([
				"code-env",
				"get",
				lang,
				envName,
			],);
			if (!afterPin.requestedPackages.includes(pinned,)) {
				throw new Error(`code-env set-packages did not record the pinned package ${pinned}`,);
			}
			const restoredResult = await ctx.run<JsonRecord>([
				"code-env",
				"set-packages",
				lang,
				envName,
				"--packages",
				got.requestedPackages.join("\n",) || "",
			],);
			await awaitMutationFuture(ctx, restoredResult, "code-env set-packages restore",);
		}
		// Real reconcile with the restored (satisfied) spec; --no-wait returns
		// the build future, which is recorded and terminal-awaited before the
		// next mutation.
		const updated = await ctx.run<JsonRecord>([
			"code-env",
			"update-packages",
			lang,
			envName,
			"--no-wait",
		],);
		await awaitMutationFuture(ctx, updated, "code-env update-packages", {
			expectFuture: true,
			timeoutMs: CODE_ENV_MUTATION_WAIT_TIMEOUT_MS,
		},);
		await ctx.run(["code-env", "get", lang, envName,],);
		// Real image reconcile: DSS itself refuses on a non-Docker env, which is
		// the honest observed result for this env kind. A successful call's
		// future is recorded and terminal-awaited; an unconfirmed future
		// preserves the env instead of deleting it mid-build.
		let images: JsonRecord;
		try {
			images = await ctx.run<JsonRecord>([
				"code-env",
				"update-images",
				lang,
				envName,
				"--no-wait",
			],);
		} catch (error) {
			throw new LiveCapabilityError(
				`code-env update-images refused for this env kind (DSS's own response): ${
					error instanceof Error ? error.message : String(error,)
				}`,
			);
		}
		await awaitMutationFuture(ctx, images, "code-env update-images", {
			expectFuture: true,
			timeoutMs: CODE_ENV_MUTATION_WAIT_TIMEOUT_MS,
		},);
		const jupyter = await ctx.run<JsonRecord>([
			"code-env",
			"set-jupyter",
			lang,
			envName,
			"--active",
			"false",
			"--no-wait",
		],);
		await awaitMutationFuture(ctx, jupyter, "code-env set-jupyter", {
			expectFuture: true,
			timeoutMs: CODE_ENV_MUTATION_WAIT_TIMEOUT_MS,
		},);
	},);
}

/**
 * External identity surfaces: enumeration futures plus the sync verbs,
 * attempted against a disposable OWNED user so the outcome is DSS's real
 * answer. These operations need a lab-owned or explicitly authorized external
 * identity supplier, which this run does not have; the refusal (sandbox or
 * DSS) is reported as the per-action prerequisite block — never an assumed
 * block, never a silent skip.
 */
async function userExternalSync(ctx: LiveContext,): Promise<void> {
	await attemptOrBlock(
		ctx,
		["user", "external-users",],
		"user external-users",
		"it enumerates users from an external identity supplier, and this run has no lab-owned or explicitly authorized supplier",
	);
	await attemptOrBlock(
		ctx,
		["user", "external-groups",],
		"user external-groups",
		"it enumerates groups from an external identity supplier, and this run has no lab-owned or explicitly authorized supplier",
	);
	const password = `L!${randomBytes(18,).toString("base64url",)}aB9`;
	const entry = await ctx.createGlobal("user", "extsync", (name, marker,) => [
		"user",
		"create",
		"--data",
		JSON.stringify({
			login: name,
			sourceType: "LOCAL",
			displayName: `Live lab extsync ${ctx.iteration}`,
			email: `${marker}@sdk-live.invalid`,
			groups: [],
			userProfile: "DATA_ANALYST",
			password,
		},),
	],);
	await withOwnedGlobal(ctx, "user", entry, async login => {
		const resynced = await attemptOrBlock<JsonRecord>(
			ctx,
			["user", "resync", login,],
			"user resync",
			"resync pulls directory state from an external identity supplier, and this run has no lab-owned or explicitly authorized supplier",
		);
		if (resynced !== undefined) {
			await awaitMutationFuture(ctx, resynced, "user resync", { expectFuture: true, },);
		}
		const resyncedMulti = await attemptOrBlock<JsonRecord>(
			ctx,
			["user", "resync-multi", "--logins", login,],
			"user resync-multi",
			"resync pulls directory state from an external identity supplier, and this run has no lab-owned or explicitly authorized supplier",
		);
		if (resyncedMulti !== undefined) {
			await awaitMutationFuture(ctx, resyncedMulti, "user resync-multi", { expectFuture: true, },);
		}
		const provisioned = await attemptOrBlock<JsonRecord>(
			ctx,
			[
				"user",
				"provision",
				"--data",
				JSON.stringify({ userSourceType: "AZURE_AD", users: [], },),
			],
			"user provision",
			"provisioning writes users sourced from an external identity supplier, and this run has no lab-owned or explicitly authorized supplier",
		);
		if (provisioned !== undefined) {
			await awaitMutationFuture(ctx, provisioned, "user provision", { expectFuture: true, },);
		}
	},);
}

/** Read-only activity surfaces for the admin user set. */
async function userActivity(ctx: LiveContext,): Promise<void> {
	await ctx.run(["user", "activity",],);
	const self = await ctx.run<JsonRecord>(["user", "get", ctx.owner,],);
	if (asString(self["login"],) === undefined) {
		throw new Error(`user get ${ctx.owner} did not return a login`,);
	}
	await ctx.run(["user", "activity-get", ctx.owner,],);
}

/**
 * Move a lab-created disposable project between two lab-owned child folders.
 * move-project <folderId> <projectKey> <destinationFolderId>: the source
 * folder is where DSS placed the fresh project (root), then the project moves
 * owned folder A → owned folder B, verified through folder B's projectKeys.
 * Cleanup deletes the project first, then both (now-empty) owned folders.
 */
async function projectFolderMoveProject(ctx: LiveContext,): Promise<void> {
	const root = await ctx.run<JsonRecord>(["project-folder", "root",],);
	const rootId = asString(root["id"],) ?? asString(root["folderId"],);
	if (!rootId) throw new Error("Project-folder root did not expose an id",);
	const source = await ctx.createGlobal("project-folder", "movesrc", name => [
		"project-folder",
		"create-child",
		rootId,
		"--name",
		name,
	], { id: result => recordId(result, ["id", "folderId",],), },);
	const destination = await ctx.createGlobal("project-folder", "movedest", name => [
		"project-folder",
		"create-child",
		rootId,
		"--name",
		name,
	], { id: result => recordId(result, ["id", "folderId",],), },);
	const project = await ctx.createProject("moveable",);
	let bodyError: unknown;
	try {
		// Fresh projects land at the root; move it into the owned source folder,
		// then between the two owned folders.
		await ctx.run(["project-folder", "move-project", rootId, project, source.id!,],);
		await ctx.run(["project-folder", "move-project", source.id!, project, destination.id!,],);
		const landed = await ctx.run<JsonRecord>(["project-folder", "get", destination.id!,],);
		const projectKeys = landed["projectKeys"];
		if (
			!Array.isArray(projectKeys,)
			|| !projectKeys.some(key => key === project)
		) throw new Error(`move-project did not land ${project} in the destination folder`,);
	} catch (error) {
		bodyError = error;
	}
	const errors: unknown[] = [];
	try {
		await ctx.deleteProject(project,);
	} catch (error) {
		errors.push(error,);
	}
	for (const folder of [destination, source,]) {
		try {
			await ctx.deleteGlobal("project-folder", folder.id!,);
		} catch (error) {
			errors.push(error,);
		}
	}
	if (bodyError !== undefined) errors.unshift(bodyError,);
	if (errors.length === 1) throw errors[0];
	if (errors.length) {
		throw new AggregateError(
			errors,
			errors.map(e => e instanceof Error ? e.message : String(e,)).join("; ",),
		);
	}
}

export async function exerciseInfrastructureDisposable(ctx: LiveContext,): Promise<void> {
	await ctx.check(
		"infrastructure.user" as LiveCaseId,
		["user.create", "user.list", "user.get", "user.update", "user.activity-get", "user.delete",],
		async () => {
			await userLifecycle(ctx,);
		},
		// Global user administration may be permission-denied for this API key;
		// a real 401/403 is recorded as the exact blocked capability, while a
		// successful lifecycle still passes and is required to be correct.
		{ capability: "infrastructure.user-crud", required: false, },
	);
	await ctx.check(
		"infrastructure.group" as LiveCaseId,
		["group.create", "group.list", "group.get", "group.update", "group.delete",],
		async () => {
			await groupLifecycle(ctx,);
		},
		// Same contract as users: 401/403 → blocked; success path must pass.
		{ capability: "infrastructure.group-crud", required: false, },
	);
	await ctx.check(
		"infrastructure.meaning" as LiveCaseId,
		["meaning.create", "meaning.list", "meaning.get", "meaning.update", "meaning.delete",],
		async () => {
			await meaningLifecycle(ctx,);
		},
		{ capability: "infrastructure.meaning-crud", required: false, },
	);
	await ctx.check(
		"infrastructure.workspace" as LiveCaseId,
		[
			"workspace.create",
			"workspace.list",
			"workspace.get",
			"workspace.add-object",
			"workspace.update-settings",
			"workspace.list-objects",
			"workspace.delete",
		],
		async () => {
			await workspaceLifecycle(ctx,);
		},
		{ capability: "infrastructure.workspace-crud", },
	);
	await ctx.check(
		"infrastructure.data-collection" as LiveCaseId,
		[
			"data-collection.create",
			"data-collection.list",
			"data-collection.get",
			"data-collection.add-object",
			"data-collection.remove-dataset",
			"data-collection.settings-set",
			"data-collection.list-objects",
			"data-collection.delete",
		],
		async () => {
			await dataCollectionLifecycle(ctx,);
		},
		{ capability: "infrastructure.data-collection-crud", },
	);
	await ctx.check(
		"infrastructure.project-folder" as LiveCaseId,
		[
			"project-folder.root",
			"project-folder.create-child",
			"project-folder.get",
			"project-folder.settings-set",
			"project-folder.move",
			"project-folder.delete",
		],
		async () => {
			await projectFolderLifecycle(ctx,);
		},
		{ capability: "infrastructure.project-folder-crud", },
	);
	await ctx.check(
		"infrastructure.connection" as LiveCaseId,
		[
			"connection.create",
			"connection.list",
			"connection.get",
			"connection.test",
			"connection.update",
			"connection.delete",
		],
		async () => {
			await connectionLifecycle(ctx,);
		},
		{ capability: "infrastructure.connection-crud", },
	);
	await ctx.check(
		"infrastructure.code-env" as LiveCaseId,
		[
			"code-env.create",
			"code-env.list",
			"code-env.get",
			"code-env.get-definition",
			"code-env.set-definition",
			"code-env.set-packages",
			"code-env.update-packages",
			"code-env.update-images",
			"code-env.set-jupyter",
			"code-env.delete",
		],
		async () => {
			await codeEnvLifecycle(ctx,);
		},
		{ capability: "infrastructure.code-env-lifecycle", },
	);
	await ctx.check(
		"infrastructure.user.external-sync" as LiveCaseId,
		[
			"user.external-users",
			"user.external-groups",
			"user.create",
			"user.resync",
			"user.resync-multi",
			"user.provision",
			"user.delete",
		],
		async () => {
			await userExternalSync(ctx,);
		},
		{ capability: "infrastructure.user-external", required: false, },
	);
	await ctx.check(
		"infrastructure.user.activity" as LiveCaseId,
		["user.activity", "user.get", "user.activity-get",],
		async () => {
			await userActivity(ctx,);
		},
		{ capability: "infrastructure.user-activity", required: false, },
	);
	await ctx.check(
		"infrastructure.project-folder.move-project" as LiveCaseId,
		[
			"project-folder.root",
			"project-folder.create-child",
			"project.create",
			"project.delete",
			"project-folder.move-project",
			"project-folder.delete",
		],
		async () => {
			await projectFolderMoveProject(ctx,);
		},
		{ capability: "infrastructure.project-folder-move", required: false, },
	);
}
