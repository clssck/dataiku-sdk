// Plugin lifecycle for the infrastructure profile: every case drives real CLI
// verbs against a plugin this run created through a receipt-gated helper
// (ctx.createPluginDev for EMPTY dev plugins, ctx.installPluginFromZip for a
// locally built archive), then deletes it in finally through ctx.deleteGlobal
// — including on assertion failures. Genuinely external prerequisites (a
// lab-owned Git remote, a store-published plugin) are separate cases reported
// as explicit LiveCapabilityError blockers, never successes, never silent
// skips. Case ids are registered by Main; nothing here runs without the
// serialized live-suite dispatcher.
import { readFile, } from "node:fs/promises";
import { join, } from "node:path";
import { crc32, } from "node:zlib";
import type { LiveContext, OwnedGlobal, } from "./live-context.js";
import { LiveCapabilityError, LiveCommandError, } from "./live-context.js";

type JsonRecord = Record<string, unknown>;

/**
 * Operator-designated Git remote owned by the lab and reachable from the DSS
 * host (SSH or credential-free HTTPS; the CLI rejects embedded credentials).
 * Only with it do the remote-sync actions run; never a discovered remote.
 */
const PLUGIN_GIT_REMOTE_ENV = "DATAIKU_LIVE_PLUGIN_GIT_REMOTE";
/** Design-managed env mirrored for the plugin code-env interpreter. */
const TEMPLATE_CODE_ENV = "default_v1";
const CODE_ENV_TIMEOUT_MS = 240_000;

function asRecord(value: unknown,): JsonRecord | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value,)
		? value as JsonRecord
		: undefined;
}

function asString(value: unknown,): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

function blocked(message: string,): never {
	throw new LiveCapabilityError(message, "blocked",);
}

function dssRefusal(subject: string, error: unknown,): never {
	blocked(
		`${subject} refused by DSS itself (its own response): ${
			error instanceof Error ? error.message : String(error,)
		}`,
	);
}

/**
 * The plugin's server-side outcome is unknown (5xx, transport failure, or a
 * --wait that never settled) and a build may still be running. Carrying this
 * error through cleanup makes the owned plugin be preserved for explicit
 * operator collection instead of deleted under a possibly-live build.
 */
class AmbiguousMutationError extends LiveCapabilityError {}

/**
 * True when a failed command may still have been delivered and processed by
 * DSS. CLI usage errors (exit 1) and structured 4xx refusals are
 * deterministic — DSS answered and no mutation is pending; every other
 * failure (5xx, transient, transport, killed) leaves the outcome unknown.
 */
function isAmbiguousMutationFailure(error: unknown,): boolean {
	if (!(error instanceof LiveCommandError)) return false;
	if (error.exitCode === 1) return false;
	const status = asRecord(error.result,)?.["status"];
	if (typeof status === "number") return status >= 500;
	return true;
}

function codeEnvHold(pluginId: string, why: string, detail: unknown,): AmbiguousMutationError {
	const summary = (detail instanceof Error ? detail.message : JSON.stringify(detail,)) ?? "";
	return new AmbiguousMutationError(
		`${why}; a plugin code-env build may still be running, so the owned plugin ${pluginId} is preserved (not deleted) for explicit operator collection: ${
			summary.slice(0, 400,)
		}`,
	);
}

/**
 * Run a plugin code-env lifecycle command that builds asynchronously. The
 * step is only complete when the CLI exited 0 AND the awaited future settled
 * (`success` true); the settled future's `result` is returned (the
 * documentated create receipt carries `envName`, per the official client's
 * `future.wait_for_result()` usage). A terminal-but-not-successful future
 * (ABORTED/FAILED) is a deterministic failure — no build is running, so
 * normal cleanup may proceed. An unsettled or unknown outcome (unsettled
 * --wait, 5xx, transport failure) holds the plugin instead: a build may
 * still be running, so cleanup must not delete it.
 */
async function runCodeEnvBuild(
	ctx: LiveContext,
	pluginId: string,
	argv: string[],
): Promise<Record<string, unknown> | undefined> {
	let result: unknown;
	try {
		result = await ctx.run<unknown>(argv,);
	} catch (error) {
		if (!isAmbiguousMutationFailure(error,)) throw error;
		throw codeEnvHold(
			pluginId,
			`${argv.slice(0, 2,).join(" ",)} failed with an unknown outcome`,
			error,
		);
	}
	const record = asRecord(result,);
	if (record?.["success"] === true) return asRecord(record["result"],);
	const state = record?.["state"];
	if (state === "ABORTED" || state === "FAILED") {
		throw new Error(
			`plugin ${
				argv[1]
			} build ended in state ${state}; the future is terminal and no build is running`,
		);
	}
	throw codeEnvHold(
		pluginId,
		`${argv.slice(0, 2,).join(" ",)} --wait did not report a settled build`,
		result,
	);
}

/**
 * The managed env's real deployment contract: read the definition back and
 * require the plugin-managed deployment mode with the mirrored interpreter.
 * The plugin settings surface does not carry this association, so the env
 * definition itself is the authoritative check.
 */
async function assertManagedEnvDefinition(
	ctx: LiveContext,
	envName: string,
	interpreter: string,
): Promise<void> {
	const definition = await ctx.run<JsonRecord>(["code-env", "get-definition", "PYTHON", envName,],);
	const desc = asRecord(definition["desc"],);
	if (desc?.["deploymentMode"] !== "PLUGIN_MANAGED" || desc["pythonInterpreter"] !== interpreter) {
		throw new Error(
			`managed env ${envName} definition mismatch (expected PLUGIN_MANAGED/${interpreter}): ${
				JSON.stringify(definition,)
			}`,
		);
	}
}

/** A create is only real when the ledger holds a bound, identity-matched entry. */
function requireBound(entry: OwnedGlobal | undefined,): OwnedGlobal & { id: string; } {
	if (!entry || entry.state !== "bound" || !entry.id) {
		throw new LiveCapabilityError("plugin creation did not produce a bound owned-global receipt",);
	}
	return entry as OwnedGlobal & { id: string; };
}

/**
 * Guaranteed teardown for one owned plugin: runs body, then deletes the bound
 * plugin in finally — including on assertion failures. Deletion only ever
 * targets a bound, identity-matched ledger entry. The transient fixture id is
 * cleared in finally so it never points at a deleted plugin. An
 * AmbiguousMutationError from the body is the one exception: a server-side
 * build may still be running, so the plugin is preserved and reported for
 * explicit operator collection instead of being deleted under it. Errors from
 * body and teardown are both surfaced.
 */
async function withOwnedPlugin(
	ctx: LiveContext,
	entry: OwnedGlobal,
	body: (id: string,) => Promise<void>,
): Promise<void> {
	const bound = requireBound(entry,);
	const errors: unknown[] = [];
	try {
		ctx.fixtures.pluginId = bound.id;
		await body(bound.id,);
	} catch (error) {
		errors.push(error,);
	} finally {
		ctx.fixtures.pluginId = undefined;
		if (errors.some(e => e instanceof AmbiguousMutationError)) {
			process.stdout.write(
				`${
					JSON.stringify({
						note: "bound global preserved: plugin mutation outcome unknown",
						kind: "plugin",
						id: bound.id,
					},)
				}\n`,
			);
		} else {
			try {
				await ctx.deleteGlobal("plugin", bound.id,);
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
}

/**
 * plugin.json in the DSS manifest convention: `id` and `version` at the root,
 * descriptive fields under `meta`. The ownership marker is exactly
 * `meta.description` — the field the owned-global ledger verifies at bind,
 * before every mutation, and again before deletion.
 */
export function pluginJson(name: string, marker: string, version = "1.0.0",): string {
	return JSON.stringify({
		id: name,
		version,
		metaVersion: 1,
		meta: { label: "SDK live-suite disposable plugin", description: marker, },
	},);
}

/* ------------------------------------------------------------------ */
/*  Local plugin archive (stored zip, no external tools)                */
/* ------------------------------------------------------------------ */

export interface ZipMember {
	name: string;
	data: Uint8Array;
}

function u16le(value: number,): Uint8Array {
	return new Uint8Array([value & 0xff, value >>> 8 & 0xff,],);
}

function u32le(value: number,): Uint8Array {
	return new Uint8Array([
		value & 0xff,
		value >>> 8 & 0xff,
		value >>> 16 & 0xff,
		value >>> 24 & 0xff,
	],);
}

function concatBytes(...parts: Uint8Array[]): Uint8Array {
	const out = new Uint8Array(parts.reduce((sum, part,) => sum + part.length, 0,),);
	let offset = 0;
	for (const part of parts) {
		out.set(part, offset,);
		offset += part.length;
	}
	return out;
}

const SIG_LOCAL = 0x04034b50;
const SIG_CENTRAL = 0x02014b50;
const SIG_EOCD = 0x06054b50;

/**
 * Minimal stored (method 0) zip with a central directory and exact CRCs.
 * Exported as a test utility: the hermetic installPluginFromZip regression
 * fixtures (tests/live-context.test.ts) reuse this single writer instead of
 * maintaining a second archive implementation.
 */
export function buildStoredZip(members: ZipMember[],): Uint8Array {
	const encoder = new TextEncoder();
	const locals: Uint8Array[] = [];
	const centrals: Uint8Array[] = [];
	let offset = 0;
	for (const member of members) {
		const name = encoder.encode(member.name,);
		const crc = crc32(member.data,) >>> 0;
		const local = concatBytes(
			u32le(SIG_LOCAL,),
			u16le(20,), // version needed
			u16le(0,), // flags
			u16le(0,), // method: store
			u16le(0,), // mod time
			u16le(0,), // mod date
			u32le(crc,),
			u32le(member.data.length,),
			u32le(member.data.length,),
			u16le(name.length,),
			u16le(0,), // extra length
			name,
			member.data,
		);
		centrals.push(
			concatBytes(
				u32le(SIG_CENTRAL,),
				u16le(20,), // version made by
				u16le(20,), // version needed
				u16le(0,), // flags
				u16le(0,), // method: store
				u16le(0,), // mod time
				u16le(0,), // mod date
				u32le(crc,),
				u32le(member.data.length,),
				u32le(member.data.length,),
				u16le(name.length,),
				u16le(0,), // extra length
				u16le(0,), // comment length
				u16le(0,), // disk start
				u16le(0,), // internal attributes
				u32le(0,), // external attributes
				u32le(offset,),
				name,
			),
		);
		locals.push(local,);
		offset += local.length;
	}
	const central = concatBytes(...centrals,);
	return concatBytes(
		...locals,
		central,
		u32le(SIG_EOCD,),
		u16le(0,),
		u16le(0,),
		u16le(members.length,),
		u16le(members.length,),
		u32le(central.length,),
		u32le(offset,),
		u16le(0,),
	);
}

/** In-memory bytes of the valid owned plugin archive for a reservation. */
export function pluginArchiveBytes(name: string, marker: string, version = "1.0.0",): Uint8Array {
	const encoder = new TextEncoder();
	return buildStoredZip([
		{ name: `${name}/plugin.json`, data: encoder.encode(pluginJson(name, marker, version,),), },
		{
			name: `${name}/resources/README.md`,
			data: encoder.encode(`SDK live-suite disposable plugin ${name} ${version}\n`,),
		},
	],);
}

/**
 * A valid DSS plugin archive for the reserved id: a single top-level folder
 * named after the plugin holding plugin.json (id === name, marker in
 * meta.description) plus a resource file. Written under ctx.dir; the nonce
 * is the ledger marker itself, so the archive is unique to this reservation.
 * Exported as a test utility — this signature is exactly the
 * ctx.installPluginFromZip builder contract, and the hermetic regressions in
 * tests/live-context.test.ts reuse it rather than a second writer.
 */
export async function writePluginArchive(
	ctx: LiveContext,
	name: string,
	marker: string,
	version = "1.0.0",
): Promise<string> {
	return ctx.writeFile(`plugin-${name}-${version}.zip`, pluginArchiveBytes(name, marker, version,),);
}

/* ------------------------------------------------------------------ */
/*  Shared assertions                                                   */
/* ------------------------------------------------------------------ */

async function listedPlugin(ctx: LiveContext, id: string,): Promise<JsonRecord> {
	const list = await ctx.run<unknown>(["plugin", "list",],);
	const found = Array.isArray(list,)
		? list.map(asRecord,).find(record => record?.["id"] === id)
		: undefined;
	if (!found) throw new Error(`plugin ${id} missing from plugin list`,);
	return found;
}

async function contentsListPaths(ctx: LiveContext, id: string,): Promise<string[]> {
	const listed = await ctx.run<unknown>(["plugin", "contents-list", id,],);
	const paths: string[] = [];
	const walk = (node: unknown,): void => {
		if (Array.isArray(node,)) {
			for (const item of node) walk(item,);
			return;
		}
		const record = asRecord(node,);
		if (!record) return;
		const path = asString(record["path"],) ?? asString(record["fullPath"],)
			?? asString(record["name"],);
		if (path) paths.push(path.replace(/^\/+/, "",),);
		for (const key of ["children", "files", "items", "contents", "folders",]) {
			if (record[key] !== undefined) walk(record[key],);
		}
	};
	walk(listed,);
	return paths;
}

async function assertContent(
	ctx: LiveContext,
	id: string,
	path: string,
	expected: string,
): Promise<void> {
	const got = await ctx.run<{ data?: string; }>(["plugin", "contents-get", id, path,],);
	if (got.data !== expected) {
		throw new Error(`plugin ${id} ${path} roundtrip mismatch: ${JSON.stringify(got.data,)}`,);
	}
}

async function assertMarkerPresent(ctx: LiveContext, id: string, marker: string,): Promise<void> {
	const got = await ctx.run<{ data?: string; }>(["plugin", "contents-get", id, "plugin.json",],);
	const description = asString(
		asRecord(asRecord(JSON.parse(got.data ?? "{}",),)?.["meta"],)?.["description"],
	);
	if (description !== marker) {
		throw new Error(`plugin ${id} plugin.json no longer carries the ownership marker`,);
	}
}

/* ------------------------------------------------------------------ */
/*  Cases                                                               */
/* ------------------------------------------------------------------ */

/**
 * Dev-plugin content surface on an owned EMPTY plugin: settings roundtrip
 * (only settings previously obtained through settings-get are sent back, as
 * the CLI contract requires), folder/file writes verified by list + get +
 * details, rename → move → delete, and a download of the whole plugin as a
 * zip into the run directory.
 */
async function pluginLifecycle(ctx: LiveContext,): Promise<void> {
	const entry = await ctx.createPluginDev("plugin", pluginJson,);
	await withOwnedPlugin(ctx, entry, async id => {
		const listed = await listedPlugin(ctx, id,);
		if (listed["isDev"] !== true) throw new Error(`plugin ${id} is not listed as a dev plugin`,);
		await ctx.run(["plugin", "usages", id,],);
		const settings = await ctx.run<JsonRecord>(["plugin", "settings-get", id,],);
		if (!asRecord(settings,)) throw new Error(`plugin settings-get ${id} returned no object`,);
		await ctx.run(["plugin", "settings-set", id, "--content", JSON.stringify(settings,),],);
		const after = await ctx.run<JsonRecord>(["plugin", "settings-get", id,],);
		if (JSON.stringify(after["config"] ?? {},) !== JSON.stringify(settings["config"] ?? {},)) {
			throw new Error(`plugin settings-set ${id} changed config on an identity roundtrip`,);
		}
		const body = `Owned disposable dev plugin ${id}\n`;
		await ctx.run(["plugin", "folder-add", id, "resources",],);
		await ctx.run(["plugin", "contents-put", id, "resources/README.md", "--content", body,],);
		const paths = await contentsListPaths(ctx, id,);
		if (!paths.some(p => p.endsWith("resources/README.md",))) {
			throw new Error(
				`plugin contents-list ${id} does not show resources/README.md: ${paths.join(", ",)}`,
			);
		}
		await assertContent(ctx, id, "resources/README.md", body,);
		const details = await ctx.run<JsonRecord>(["plugin", "details", id, "resources/README.md",],);
		if (!asRecord(details,)) throw new Error(`plugin details ${id} returned no record`,);
		await ctx.run(["plugin", "rename", id, "resources/README.md", "INTRO.md",],);
		await assertContent(ctx, id, "resources/INTRO.md", body,);
		await ctx.run(["plugin", "move", id, "resources/INTRO.md", "/",],);
		await assertContent(ctx, id, "INTRO.md", body,);
		await ctx.run(["plugin", "contents-delete", id, "INTRO.md",],);
		const remaining = await contentsListPaths(ctx, id,);
		if (remaining.some(p => p === "INTRO.md" || p.endsWith("/INTRO.md",))) {
			throw new Error(`plugin contents-delete ${id} left INTRO.md listed`,);
		}
		const archive = join(ctx.dir, `plugin-download-${id}-i${String(ctx.iteration,)}.zip`,);
		const download = await ctx.run<{ path: string; bytes: number; }>([
			"plugin",
			"download",
			id,
			"--output",
			archive,
		],);
		const bytes = await readFile(archive,);
		if (bytes.byteLength !== download.bytes || bytes.subarray(0, 2,).toString("latin1",) !== "PK") {
			throw new Error(
				`plugin download ${id} did not produce a zip of ${String(download.bytes,)} bytes`,
			);
		}
	},);
}

/**
 * The DSS-documented plugin code-env spec used by this module's case and by
 * the operator's direct SDK probe (single source of truth for the folder
 * layout and file contents). desc.json shape per the DSS 15 plugin reference
 * ("Managing dependencies"): interpreter list, install flags, base packages
 * method, conda.
 */
export function pluginCodeEnvSpec(interpreter: string,): {
	folders: string[];
	descPath: string;
	descJson: string;
	requirementsPath: string;
	requirements: string;
} {
	return {
		folders: ["code-env", "code-env/python", "code-env/python/spec",],
		descPath: "code-env/python/desc.json",
		descJson: JSON.stringify({
			acceptedPythonInterpreters: [interpreter,],
			installCorePackages: false,
			installJupyterSupport: false,
			basePackagesInstallMethod: "PRE_BUILT",
			conda: false,
		},),
		requirementsPath: "code-env/python/spec/requirements.txt",
		requirements: "",
	};
}

/**
 * The complete owned plugin archive for the code-env case: plugin.json plus
 * the full documented code-env spec in one install, so the server loads the
 * environment definition AT INSTALL. Late file writes to a dev plugin are not
 * observed by the cached plugin model (live-proven: list meta stayed empty
 * after plugin.json writes while the archive path binds correctly), so the
 * archive is the only path where the spec is guaranteed visible to the
 * code-env actions.
 */
export function pluginCodeEnvArchiveBytes(
	name: string,
	marker: string,
	interpreter: string,
	version = "1.0.0",
): Uint8Array {
	const encoder = new TextEncoder();
	const spec = pluginCodeEnvSpec(interpreter,);
	return buildStoredZip([
		{ name: `${name}/plugin.json`, data: encoder.encode(pluginJson(name, marker, version,),), },
		{ name: `${name}/${spec.descPath}`, data: encoder.encode(spec.descJson,), },
		{ name: `${name}/${spec.requirementsPath}`, data: encoder.encode(spec.requirements,), },
	],);
}

/**
 * Plugin-managed code env on an owned plugin provisioned COMPLETE through the
 * receipt-gated installPluginFromZip helper: the archive carries plugin.json
 * plus the documented code-env/python spec (desc.json,
 * spec/requirements.txt), the env is created with the interpreter mirrored
 * from the instance's design-managed template env, associated in the plugin
 * settings (the official client's set_code_env + save flow — DSS does not
 * populate the name itself), and the env is rebuilt. Both builds are awaited
 * to a settled future and the definition is read back to confirm
 * PLUGIN_MANAGED with the mirrored interpreter. The derived env
 * (plugin_<id>_managed) gets its own ledger ownership (reserve before
 * create, bind after the settled future) and is deleted BEFORE its parent
 * plugin; an unknown build outcome preserves both.
 */
async function pluginCodeEnv(ctx: LiveContext,): Promise<void> {
	const template = await ctx.run<{ pythonInterpreter?: string; deploymentMode?: string; }>([
		"code-env",
		"get",
		"PYTHON",
		TEMPLATE_CODE_ENV,
	],);
	const interpreter = asString(template.pythonInterpreter,);
	if (template.deploymentMode !== "DESIGN_MANAGED" || interpreter === undefined) {
		blocked(
			`no design-managed template env to mirror an interpreter from (${TEMPLATE_CODE_ENV} reports ${
				template.deploymentMode ?? "unknown"
			})`,
		);
	}
	const entry = await ctx.installPluginFromZip(
		"pluginenv",
		(name, marker,) =>
			ctx.writeFile(
				`plugin-${name}-env.zip`,
				pluginCodeEnvArchiveBytes(name, marker, interpreter,),
			),
	);
	await withOwnedPlugin(ctx, entry, async id => {
		const envName = await ctx.reservePluginCodeEnv(id,);
		let envEntry: OwnedGlobal | undefined;
		let bodyError: unknown;
		try {
			const timeout = String(CODE_ENV_TIMEOUT_MS,);
			// The settled future plus the env existing at the reserved derived
			// name (bound below) is the create proof; the receipt's own fields
			// are not asserted until observed live.
			await runCodeEnvBuild(ctx, id, [
				"plugin",
				"code-env-create",
				id,
				"--python-interpreter",
				interpreter,
				"--wait",
				"--timeout",
				timeout,
			],);
			try {
				envEntry = await ctx.bindGlobal("code-env", envName, `PYTHON/${envName}`,);
			} catch (error) {
				throw new AmbiguousMutationError(
					`managed env ${envName} could not be confirmed after the settled create (missing or unverifiable); its state is unknown, so the parent plugin is preserved: ${
						error instanceof Error ? error.message : String(error,)
					}`,
				);
			}
			await assertManagedEnvDefinition(ctx, envName, interpreter,);
			// Official association flow (Python client): create provisions the
			// env and returns envName, then the plugin settings must name it
			// (set_code_env + save => top-level codeEnvName) before update can
			// rebuild the associated env. Shape stays the object obtained
			// through settings-get per the CLI settings-set contract.
			const settings = await ctx.run<JsonRecord>(["plugin", "settings-get", id,],);
			await ctx.run([
				"plugin",
				"settings-set",
				id,
				"--content",
				JSON.stringify({ ...settings, codeEnvName: envName, },),
			],);
			const associated = await ctx.run<JsonRecord>(["plugin", "settings-get", id,],);
			if (asString(associated["codeEnvName"],) !== envName) {
				throw new Error(
					`plugin settings-get ${id} does not report the associated code env ${envName} after settings-set (reported: ${
						JSON.stringify(associated["codeEnvName"],)
					})`,
				);
			}
			await runCodeEnvBuild(ctx, id, [
				"plugin",
				"code-env-update",
				id,
				"--wait",
				"--timeout",
				timeout,
			],);
		} catch (error) {
			bodyError = error;
		}
		if (bodyError instanceof AmbiguousMutationError) {
			// Unknown build outcome: preserve the env together with its parent.
			throw bodyError;
		}
		if (envEntry?.state === "bound") {
			// Confirmed-terminal branches (success or deterministic failure)
			// clean the env up BEFORE the parent: plugin delete leaves the
			// plugin-managed env behind (observed leak), so this case owns the
			// env teardown. A pending or unconfirmed entry is never deleted
			// blind.
			try {
				await ctx.deleteGlobal("code-env", `PYTHON/${envName}`,);
			} catch (error) {
				throw new AmbiguousMutationError(
					`managed env ${envName} teardown failed; the parent plugin is preserved so the pair stays collectable: ${
						error instanceof Error ? error.message : String(error,)
					}${
						bodyError === undefined
							? ""
							: ` | preceding case failure: ${
								bodyError instanceof Error ? bodyError.message : String(bodyError,)
							}`
					}`,
				);
			}
		}
		if (bodyError !== undefined) throw bodyError;
	},);
}

/**
 * Archive install path on a plugin this run built locally: install-from-zip
 * through the receipt-gated helper (the archive carries the reserved id and
 * the ledger marker), update-from-zip with a bumped version, move-to-dev, then
 * reset-local on the now-dev plugin. The marker is re-read after every state
 * transition so a lost identity is an explicit failure, never a silent one.
 */
async function pluginZip(ctx: LiveContext,): Promise<void> {
	const entry = await ctx.installPluginFromZip(
		"zipplugin",
		(name, marker,) => writePluginArchive(ctx, name, marker, "1.0.0",),
	);
	const marker = ctx.markerFor("plugin", entry.name,);
	await withOwnedPlugin(ctx, entry, async id => {
		const installed = await listedPlugin(ctx, id,);
		if (installed["isDev"] === true) {
			throw new Error(`plugin ${id} installed from zip is unexpectedly a dev plugin`,);
		}
		const updated = await writePluginArchive(ctx, id, marker, "1.1.0",);
		await ctx.run(["plugin", "update-from-zip", id, "--file", updated,],);
		const afterUpdate = await listedPlugin(ctx, id,);
		if (asString(afterUpdate["version"],) !== "1.1.0") {
			throw new Error(
				`plugin update-from-zip ${id} did not raise the listed version to 1.1.0: ${
					JSON.stringify(afterUpdate["version"],)
				}`,
			);
		}
		await ctx.run(["plugin", "move-to-dev", id,],);
		const dev = await listedPlugin(ctx, id,);
		if (dev["isDev"] !== true) {
			throw new Error(`plugin move-to-dev ${id} did not yield a dev plugin`,);
		}
		await assertMarkerPresent(ctx, id, marker,);
		await ctx.run(["plugin", "reset-local", id,],);
		await assertMarkerPresent(ctx, id, marker,);
	},);
}

/**
 * Unconfigured git-remote surface on an owned dev plugin: the null
 * repositoryUrl is the honest observed state; branch listing against a
 * missing remote surfaces DSS's own refusal as the exact blocker. The branch
 * route is GET `/plugins/{pluginId}/gitBranches` (observed `200 ["master"]`
 * on DSS 15; the upstream docs' POST is wrong and answered 405). A 405 with
 * the GET method would be a method/route regression, never a missing-remote
 * prerequisite.
 */
async function pluginGitRemote(ctx: LiveContext,): Promise<void> {
	const entry = await ctx.createPluginDev("gitremote", pluginJson,);
	await withOwnedPlugin(ctx, entry, async id => {
		const remote = await ctx.run<JsonRecord>(["plugin", "get-git-remote", id,],);
		if (asString(remote["repositoryUrl"],) !== undefined) {
			blocked(
				"owned dev plugin unexpectedly reports a configured git remote; refusing to exercise external-git actions against a non-lab remote",
			);
		}
		try {
			await ctx.run(["plugin", "git-branches", id,],);
		} catch (error) {
			const status = error instanceof LiveCommandError
				? asRecord(error.result,)?.["status"]
				: undefined;
			if (status === 405) {
				throw new Error(
					"plugin git-branches answered 405 with the GET method DSS 15 observes for /plugins/{pluginId}/gitBranches; the SDK uses that observed GET, so a 405 is a method/route regression to investigate — not a missing-remote prerequisite.",
					{ cause: error, },
				);
			}
			dssRefusal("plugin git-branches without a declared remote", error,);
		}
	},);
}

/**
 * Remote sync on an owned dev plugin against the operator-designated lab
 * remote: declare → observe → branches → fetch → push → pull → reset-remote →
 * undeclare. Without DATAIKU_LIVE_PLUGIN_GIT_REMOTE the case blocks before any
 * plugin exists. reset-remote runs last because it discards local work; the
 * marker is re-read afterwards so a discarded identity fails loudly.
 */
async function pluginGitSync(ctx: LiveContext,): Promise<void> {
	const remoteUrl = process.env[PLUGIN_GIT_REMOTE_ENV]?.trim();
	if (!remoteUrl) {
		blocked(
			`plugin set-git-remote/fetch/push/pull/reset-remote/delete-git-remote need a lab-owned Git remote reachable from the DSS host: set ${PLUGIN_GIT_REMOTE_ENV} to an SSH or credential-free HTTPS URL of a repository this lab owns. No remote is guessed.`,
		);
	}
	const entry = await ctx.createPluginDev("gitsync", pluginJson,);
	const marker = ctx.markerFor("plugin", entry.name,);
	await withOwnedPlugin(ctx, entry, async id => {
		await ctx.run(["plugin", "set-git-remote", id, "--repository", remoteUrl,],);
		const remote = await ctx.run<JsonRecord>(["plugin", "get-git-remote", id,],);
		if (asString(remote["repositoryUrl"],) !== remoteUrl) {
			throw new Error(`plugin get-git-remote ${id} does not echo the declared remote`,);
		}
		await ctx.run(["plugin", "git-branches", id,],);
		await ctx.run(["plugin", "fetch", id,],);
		await ctx.run(["plugin", "push", id,],);
		await ctx.run(["plugin", "pull", id,],);
		await ctx.run(["plugin", "reset-remote", id,],);
		await assertMarkerPresent(ctx, id, marker,);
		await ctx.run(["plugin", "delete-git-remote", id,],);
		const cleared = await ctx.run<JsonRecord>(["plugin", "get-git-remote", id,],);
		if (asString(cleared["repositoryUrl"],) !== undefined) {
			throw new Error(`plugin delete-git-remote ${id} left the remote declared`,);
		}
	},);
}

export async function exercisePlugins(ctx: LiveContext,): Promise<void> {
	await ctx.check(
		"infrastructure.plugin",
		[
			"plugin.create-dev",
			"plugin.list",
			"plugin.usages",
			"plugin.settings-get",
			"plugin.settings-set",
			"plugin.folder-add",
			"plugin.contents-put",
			"plugin.contents-list",
			"plugin.contents-get",
			"plugin.details",
			"plugin.rename",
			"plugin.move",
			"plugin.contents-delete",
			"plugin.download",
			"plugin.delete",
		],
		async () => {
			await pluginLifecycle(ctx,);
		},
		{ capability: "infrastructure.plugin-crud", },
	);
	await ctx.check(
		"infrastructure.plugin.code-env",
		[
			"code-env.get",
			"plugin.install-from-zip",
			"plugin.code-env-create",
			"code-env.get-definition",
			"plugin.settings-get",
			"plugin.settings-set",
			"plugin.code-env-update",
			"code-env.delete",
			"plugin.delete",
		],
		async () => {
			await pluginCodeEnv(ctx,);
		},
		{ capability: "infrastructure.plugin-code-env", },
	);
	await ctx.check(
		"infrastructure.plugin.zip",
		[
			"plugin.install-from-zip",
			"plugin.list",
			"plugin.update-from-zip",
			"plugin.move-to-dev",
			"plugin.contents-get",
			"plugin.reset-local",
			"plugin.delete",
		],
		async () => {
			await pluginZip(ctx,);
		},
		{ capability: "infrastructure.plugin-zip", },
	);
	await ctx.check(
		"infrastructure.plugin.git-remote",
		["plugin.create-dev", "plugin.get-git-remote", "plugin.git-branches", "plugin.delete",],
		async () => {
			await pluginGitRemote(ctx,);
		},
		{ capability: "infrastructure.plugin-git-remote", required: false, },
	);
	await ctx.check(
		"infrastructure.plugin.git-sync",
		[
			"plugin.create-dev",
			"plugin.set-git-remote",
			"plugin.get-git-remote",
			"plugin.git-branches",
			"plugin.fetch",
			"plugin.push",
			"plugin.pull",
			"plugin.reset-remote",
			"plugin.contents-get",
			"plugin.delete-git-remote",
			"plugin.delete",
		],
		async () => {
			await pluginGitSync(ctx,);
		},
		{ capability: "infrastructure.plugin-git-sync", required: false, },
	);
	await ctx.check(
		"infrastructure.plugin.store",
		["plugin.install-from-store", "plugin.update-from-store",],
		async () => {
			// The Dataiku store only serves Dataiku-published plugins, so any
			// install would adopt a foreign artifact the ownership ledger can
			// neither bind nor delete. No store request is made.
			blocked(
				"plugin install-from-store/update-from-store need a store-published plugin owned by this lab: the Dataiku store serves only Dataiku-published plugins, so every candidate is a foreign artifact the ownership ledger can neither bind nor delete. No store request is made.",
			);
		},
		{ capability: "infrastructure.plugin-store", required: false, },
	);
	await ctx.check(
		"infrastructure.plugin.git-install",
		["plugin.install-from-git", "plugin.update-from-git",],
		async () => {
			// The CLI has no plugin commit action, so a marker-bearing
			// plugin.json cannot be published to a remote by this lab; every
			// reachable repository installs a plugin whose identity the ledger
			// cannot bind. No git request is made.
			blocked(
				"plugin install-from-git/update-from-git need a lab-owned repository whose plugin.json meta.description carries this run's ownership marker; the CLI exposes no plugin commit action, so the lab cannot publish one and every reachable repository is a foreign artifact. No git request is made.",
			);
		},
		{ capability: "infrastructure.plugin-git-install", required: false, },
	);
}
