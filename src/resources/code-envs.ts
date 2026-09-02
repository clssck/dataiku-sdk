import { ClientValidationError, } from "../errors.js";
import type {
	CodeEnvActionResult,
	CodeEnvCreateOptions,
	CodeEnvDetails,
	CodeEnvGetLogOptions,
	CodeEnvLogResult,
	CodeEnvLogSummary,
	CodeEnvPackageList,
	CodeEnvSetPackagesOptions,
	CodeEnvSummary,
	CodeEnvUpdateImagesOptions,
	CodeEnvUpdatePackagesOptions,
	CodeEnvVersionForProject,
	CodeEnvWaitOptions,
} from "../schemas.js";
import {
	CodeEnvLogSummaryArraySchema,
	CodeEnvSummaryArraySchema,
	CodeEnvUsageArraySchema,
	CodeEnvVersionForProjectSchema,
} from "../schemas.js";
import { stableHash, } from "../utils/stable-hash.js";
import { BaseResource, } from "./base.js";

/** Byte cap applied to fetched code-env logs so stdout/JSON stays bounded. */
const DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024;
/** Default number of tail lines returned by getLog (0 disables). */
const DEFAULT_LOG_MAX_LINES = 500;

export class CodeEnvsResource extends BaseResource {
	async list(opts?: { envLang?: "PYTHON" | "R"; },): Promise<CodeEnvSummary[]> {
		const raw = await this.client.get<unknown>("/public/api/admin/code-envs/",);
		const envs = this.client.safeParse(CodeEnvSummaryArraySchema, raw, "codeEnvs.list",);
		if (opts?.envLang) {
			return envs.filter((e,) => e.envLang === opts.envLang);
		}
		return envs;
	}

	async get(envLang: string, envName: string,): Promise<CodeEnvDetails> {
		const langEnc = encodeURIComponent(envLang,);
		const nameEnc = encodeURIComponent(envName,);
		const raw = await this.client.get<Record<string, unknown>>(
			`/public/api/admin/code-envs/${langEnc}/${nameEnc}/`,
		);

		const desc = (raw.desc ?? {}) as Record<string, unknown>;

		const requestedPackages = splitPackageList(raw.specPackageList as string | undefined,);
		requestedPackages.sort();

		const installedPackages = splitPackageList(raw.actualPackageList as string | undefined,);

		return {
			envName: (raw.envName as string) ?? envName,
			envLang: (raw.envLang as string) ?? envLang,
			pythonInterpreter: desc.pythonInterpreter as string | undefined,
			deploymentMode: typeof raw.deploymentMode === "string"
				? raw.deploymentMode
				: undefined,
			requestedPackages,
			installedPackages,
			definitionHash: stableHash(raw,),
		};
	}

	async getDefinition(envLang: string, envName: string,): Promise<Record<string, unknown>> {
		const langEnc = encodeURIComponent(envLang,);
		const nameEnc = encodeURIComponent(envName,);
		return this.client.get<Record<string, unknown>>(
			`/public/api/admin/code-envs/${langEnc}/${nameEnc}`,
		);
	}

	/** List the build log files DSS keeps for this code environment. */
	async listLogs(envLang: string, envName: string,): Promise<CodeEnvLogSummary[]> {
		const langEnc = encodeURIComponent(envLang,);
		const nameEnc = encodeURIComponent(envName,);
		const raw = await this.client.get<unknown>(
			`/public/api/admin/code-envs/${langEnc}/${nameEnc}/logs`,
		);
		return this.client.safeParse(CodeEnvLogSummaryArraySchema, raw, "codeEnvs.listLogs",);
	}

	/**
	 * Fetch one code-env build log, bounded by default: at most
	 * `maxBytes` bytes (default 10 MiB, 0 disables the cap) and the last
	 * `maxLines` lines (default 500, 0 disables the tail). Returns what was
	 * kept plus the truncation flags so callers can warn instead of
	 * silently losing output.
	 */
	async getLog(
		envLang: string,
		envName: string,
		logName: string,
		opts?: CodeEnvGetLogOptions,
	): Promise<CodeEnvLogResult> {
		const langEnc = encodeURIComponent(envLang,);
		const nameEnc = encodeURIComponent(envName,);
		const logEnc = encodeURIComponent(logName,);
		const path = `/public/api/admin/code-envs/${langEnc}/${nameEnc}/logs/${logEnc}`;
		const maxBytes = opts?.maxBytes ?? DEFAULT_LOG_MAX_BYTES;
		const fetched = maxBytes > 0
			? await this.client.getTextLimited(path, maxBytes,)
			: { text: await this.client.getText(path,), truncated: false, };
		const tailed = tailLog(fetched.text, opts?.maxLines ?? DEFAULT_LOG_MAX_LINES,);
		return {
			log: tailed.log,
			bytes: Buffer.byteLength(fetched.text, "utf-8",),
			truncated: fetched.truncated,
			tailed: tailed.tailed,
		};
	}

	/**
	 * Rebuild the Docker image(s) of the code environment to match its
	 * settings. `wait: false` returns the raw DSS future payload.
	 */
	async updateImages(
		envLang: string,
		envName: string,
		opts?: CodeEnvUpdateImagesOptions,
	): Promise<CodeEnvActionResult> {
		const langEnc = encodeURIComponent(envLang,);
		const nameEnc = encodeURIComponent(envName,);
		const query = new URLSearchParams();
		if (opts?.envVersion !== undefined) query.set("envVersion", opts.envVersion,);
		query.set("wait", String(opts?.wait !== false,),);

		return this.client.post<CodeEnvActionResult>(
			`/public/api/admin/code-envs/${langEnc}/${nameEnc}/images?${query.toString()}`,
		);
	}

	/**
	 * Resolve the code-env version a project uses (versioned environments on
	 * automation nodes); empty `version` for unversioned environments.
	 */
	async getVersionForProject(
		envLang: string,
		envName: string,
		projectKey: string,
	): Promise<CodeEnvVersionForProject> {
		const langEnc = encodeURIComponent(envLang,);
		const nameEnc = encodeURIComponent(envName,);
		const pkEnc = encodeURIComponent(projectKey,);
		const raw = await this.client.get<unknown>(
			`/public/api/admin/code-envs/${langEnc}/${nameEnc}/${pkEnc}/version`,
		);
		return this.client.safeParse(
			CodeEnvVersionForProjectSchema,
			raw,
			"codeEnvs.getVersionForProject",
		);
	}

	async create(opts: CodeEnvCreateOptions,): Promise<CodeEnvActionResult> {
		const langEnc = encodeURIComponent(opts.envLang,);
		const nameEnc = encodeURIComponent(opts.envName,);
		const body = {
			...opts.params,
			deploymentMode: opts.deploymentMode,
		};
		return this.client.post<CodeEnvActionResult>(
			`/public/api/admin/code-envs/${langEnc}/${nameEnc}?wait=${opts.wait !== false}`,
			body,
		);
	}

	async setDefinition(
		envLang: string,
		envName: string,
		definition: Record<string, unknown>,
		opts?: { expectHash?: string; },
	): Promise<CodeEnvActionResult> {
		if (opts?.expectHash !== undefined) {
			const current = await this.getDefinition(envLang, envName,);
			assertDefinitionHash(envLang, envName, current, opts.expectHash,);
		}
		const langEnc = encodeURIComponent(envLang,);
		const nameEnc = encodeURIComponent(envName,);
		return this.client.put<CodeEnvActionResult>(
			`/public/api/admin/code-envs/${langEnc}/${nameEnc}`,
			definition,
		);
	}

	async setPackages(
		envLang: string,
		envName: string,
		packages: CodeEnvPackageList,
		opts?: Pick<CodeEnvSetPackagesOptions, "installCorePackages" | "expectHash">,
	): Promise<CodeEnvActionResult> {
		const current = await this.getDefinition(envLang, envName,);
		if (opts?.expectHash !== undefined) {
			assertDefinitionHash(envLang, envName, current, opts.expectHash,);
		}
		const next: Record<string, unknown> = {
			...current,
			specPackageList: normalizePackageList(packages,),
		};

		if (opts?.installCorePackages !== undefined) {
			const currentDesc = current.desc;
			const desc = isRecord(currentDesc,) ? { ...currentDesc, } : {};
			desc.installCorePackages = opts.installCorePackages;
			next.desc = desc;
		}

		return this.setDefinition(envLang, envName, next,);
	}

	async updatePackages(
		envLang: string,
		envName: string,
		opts?: CodeEnvUpdatePackagesOptions,
	): Promise<CodeEnvActionResult> {
		const langEnc = encodeURIComponent(envLang,);
		const nameEnc = encodeURIComponent(envName,);
		const query = new URLSearchParams();
		query.set("forceRebuildEnv", String(opts?.forceRebuildEnv === true,),);
		if (opts?.versionToUpdate !== undefined) query.set("versionToUpdate", opts.versionToUpdate,);
		query.set("wait", String(opts?.wait !== false,),);

		return this.client.post<CodeEnvActionResult>(
			`/public/api/admin/code-envs/${langEnc}/${nameEnc}/packages?${query.toString()}`,
		);
	}

	async setJupyterSupport(
		envLang: string,
		envName: string,
		active: boolean,
		opts?: CodeEnvWaitOptions,
	): Promise<CodeEnvActionResult> {
		const langEnc = encodeURIComponent(envLang,);
		const nameEnc = encodeURIComponent(envName,);
		const query = new URLSearchParams();
		query.set("active", String(active,),);
		query.set("wait", String(opts?.wait !== false,),);

		return this.client.post<CodeEnvActionResult>(
			`/public/api/admin/code-envs/${langEnc}/${nameEnc}/jupyter?${query.toString()}`,
		);
	}

	async delete(
		envLang: string,
		envName: string,
		opts?: CodeEnvWaitOptions,
	): Promise<CodeEnvActionResult> {
		const langEnc = encodeURIComponent(envLang,);
		const nameEnc = encodeURIComponent(envName,);
		await this.client.del(
			`/public/api/admin/code-envs/${langEnc}/${nameEnc}?wait=${opts?.wait !== false}`,
		);
		return { deleted: envName, envLang, };
	}

	async listUsages(envLang?: string, envName?: string,): Promise<Record<string, unknown>[]> {
		let raw: unknown;
		if (envLang !== undefined || envName !== undefined) {
			if (!envLang || !envName) {
				throw new Error(
					"codeEnvs.listUsages requires both envLang and envName when either is provided",
				);
			}
			const langEnc = encodeURIComponent(envLang,);
			const nameEnc = encodeURIComponent(envName,);
			raw = await this.client.get<unknown>(
				`/public/api/admin/code-envs/${langEnc}/${nameEnc}/usages`,
			);
		} else {
			raw = await this.client.get<unknown>("/public/api/admin/code-envs/usages",);
		}

		return this.client.safeParse(CodeEnvUsageArraySchema, raw, "codeEnvs.listUsages",);
	}
}

/**
 * Keep only the last `maxLines` lines of a log (0 disables the tail).
 * Returns whether any leading lines were dropped.
 */
function tailLog(log: string, maxLines: number,): { log: string; tailed: boolean; } {
	if (!log) return { log, tailed: false, };
	if (maxLines <= 0) return { log, tailed: false, };
	const lines = log.split(/\r\n|\n/,);
	if (lines.length <= maxLines) return { log, tailed: false, };
	return { log: lines.slice(lines.length - maxLines,).join("\n",), tailed: true, };
}

/**
 * Provenance guard for full-definition writes: DSS requires the PUT body to
 * come from a prior GET, so refuse the write when the definition no longer
 * hashes to the hash the caller captured.
 */
function assertDefinitionHash(
	envLang: string,
	envName: string,
	current: Record<string, unknown>,
	expected: string,
): void {
	const currentHash = stableHash(current,);
	if (currentHash === expected) return;
	throw new ClientValidationError(
		`Code env ${envLang}/${envName} changed since the expected definition hash was captured; refusing to overwrite it.`,
		"validation_failed",
		"Re-read the definition with dss code-env get-definition (definitionHash comes from code-env get), review the diff, and retry with the fresh hash.",
		{
			envLang,
			envName,
			expectedDefinitionHash: expected,
			currentDefinitionHash: currentHash,
		},
	);
}

function splitPackageList(raw: string | undefined,): string[] {
	if (!raw) return [];
	return raw
		.split("\n",)
		.map((line,) => line.trim())
		.filter((line,) => line.length > 0);
}

function normalizePackageList(packages: CodeEnvPackageList,): string {
	const lines = Array.isArray(packages,) ? packages : packages.split(/\r?\n/,);
	return lines.map((line,) => line.trim()).filter((line,) => line.length > 0).join("\n",);
}

function isRecord(value: unknown,): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value,);
}
