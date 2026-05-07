import type {
	CodeEnvActionResult,
	CodeEnvCreateOptions,
	CodeEnvDetails,
	CodeEnvPackageList,
	CodeEnvSetPackagesOptions,
	CodeEnvSummary,
	CodeEnvUpdatePackagesOptions,
	CodeEnvWaitOptions,
} from "../schemas.js";
import { CodeEnvSummaryArraySchema, CodeEnvUsageArraySchema, } from "../schemas.js";
import { BaseResource, } from "./base.js";

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
			requestedPackages,
			installedPackages,
		};
	}

	async getDefinition(envLang: string, envName: string,): Promise<Record<string, unknown>> {
		const langEnc = encodeURIComponent(envLang,);
		const nameEnc = encodeURIComponent(envName,);
		return this.client.get<Record<string, unknown>>(
			`/public/api/admin/code-envs/${langEnc}/${nameEnc}`,
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
	): Promise<CodeEnvActionResult> {
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
		opts?: Pick<CodeEnvSetPackagesOptions, "installCorePackages">,
	): Promise<CodeEnvActionResult> {
		const current = await this.getDefinition(envLang, envName,);
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
