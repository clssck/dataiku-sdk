import type { CodeEnvDetails, CodeEnvSummary, } from "../schemas.js";
import { CodeEnvSummaryArraySchema, } from "../schemas.js";
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
}

function splitPackageList(raw: string | undefined,): string[] {
	if (!raw) return [];
	return raw
		.split("\n",)
		.map((line,) => line.trim())
		.filter((line,) => line.length > 0);
}
