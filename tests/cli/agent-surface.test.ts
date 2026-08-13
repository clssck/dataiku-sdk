import { describe, expect, it, } from "bun:test";
import { commands, } from "../../src/cli/commands/index.js";
import { dss, dssFailure, } from "./_harness.js";

describe("CLI agent-only command surface", () => {
	it("dss with no args emits a JSON usage envelope pointing at commands run", async () => {
		const failure = await dssFailure([],);
		expect(failure.code,).toBe(1,);
		expect(failure.stdout,).toBe("",);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			ok: false,
			error: expect.any(String,),
			code: "usage_error",
			category: "usage",
			exitCode: 1,
			hint: "Use `dss commands run` for machine-readable command discovery.",
		},);
		expect((report.details as Record<string, unknown>).command,).toBe("dss commands run",);
	});

	it("--help and -h fail as JSON usage envelopes", async () => {
		for (const flag of ["--help", "-h",]) {
			const failure = await dssFailure([flag,],);
			expect(failure.code,).toBe(1,);
			expect(failure.stdout,).toBe("",);
			const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				ok: false,
				error: "Help screens are not supported.",
				code: "usage_error",
				category: "usage",
				exitCode: 1,
			},);
			expect((report.details as Record<string, unknown>).command,).toBe("dss commands run",);
		}
	});

	it("--version and version command emit JSON", async () => {
		for (const args of [["--version",], ["version",], ["version", "run",],]) {
			const { stdout, stderr, } = await dss(args,);
			expect(stderr,).toBe("",);
			const version = JSON.parse(stdout,) as Record<string, unknown>;
			expect(version.version,).toEqual(expect.any(String,),);
			expect(version,).toHaveProperty("gitRevision",);
		}
	});

	it("commands run prints the machine-readable registry", async () => {
		const { stdout, stderr, } = await dss(["commands", "run",],);
		expect(stderr,).toBe("",);
		const registry = JSON.parse(stdout,) as Record<string, Record<string, unknown>>;
		expect(registry.version.run,).toMatchObject({
			resource: "version",
			action: "run",
			usage: "dss version",
			requiresAuth: false,
			outputShape: "object",
		},);
		expect(registry.auth.login,).toMatchObject({
			resource: "auth",
			action: "login",
			requiredFlags: ["url", "api-key",],
			outputShape: "object",
		},);
		expect(registry.auth,).not.toHaveProperty("status",);
		expect(registry.auth,).not.toHaveProperty("logout",);
		expect(registry.dataset.list,).toMatchObject({
			resource: "dataset",
			action: "list",
		},);
		expect(registry.agent.contract,).toMatchObject({
			resource: "agent",
			action: "contract",
			requiresAuth: false,
			agentContractVersion: 1,
		},);
		expect(registry.recipe["get-payload"],).toHaveProperty("unsafeOutputs",);
	});

	it("agent contract prints the versioned agent protocol", async () => {
		const { stdout, stderr, } = await dss(["agent", "contract",],);
		expect(stderr,).toBe("",);
		const contract = JSON.parse(stdout,) as Record<string, unknown>;
		expect(contract,).toMatchObject({
			protocol: "dataiku-sdk-agent",
			agentContractVersion: 1,
		},);
		expect(contract,).toHaveProperty("commands.actions.agent",);
		expect(contract,).toHaveProperty("schemas.agentContract",);
		expect(contract,).toHaveProperty("schemas.traceEvent",);
	});
	it("--report-json is rejected as an unknown flag", async () => {
		const failure = await dssFailure(["commands", "run", "--report-json",],);
		expect(failure.code,).toBe(1,);
		expect(failure.stdout,).toBe("",);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			ok: false,
			error: "Unknown flag: --report-json",
			code: "unknown_flag",
			category: "usage",
			exitCode: 1,
		},);
	});

	it("DSS_REPORT_JSON no longer changes success or failure output", async () => {
		const success = await dss(["commands", "run",], {
			env: {
				...process.env,
				DSS_REPORT_JSON: "1",
				DATAIKU_URL: "",
				DATAIKU_API_KEY: "",
				DATAIKU_PROJECT_KEY: "",
				DATAIKU_DISABLE_ENV: "1",
			},
		},);
		expect(success.stderr,).toBe("",);
		expect(JSON.parse(success.stdout,),).toHaveProperty("commands",);

		const failure = await dssFailure(["project", "list",], {
			env: {
				...process.env,
				DSS_REPORT_JSON: "1",
				DATAIKU_URL: "",
				DATAIKU_API_KEY: "",
				DATAIKU_PROJECT_KEY: "",
				DATAIKU_DISABLE_ENV: "1",
			},
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stdout,).toBe("",);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			ok: false,
			code: "missing_required_flag",
			exitCode: 1,
			resource: "project",
			action: "list",
		},);
	});

	it("unknown resource and action envelopes include valid options", async () => {
		const unknownResource = await dssFailure(["not-a-resource",],);
		expect(unknownResource.code,).toBe(1,);
		const resourceReport = JSON.parse(unknownResource.stderr,) as Record<string, unknown>;
		expect(resourceReport,).toMatchObject({
			ok: false,
			error: expect.any(String,),
			code: "usage_error",
			category: "usage",
			resource: "not-a-resource",
			exitCode: 1,
		},);
		expect((resourceReport.details as Record<string, unknown>).validResources,).toEqual(
			expect.arrayContaining(["project", "commands", "install-skill",],),
		);

		const unknownAction = await dssFailure(["project", "not-an-action",],);
		expect(unknownAction.code,).toBe(1,);
		const actionReport = JSON.parse(unknownAction.stderr,) as Record<string, unknown>;
		expect(actionReport,).toMatchObject({
			ok: false,
			error: expect.any(String,),
			code: "usage_error",
			category: "usage",
			resource: "project",
			action: "not-an-action",
			exitCode: 1,
		},);
		expect((actionReport.details as Record<string, unknown>).validActions,).toEqual(
			expect.arrayContaining(["list", "get",],),
		);

		const specialRunAction = await dssFailure(["version", "bogus",],);
		expect(specialRunAction.code,).toBe(1,);
		const specialReport = JSON.parse(specialRunAction.stderr,) as Record<string, unknown>;
		expect(specialReport,).toMatchObject({
			ok: false,
			error: expect.any(String,),
			code: "usage_error",
			category: "usage",
			resource: "version",
			action: "bogus",
			exitCode: 1,
		},);
		expect((specialReport.details as Record<string, unknown>).validActions,).toEqual(["run",],);
	});
});

describe("CLI command registry discovery", () => {
	it("registry exposes flags and recipe actions instead of help text", async () => {
		const { stdout, stderr, } = await dss(["commands", "run",],);
		expect(stderr,).toBe("",);
		const registry = JSON.parse(stdout,) as Record<
			string,
			Record<string, {
				flags?: Array<{ name: string; kind: string; }>;
				action?: string;
				agentContractVersion?: number;
			}>
		>;
		const projectFlags = registry.project.list.flags?.map((flag,) => flag.name) ?? [];
		expect(projectFlags,).toEqual(expect.arrayContaining(["json", "verbose", "url", "api-key",],),);
		expect(projectFlags,).not.toContain("help",);
		expect(projectFlags,).not.toContain("report-json",);
		expect(registry.recipe["get-payload"].action,).toBe("get-payload",);
		expect(registry.recipe["set-payload"].action,).toBe("set-payload",);
		expect(registry.agent.contract.agentContractVersion,).toBe(1,);
	});
});

describe("CLI command registry short flags", () => {
	it("registry exposes supported short-alias-backed flags without help metadata", async () => {
		const { stdout, stderr, } = await dss(["commands", "run",],);
		expect(stderr,).toBe("",);
		const registry = JSON.parse(stdout,) as Record<
			string,
			Record<string, { flags?: Array<{ name: string; }>; }>
		>;
		const projectFlags = registry.project.list.flags?.map((flag,) => flag.name) ?? [];
		expect(projectFlags,).toContain("verbose",);
		expect(projectFlags,).not.toContain("help",);
	});
});

describe("CLI registry required-input usage accuracy", () => {
	it("marks handler-required payload/file/SQL inputs as required in usage", () => {
		const registry = commands as unknown as Record<
			string,
			Record<string, { usage: string; }>
		>;
		// Each handler unconditionally requires its payload/file/SQL input (requireArgs /
		// explicit validation), so the registry usage must advertise it as a required
		// "(...)" group rather than an optional "[...]" group.
		const expectedRequired = [
			["code-env", "set-definition", "(--data JSON|--data-file PATH|--stdin)",],
			["code-env", "set-packages", "(--packages PKGS|--package PKG|--file PATH)",],
			["dashboard", "update", "(--name NAME|--data JSON|--data-file PATH|--stdin)",],
			["dataset", "refresh-schema", "(--data JSON | --data-file PATH | --stdin)",],
			["dataset", "update", "(--data '{...}' | --data-file PATH | --stdin)",],
			["folder", "update", "(--data JSON | --data-file PATH | --stdin)",],
			[
				"insight",
				"update",
				"(--name NAME|--listed true|false|--params JSON|--content TEXT|--file PATH --content-type MIME|--data JSON|--data-file PATH|--stdin)",
			],
			["notebook", "save-jupyter", "(--data '{...}' | --data-file PATH | --stdin)",],
			["notebook", "save-sql", "(--data '{...}' | --data-file PATH | --stdin)",],
			["recipe", "clone", "(source|--from SOURCE)",],
			["recipe", "update", "(--data '{...}' | --data-file PATH | --stdin)",],
			["scenario", "update", "(--data '{...}' | --data-file PATH | --stdin)",],
			["sql", "query", "(SQL | --sql QUERY | --sql-file PATH | --sql - | --stdin)",],
			[
				"wiki",
				"update",
				"(--name NAME | --content TEXT|--file PATH|--data JSON|--data-file PATH|--stdin)",
			],
		];
		for (const [resource, action, requiredGroup,] of expectedRequired) {
			const usage = registry[resource]?.[action]?.usage ?? "";
			expect(usage, `${resource} ${action} must advertise required input group`,).toContain(
				requiredGroup,
			);
		}
	});

	it("never advertises an optional payload group for a handler-required payload", () => {
		const registry = commands as unknown as Record<
			string,
			Record<string, { usage: string; }>
		>;
		// Commands whose payload/file group is genuinely optional (handler accepts an
		// absent payload); everything else must not bracket-mark a payload input.
		const optionalPayload = new Set([
			"project create",
			"project duplicate",
			"project export",
			"streaming-endpoint create",
			"continuous-activity start",
			"meaning create",
			"wiki create",
			"dashboard create",
			"insight create",
			"code-env create",
		],);
		const payloadTokens =
			/--data\b|--stdin\b|--file\b|--content\b|--sql\b|\[SQL\b|--packages\b|--package\b|--params\b/;
		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const [action, meta,] of Object.entries(actions,)) {
				if (/validate/.test(action,)) continue; // validation commands are out of audit scope
				if (optionalPayload.has(`${resource} ${action}`,)) continue;
				const bracketed = meta.usage.match(/\[[^\]]*\]/g,) ?? [];
				const offending = bracketed.filter((group,) => payloadTokens.test(group,));
				expect(
					offending,
					`${resource} ${action} marks a handler-required payload optional: ${offending.join(" ",)}`,
				).toEqual([],);
			}
		}
	});
});
