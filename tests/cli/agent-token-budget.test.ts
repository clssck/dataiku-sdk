import { describe, expect, it, } from "bun:test";
import { AGENTS, } from "../../src/skill.js";
import { dss, dssFailure, join, mkdirSync, rmSync, tmpdir, writeFileSync, } from "./_harness.js";
import { assertAgentEncodingPinned, measureAgentText, } from "./_token-metrics.js";

interface TokenBudget {
	baseline: number;
	maxTokens: number;
}

const TOKEN_BUDGETS = {
	skill: { baseline: 3_943, maxTokens: 4_300, },
	agentContract: { baseline: 2_705, maxTokens: 2_900, },
	fullRegistry: { baseline: 219_638, maxTokens: 250_000, },
	datasetResource: { baseline: 11_353, maxTokens: 13_000, },
	datasetCreate: { baseline: 1_035, maxTokens: 1_200, },
	datasetCreateUsage: { baseline: 50, maxTokens: 70, },
	datasetCreateDescription: { baseline: 11, maxTokens: 24, },
	scopedBootstrap: { baseline: 245, maxTokens: 285, },
	actionSummary: { baseline: 1_039, maxTokens: 1_200, },
	fourFieldProjection: { baseline: 338, maxTokens: 390, },
	fieldsUsageFailure: { baseline: 92, maxTokens: 110, },
	unknownFlag: { baseline: 42, maxTokens: 60, },
	unknownResourceRecovery: { baseline: 204, maxTokens: 240, },
	doctorFailure: { baseline: 105, maxTokens: 130, },
	batchFailure: { baseline: 138, maxTokens: 170, },
	cleanupFailure: { baseline: 99, maxTokens: 130, },
} satisfies Record<string, TokenBudget>;

function expectWithinBudget(name: string, text: string, budget: TokenBudget,): number {
	const metrics = measureAgentText(text,);
	expect(
		metrics.tokens,
		`${name} uses ${metrics.tokens.toLocaleString()} ${metrics.encoding} tokens; baseline ${budget.baseline.toLocaleString()}, budget ${budget.maxTokens.toLocaleString()}, ${metrics.utf8Bytes.toLocaleString()} UTF-8 bytes`,
	).toBeLessThanOrEqual(budget.maxTokens,);
	return metrics.tokens;
}

describe("agent-facing token budgets", () => {
	it("pins the loaded tokenizer to the exact o200k_base model", () => {
		const fingerprint = assertAgentEncodingPinned();
		expect(fingerprint.encoding,).toBe("o200k_base",);
	});

	it("bounds bootstrap and scoped discovery output with a pinned tokenizer", async () => {
		const [
			agentContract,
			fullRegistry,
			datasetResource,
			datasetCreate,
			prettyDatasetCreate,
			usage,
			description,
			scopedBootstrap,
			actionSummary,
			fourFieldProjection,
		] = await Promise.all([
			dss(["agent", "contract", "--json",],),
			dss(["commands", "run", "--json",],),
			dss(["commands", "run", "--fields", "dataset", "--json",],),
			dss(["commands", "run", "--fields", "dataset.create", "--json",],),
			dss(["commands", "run", "--fields", "dataset.create",],),
			dss(["commands", "run", "--fields", "dataset.create.usage", "--json",],),
			dss(["commands", "run", "--fields", "dataset.create.description", "--json",],),
			dss([
				"agent",
				"contract",
				"--fields",
				"protocol,agentContractVersion,cli,stdio,planning,compatibility",
				"--json",
			],),
			dss(["agent", "contract", "--fields", "commands.actions", "--json",],),
			dss([
				"commands",
				"run",
				"--fields",
				"dataset.create.usage,dataset.create.description,dataset.create.flags,dataset.create.examples",
				"--json",
			],),
		],);
		const skill = AGENTS.omp?.content();
		expect(skill,).toBeDefined();

		expectWithinBudget("installed skill", skill!, TOKEN_BUDGETS.skill,);
		expectWithinBudget("agent contract", agentContract.stdout, TOKEN_BUDGETS.agentContract,);
		const fullRegistryTokens = expectWithinBudget(
			"full command registry",
			fullRegistry.stdout,
			TOKEN_BUDGETS.fullRegistry,
		);
		expectWithinBudget(
			"dataset resource registry",
			datasetResource.stdout,
			TOKEN_BUDGETS.datasetResource,
		);
		const actionTokens = expectWithinBudget(
			"dataset.create registry entry",
			datasetCreate.stdout,
			TOKEN_BUDGETS.datasetCreate,
		);
		expectWithinBudget(
			"dataset.create usage projection",
			usage.stdout,
			TOKEN_BUDGETS.datasetCreateUsage,
		);
		expectWithinBudget(
			"dataset.create description projection",
			description.stdout,
			TOKEN_BUDGETS.datasetCreateDescription,
		);
		expectWithinBudget(
			"scoped agent-contract bootstrap",
			scopedBootstrap.stdout,
			TOKEN_BUDGETS.scopedBootstrap,
		);
		const bootstrap = JSON.parse(scopedBootstrap.stdout,) as Record<string, unknown>;
		expect(Object.keys(bootstrap,).sort(),).toEqual([
			"agentContractVersion",
			"cli",
			"compatibility",
			"planning",
			"protocol",
			"stdio",
		],);
		expectWithinBudget(
			"resource/action summary",
			actionSummary.stdout,
			TOKEN_BUDGETS.actionSummary,
		);
		const actionIndex = JSON.parse(actionSummary.stdout,) as {
			"commands.actions"?: Record<string, string[]>;
		};
		expect(actionIndex["commands.actions"]?.dataset,).toContain("create",);
		expectWithinBudget(
			"recommended four-field projection",
			fourFieldProjection.stdout,
			TOKEN_BUDGETS.fourFieldProjection,
		);
		const projected = JSON.parse(fourFieldProjection.stdout,) as Record<string, unknown>;
		expect(Object.keys(projected,).sort(),).toEqual([
			"dataset.create.description",
			"dataset.create.examples",
			"dataset.create.flags",
			"dataset.create.usage",
		],);

		// The intended action-scoped lookup must stay below 1% of the full registry.
		expect(actionTokens * 100,).toBeLessThan(fullRegistryTokens,);
		const prettyActionTokens = measureAgentText(prettyDatasetCreate.stdout,).tokens;
		expect(actionTokens,).toBeLessThanOrEqual(prettyActionTokens,);
		expect(datasetCreate.stdout.trim(),).not.toContain("\n",);
		const scoped = JSON.parse(datasetCreate.stdout,) as Record<string, Record<string, unknown>>;
		expect(scoped["dataset.create"],).toMatchObject({
			usage: expect.any(String,),
			description: expect.any(String,),
			examples: expect.any(Array,),
			flags: expect.any(Array,),
		},);
	});

	it("keeps every command description concise without dropping recovery context", async () => {
		const { stdout, } = await dss(["commands", "run", "--json",],);
		const registry = JSON.parse(stdout,) as Record<
			string,
			Record<string, { description?: string; usage?: string; examples?: string[]; }>
		>;

		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const [action, entry,] of Object.entries(actions,)) {
				const key = `${resource}.${action}`;
				const descriptionTokens = measureAgentText(entry.description ?? "",).tokens;
				const usageTokens = measureAgentText(entry.usage ?? "",).tokens;
				const exampleTokens = measureAgentText(JSON.stringify(entry.examples ?? [],),).tokens;
				expect(descriptionTokens, `${key} description is too terse`,).toBeGreaterThanOrEqual(4,);
				expect(descriptionTokens, `${key} description is too verbose`,).toBeLessThanOrEqual(80,);
				expect(usageTokens, `${key} usage is too verbose`,).toBeLessThanOrEqual(160,);
				expect(exampleTokens, `${key} examples are too verbose`,).toBeLessThanOrEqual(120,);
			}
		}
	});

	it("keeps usage failures and stdout failure reports compact", async () => {
		// An empty or comma-only projection must fail usage, never dump the registry.
		for (const fields of ["", ",,",]) {
			const failure = await dssFailure(["commands", "run", "--fields", fields, "--json",],);
			expect(failure.code, `--fields ${JSON.stringify(fields,)} must be a usage error`,).toBe(1,);
			expect(failure.stdout,).toBe("",);
			expectWithinBudget(
				`empty/comma-only projection failure (--fields ${JSON.stringify(fields,)})`,
				failure.stderr,
				TOKEN_BUDGETS.fieldsUsageFailure,
			);
			expect(JSON.parse(failure.stderr,) as Record<string, unknown>,).toMatchObject({
				type: "error",
				ok: false,
				code: "usage_error",
				exitCode: 1,
				resource: "commands",
				action: "run",
			},);
		}

		// Unknown flags stay terse; unknown resources recover with valid options.
		const unknownFlag = await dssFailure(["dataset", "list", "--bogus-flag", "--json",],);
		expect(unknownFlag.code,).toBe(1,);
		expect(unknownFlag.stdout,).toBe("",);
		expectWithinBudget("unknown-flag error", unknownFlag.stderr, TOKEN_BUDGETS.unknownFlag,);
		expect(JSON.parse(unknownFlag.stderr,) as Record<string, unknown>,).toMatchObject({
			code: "unknown_flag",
			exitCode: 1,
		},);

		const unknownResource = await dssFailure(["datsaet", "list", "--json",],);
		expect(unknownResource.code,).toBe(1,);
		expectWithinBudget(
			"unknown-resource recovery error",
			unknownResource.stderr,
			TOKEN_BUDGETS.unknownResourceRecovery,
		);
		const recovery = JSON.parse(unknownResource.stderr,) as {
			details?: { validResources?: string[]; };
		};
		expect(Array.isArray(recovery.details?.validResources,),).toBe(true,);

		// doctor, batch, and cleanup failure reports stay single JSON values on
		// stdout with stderr empty, per the agent-contract stdio metadata.
		const doctorConfigDir = join(tmpdir(), `dss-token-budget-config-${Date.now()}`,);
		mkdirSync(doctorConfigDir, { recursive: true, },);
		try {
			const doctor = await dssFailure(["doctor",], {
				env: {
					PATH: process.env.PATH,
					HOME: process.env.HOME,
					DSS_CONFIG_DIR: doctorConfigDir,
					DATAIKU_URL: " ",
					DATAIKU_API_KEY: " ",
				},
			},);
			expect(doctor.code,).toBe(2,);
			expect(doctor.stderr,).toBe("",);
			expectWithinBudget("doctor failure report", doctor.stdout, TOKEN_BUDGETS.doctorFailure,);
			expect(JSON.parse(doctor.stdout,) as { ok: boolean; },).toMatchObject({ ok: false, },);
		} finally {
			rmSync(doctorConfigDir, { recursive: true, force: true, },);
		}

		const batch = await dssFailure([
			"batch",
			"run",
			"--data",
			'[["commands","run","--fields","","--json"]]',
			"--json",
		],);
		expect(batch.code,).toBe(1,);
		expect(batch.stderr,).toBe("",);
		expectWithinBudget("batch failure report", batch.stdout, TOKEN_BUDGETS.batchFailure,);
		expect(JSON.parse(batch.stdout,) as { ok: boolean; steps: unknown[]; },).toMatchObject({
			ok: false,
		},);

		const ledgerPath = join(tmpdir(), `dss-token-budget-ledger-${Date.now()}.json`,);
		try {
			writeFileSync(
				ledgerPath,
				JSON.stringify({
					ts: "2026-08-13T00:00:00.000Z",
					resource: "dataset",
					action: "create",
					name: "nope",
					dssUrl: "https://other.example.com/",
					cleanup: { argv: ["dataset", "delete", "nope",], },
				},) + "\n",
			);
			const cleanup = await dssFailure(
				["cleanup", "run", "--file", ledgerPath, "--apply", "--json",],
				{
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DATAIKU_URL: "http://127.0.0.1:1",
						DATAIKU_API_KEY: "test-key",
					},
				},
			);
			expect(cleanup.code,).toBe(2,);
			expect(cleanup.stderr,).toBe("",);
			expectWithinBudget("cleanup failure report", cleanup.stdout, TOKEN_BUDGETS.cleanupFailure,);
			expect(
				JSON.parse(cleanup.stdout,) as { applied: boolean; bindingError?: unknown; },
			).toMatchObject({ applied: false, },);
		} finally {
			rmSync(ledgerPath, { force: true, },);
		}
	});
});
