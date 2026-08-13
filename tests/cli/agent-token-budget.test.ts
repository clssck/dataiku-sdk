import { describe, expect, it, } from "bun:test";
import { AGENTS, } from "../../src/skill.js";
import { dss, } from "./_harness.js";
import { AGENT_TOKEN_ENCODING, measureAgentText, } from "./_token-metrics.js";

interface TokenBudget {
	baseline: number;
	maxTokens: number;
}

const TOKEN_BUDGETS = {
	skill: { baseline: 3_930, maxTokens: 4_300, },
	agentContract: { baseline: 2_592, maxTokens: 2_900, },
	fullRegistry: { baseline: 219_363, maxTokens: 250_000, },
	datasetResource: { baseline: 11_353, maxTokens: 13_000, },
	datasetCreate: { baseline: 1_035, maxTokens: 1_200, },
	datasetCreateUsage: { baseline: 50, maxTokens: 70, },
	datasetCreateDescription: { baseline: 11, maxTokens: 24, },
} satisfies Record<string, TokenBudget>;

function expectWithinBudget(name: string, text: string, budget: TokenBudget,): number {
	const metrics = measureAgentText(text,);
	expect(metrics.encoding,).toBe(AGENT_TOKEN_ENCODING,);
	expect(
		metrics.tokens,
		`${name} uses ${metrics.tokens.toLocaleString()} ${metrics.encoding} tokens; baseline ${budget.baseline.toLocaleString()}, budget ${budget.maxTokens.toLocaleString()}, ${metrics.utf8Bytes.toLocaleString()} UTF-8 bytes`,
	).toBeLessThanOrEqual(budget.maxTokens,);
	return metrics.tokens;
}

describe("agent-facing token budgets", () => {
	it("bounds bootstrap and scoped discovery output with a pinned tokenizer", async () => {
		const [
			agentContract,
			fullRegistry,
			datasetResource,
			datasetCreate,
			prettyDatasetCreate,
			usage,
			description,
		] = await Promise.all([
			dss(["agent", "contract", "--json",],),
			dss(["commands", "run", "--json",],),
			dss(["commands", "run", "--fields", "dataset", "--json",],),
			dss(["commands", "run", "--fields", "dataset.create", "--json",],),
			dss(["commands", "run", "--fields", "dataset.create",],),
			dss(["commands", "run", "--fields", "dataset.create.usage", "--json",],),
			dss(["commands", "run", "--fields", "dataset.create.description", "--json",],),
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

		// The intended action-scoped lookup must stay below 1% of the full registry.
		expect(actionTokens * 100,).toBeLessThan(fullRegistryTokens,);
		const prettyActionTokens = measureAgentText(prettyDatasetCreate.stdout,).tokens;
		expect(actionTokens * 100,).toBeLessThan(prettyActionTokens * 70,);
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
});
