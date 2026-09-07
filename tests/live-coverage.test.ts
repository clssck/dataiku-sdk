import { describe, expect, it, } from "bun:test";
import { buildCommandRegistry, commandActionSummary, } from "../src/cli/contract.js";
import type { CaseResult, } from "./live-context.js";
import { checkLiveCoverageCatalogue, liveCoverage, } from "./live-coverage.js";

describe("live action coverage", () => {
	it("requires an explicit classification for registry additions and removals", () => {
		const summary = commandActionSummary(buildCommandRegistry(),);
		checkLiveCoverageCatalogue(summary,);
		expect(() =>
			checkLiveCoverageCatalogue({ ...summary, dataset: [...summary.dataset!, "new-action",], },)
		).toThrow();
		expect(() =>
			checkLiveCoverageCatalogue({
				...summary,
				dataset: summary.dataset!.filter(action => action !== "preview"),
			},)
		).toThrow();
	});
	it("does not count a passing plan or unexecuted claim as live behavior", () => {
		const plan: CaseResult = {
			id: "plan",
			status: "passed",
			actions: ["dataset.create",],
			executedActions: [],
			durationMs: 1,
		};
		expect(liveCoverage([plan,], ["core",],).find(row => row.id === "dataset.create")?.status,).toBe(
			"uncovered",
		);
		const behavior: CaseResult = { ...plan, id: "lifecycle", executedActions: ["dataset.create",], };
		expect(liveCoverage([behavior,], ["core",],).find(row => row.id === "dataset.create")?.status,)
			.toBe("passed",);
		const broken: CaseResult = {
			...behavior,
			id: "regression",
			status: "failed",
			error: "Persisted state did not match",
		};
		expect(
			liveCoverage([behavior, broken,], ["core",],).find(row => row.id === "dataset.create")?.status,
		).toBe("failed",);
	});
	it("preserves capability blockers and disabled-profile gaps", () => {
		const blocked: CaseResult = {
			id: "ml",
			status: "blocked",
			actions: ["ml-task.train",],
			executedActions: [],
			durationMs: 1,
			error: "No training license",
			required: false,
		};
		expect(
			liveCoverage([blocked,], ["core", "ml",],).find(row => row.id === "ml-task.train")?.status,
		).toBe("blocked",);
		const unselected = liveCoverage([], ["core",],).find(row => row.id === "ml-task.train")!;
		expect(unselected.status,).toBe("uncovered",);
		expect(unselected.executed,).toBe(false,);
	});
});
