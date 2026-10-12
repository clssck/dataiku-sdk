import { describe, expect, it, } from "bun:test";
import { suggestSelectors, } from "../../src/cli/selector-suggestions.js";

const ACTIONS: Record<string, string[]> = {
	dataset: ["create", "get", "list", "preview",],
	flow: ["propagate-schema",],
	project: ["get", "list", "map",],
	scenario: ["get", "get-run", "last-runs", "run",],
};

describe("suggestSelectors", () => {
	it("suggests the same action under other resources and keeps the field path", () => {
		expect(suggestSelectors("flow.map.usage", ACTIONS,),).toEqual(["project.map.usage",],);
		expect(suggestSelectors("flow.get", ACTIONS,),).toEqual([
			"dataset.get",
			"project.get",
			"scenario.get",
		],);
	});

	it("suggests close action names of the same resource", () => {
		expect(suggestSelectors("scenario.last-run.usage", ACTIONS,),).toEqual([
			"scenario.last-runs.usage",
		],);
		expect(suggestSelectors("scenario.getrun", ACTIONS,),).toEqual(["scenario.get-run",],);
	});

	it("suggests close resource names and keeps the action", () => {
		expect(suggestSelectors("datsaet.list.usage", ACTIONS,),).toEqual(["dataset.list.usage",],);
		expect(suggestSelectors("scenarios", ACTIONS,),).toEqual(["scenario",],);
	});

	it("returns nothing for a name with no neighbour", () => {
		expect(suggestSelectors("zzzzzz.qqqqqq", ACTIONS,),).toEqual([],);
	});
});
