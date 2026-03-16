import { describe, expect, it, } from "bun:test";
import { DataikuClient, } from "../src/client.js";
import { ProjectSummaryArraySchema, ProjectSummarySchema, } from "../src/schemas.js";

const client = new DataikuClient({ url: "http://localhost:0", apiKey: "test", },);

describe("DataikuClient.parse()", () => {
	it("returns valid object unchanged", () => {
		const input = { projectKey: "X", name: "Y", };
		const result = client.parse(ProjectSummarySchema, input,);
		expect(result,).toEqual(input,);
	});

	it("preserves extra fields not in the schema", () => {
		const input = { projectKey: "X", name: "Y", extra: 1, };
		const result = client.parse(ProjectSummarySchema, input,);
		expect((result as Record<string, unknown>).extra,).toBe(1,);
	});

	it("throws on invalid data (wrong type for field)", () => {
		expect(
			() => client.parse(ProjectSummarySchema, { projectKey: 123, },),
		).toThrow();
	});

	it("returns valid array", () => {
		const input = [{ projectKey: "X", name: "Y", },];
		const result = client.parse(ProjectSummaryArraySchema, input,);
		expect(result,).toEqual(input,);
	});

	it("accepts empty array as valid", () => {
		const result = client.parse(ProjectSummaryArraySchema, [],);
		expect(result,).toEqual([],);
	});

	it("throws when data is a string instead of object", () => {
		expect(
			() => client.parse(ProjectSummarySchema, "string",),
		).toThrow();
	});

	it("throws when data is null", () => {
		expect(
			() => client.parse(ProjectSummarySchema, null,),
		).toThrow();
	});
});
