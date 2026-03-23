import { describe, expect, it, } from "bun:test";
import { DataikuClient, } from "../src/client.js";
import {
	ProjectSummaryArraySchema,
	ProjectSummarySchema,
	safeParseSchema,
} from "../src/schemas.js";

describe("safeParseSchema", () => {
	it("returns success=true for valid data", () => {
		const data = { projectKey: "X", name: "Y", extra: 1, };
		const result = safeParseSchema(ProjectSummarySchema, data,);
		expect(result.success,).toBe(true,);
		expect(result.data,).toEqual(data,);
	});

	it("returns success=false with errors for invalid data", () => {
		const data: unknown = { projectKey: 123, };
		const result = safeParseSchema(ProjectSummarySchema, data,);
		expect(result.success,).toBe(false,);
		expect(result.data as unknown,).toBe(data,);
		if (!result.success) {
			expect(result.errors.length,).toBeGreaterThan(0,);
			expect(result.errors.some((e,) => e.includes("/projectKey",)),).toBe(true,);
			expect(result.errors.some((e,) => e.includes("/name",)),).toBe(true,);
		}
	});

	it("preserves extra fields in data on failure", () => {
		const data = { projectKey: 123, name: "Y", extra: "kept", };
		const result = safeParseSchema(ProjectSummarySchema, data,);
		expect(result.success,).toBe(false,);
		expect((result.data as Record<string, unknown>).extra,).toBe("kept",);
	});

	it("returns success=true for valid array", () => {
		const data = [{ projectKey: "A", name: "A", },];
		const result = safeParseSchema(ProjectSummaryArraySchema, data,);
		expect(result.success,).toBe(true,);
	});

	it("returns success=false for array with invalid element", () => {
		const data = [{ projectKey: "A", name: "A", }, { bad: true, },];
		const result = safeParseSchema(ProjectSummaryArraySchema, data,);
		expect(result.success,).toBe(false,);
		if (!result.success) {
			expect(result.errors.some((e,) => e.includes("/1",)),).toBe(true,);
		}
	});

	it("does not throw while formatting bigint validation errors", () => {
		const data: unknown = 1n;
		const invoke = () => safeParseSchema(ProjectSummarySchema, data,);
		expect(invoke,).not.toThrow();
		const result = invoke();
		expect(result.success,).toBe(false,);
		expect(result.data as unknown,).toBe(data,);
		if (!result.success) {
			expect(result.errors.some((e,) => e.includes("1n",)),).toBe(true,);
			expect(result.errors.some((e,) => e.includes("Expected",)),).toBe(true,);
		}
	});
});

describe("DataikuClient.safeParse", () => {
	const client = new DataikuClient({ url: "http://localhost:0", apiKey: "test", },);

	it("returns data for valid input without warning", () => {
		const data = { projectKey: "X", name: "Y", };
		const result = client.safeParse(ProjectSummarySchema, data, "test.method",);
		expect(result,).toEqual(data,);
	});

	it("returns data for invalid input (does not throw)", () => {
		const data: unknown = { projectKey: 123, };
		const result = client.safeParse(ProjectSummarySchema, data, "test.method",);
		expect(result as unknown,).toBe(data,);
	});

	it("fires onValidationWarning callback on mismatch", () => {
		const warnings: { method: string; errors: string[]; }[] = [];
		const warnClient = new DataikuClient({
			url: "http://localhost:0",
			apiKey: "test",
			onValidationWarning: (method, errors,) => {
				warnings.push({ method, errors, },);
			},
		},);

		warnClient.safeParse(ProjectSummarySchema, { bad: true, }, "datasets.list",);
		expect(warnings,).toHaveLength(1,);
		expect(warnings[0].method,).toBe("datasets.list",);
		expect(warnings[0].errors.length,).toBeGreaterThan(0,);
	});

	it("warns on bigint mismatches without throwing", () => {
		const warnings: { method: string; errors: string[]; }[] = [];
		const data: unknown = 1n;
		const warnClient = new DataikuClient({
			url: "http://localhost:0",
			apiKey: "test",
			onValidationWarning: (method, errors,) => {
				warnings.push({ method, errors, },);
			},
		},);

		expect(() => {
			const result = warnClient.safeParse(ProjectSummarySchema, data, "datasets.list",);
			expect(result as unknown,).toBe(data,);
		},).not.toThrow();
		expect(warnings,).toHaveLength(1,);
		expect(warnings[0].method,).toBe("datasets.list",);
		expect(warnings[0].errors.some((e,) => e.includes("1n",)),).toBe(true,);
		expect(warnings[0].errors.some((e,) => e.includes("Expected",)),).toBe(true,);
	});

	it("does not fire callback on valid data", () => {
		const warnings: unknown[] = [];
		const warnClient = new DataikuClient({
			url: "http://localhost:0",
			apiKey: "test",
			onValidationWarning: () => {
				warnings.push(true,);
			},
		},);

		warnClient.safeParse(ProjectSummarySchema, { projectKey: "X", name: "Y", }, "test",);
		expect(warnings,).toHaveLength(0,);
	});

	it("strict mode: throwing callback propagates error", () => {
		const strictClient = new DataikuClient({
			url: "http://localhost:0",
			apiKey: "test",
			onValidationWarning: (method, errors,) => {
				throw new Error(`Strict validation failed in ${method}: ${errors.join(", ",)}`,);
			},
		},);

		expect(() => {
			strictClient.safeParse(ProjectSummarySchema, { bad: true, }, "test.strict",);
		},).toThrow("Strict validation failed",);
	});
});
