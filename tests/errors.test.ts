import { describe, expect, it, } from "bun:test";
import { classifyDataikuError, DataikuError, } from "../src/errors.js";

describe("classifyDataikuError", () => {
	describe("network/transport (status=0)", () => {
		it("treats status 0 as transient and retryable", () => {
			const result = classifyDataikuError(0, "",);
			expect(result.category,).toBe("transient",);
			expect(result.retryable,).toBe(true,);
			expect(result.retryHint.length > 0,).toBeTruthy();
		});
	});

	describe("500 + missing dataset root path", () => {
		it("classifies missing root path as validation, not retryable", () => {
			const result = classifyDataikuError(
				500,
				"The root path of the dataset /data/foo does not exist",
			);
			expect(result.category,).toBe("validation",);
			expect(result.retryable,).toBe(false,);
		});
	});

	describe("500 + not-found-like with entity token", () => {
		it("classifies 'Dataset … was not found' as not_found", () => {
			const result = classifyDataikuError(500, "Dataset my_dataset was not found",);
			expect(result.category,).toBe("not_found",);
			expect(result.retryable,).toBe(false,);
		});

		it("classifies 'Recipe … does not exist' as not_found", () => {
			const result = classifyDataikuError(500, "Recipe xyz does not exist in project",);
			expect(result.category,).toBe("not_found",);
			expect(result.retryable,).toBe(false,);
		});

		it("does NOT classify generic 500 as not_found", () => {
			const result = classifyDataikuError(500, "Something else entirely",);
			expect(result.category,).not.toBe("not_found",);
			expect(result.category,).toBe("transient",);
			expect(result.retryable,).toBe(true,);
		});
	});

	describe("500 + validation-like", () => {
		it("classifies 'Invalid parameter value' as validation", () => {
			const result = classifyDataikuError(500, "Invalid parameter value",);
			expect(result.category,).toBe("validation",);
			expect(result.retryable,).toBe(false,);
		});

		it("classifies 'Illegal argument for column type' as validation", () => {
			const result = classifyDataikuError(500, "Illegal argument for column type",);
			expect(result.category,).toBe("validation",);
			expect(result.retryable,).toBe(false,);
		});
	});

	describe("404", () => {
		it("classifies 404 as not_found", () => {
			const result = classifyDataikuError(404, "not found",);
			expect(result.category,).toBe("not_found",);
			expect(result.retryable,).toBe(false,);
		});

		it("classifies 404 with HTML body as not_found with gateway hint", () => {
			const result = classifyDataikuError(404, "<!doctype html>some gateway page",);
			expect(result.category,).toBe("not_found",);
			expect(result.retryable,).toBe(false,);
			expect(result.retryHint.toLowerCase(),).toContain("gateway",);
		});

		it("uses non-gateway hint for plain 404", () => {
			const result = classifyDataikuError(404, "not found",);
			expect(result.retryHint.toLowerCase(),).not.toContain("gateway",);
		});
	});

	describe("auth errors", () => {
		it("classifies 401 as forbidden", () => {
			const result = classifyDataikuError(401, "",);
			expect(result.category,).toBe("forbidden",);
			expect(result.retryable,).toBe(false,);
		});

		it("classifies 403 as forbidden", () => {
			const result = classifyDataikuError(403, "insufficient permissions",);
			expect(result.category,).toBe("forbidden",);
			expect(result.retryable,).toBe(false,);
		});
	});

	describe("client validation", () => {
		it("classifies 400 as validation", () => {
			const result = classifyDataikuError(400, "bad request",);
			expect(result.category,).toBe("validation",);
			expect(result.retryable,).toBe(false,);
		});

		it("classifies 409 as validation", () => {
			const result = classifyDataikuError(409, "conflict",);
			expect(result.category,).toBe("validation",);
			expect(result.retryable,).toBe(false,);
		});

		it("classifies 422 as validation", () => {
			const result = classifyDataikuError(422, "unprocessable",);
			expect(result.category,).toBe("validation",);
			expect(result.retryable,).toBe(false,);
		});
	});

	describe("transient/retryable", () => {
		it("classifies 429 as transient", () => {
			const result = classifyDataikuError(429, "rate limited",);
			expect(result.category,).toBe("transient",);
			expect(result.retryable,).toBe(true,);
		});

		it("classifies 502 as transient", () => {
			const result = classifyDataikuError(502, "bad gateway",);
			expect(result.category,).toBe("transient",);
			expect(result.retryable,).toBe(true,);
		});

		it("classifies 503 as transient", () => {
			const result = classifyDataikuError(503, "service unavailable",);
			expect(result.category,).toBe("transient",);
			expect(result.retryable,).toBe(true,);
		});

		it("classifies 408 as transient", () => {
			const result = classifyDataikuError(408, "timeout",);
			expect(result.category,).toBe("transient",);
			expect(result.retryable,).toBe(true,);
		});
	});

	describe("unknown", () => {
		it("classifies 301 as unknown", () => {
			const result = classifyDataikuError(301, "",);
			expect(result.category,).toBe("unknown",);
			expect(result.retryable,).toBe(false,);
		});

		it("classifies 418 as unknown", () => {
			const result = classifyDataikuError(418, "teapot",);
			expect(result.category,).toBe("unknown",);
			expect(result.retryable,).toBe(false,);
		});
	});
});

describe("DataikuError", () => {
	it("sets all taxonomy fields on construction", () => {
		const err = new DataikuError(404, "Not Found", "resource missing",);
		expect(err.status,).toBe(404,);
		expect(err.statusText,).toBe("Not Found",);
		expect(err.body,).toBe("resource missing",);
		expect(err.category,).toBe("not_found",);
		expect(err.retryable,).toBe(false,);
		expect(err.retryHint.length > 0,).toBeTruthy();
		expect(err.name,).toBe("DataikuError",);
		expect(err,).toBeInstanceOf(Error,);
	});

	it("includes status, statusText, body summary, category, retryable, and hint in message", () => {
		const err = new DataikuError(500, "Internal Server Error", "Something broke",);
		expect(err.message,).toContain("500",);
		expect(err.message,).toContain("Internal Server Error",);
		expect(err.message,).toContain("Something broke",);
		expect(err.message,).toContain("transient",);
		expect(err.message,).toContain("Retryable: yes",);
		expect(err.message,).toContain("Hint:",);
	});

	it("extracts .message from JSON body as summary", () => {
		const jsonBody = JSON.stringify({ message: "Dataset not configured", },);
		const err = new DataikuError(400, "Bad Request", jsonBody,);
		expect(err.message,).toContain("Dataset not configured",);
		// should use the extracted message, not the raw JSON
		expect(err.message,).not.toContain("{",);
	});

	it("truncates long body with ellipsis", () => {
		const longBody = "x".repeat(300,);
		const err = new DataikuError(500, "Error", longBody,);
		// The summary in the message should be truncated to 200 chars + ellipsis
		expect(err.message,).toContain("x".repeat(200,),);
		expect(err.message,).toContain("…",);
		expect(err.message,).not.toContain("x".repeat(201,),);
	});

	it("shows '(empty response body)' for empty body", () => {
		const err = new DataikuError(500, "Error", "",);
		expect(err.message,).toContain("(empty response body)",);
	});

	it("includes retry metadata in message when provided", () => {
		const retry = {
			method: "GET",
			enabled: true,
			maxAttempts: 3,
			attempts: 3,
			retries: 2,
			delaysMs: [100, 200,],
			timedOut: false,
		};
		const err = new DataikuError(503, "Service Unavailable", "unavailable", retry,);
		expect(err.retry,).toBe(retry,);
		expect(err.message,).toContain("Retry attempts: 3/3",);
		expect(err.message,).toContain("enabled for GET",);
		expect(err.message,).toContain("Retries performed: 2",);
		expect(err.message,).toContain("[100, 200]",);
		expect(err.message,).toContain("Timed out: no",);
	});

	it("omits retry metadata line from message when not provided", () => {
		const err = new DataikuError(500, "Error", "fail",);
		expect(err.message,).not.toContain("Retry attempts:",);
	});
});
