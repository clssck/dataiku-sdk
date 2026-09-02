import { describe, expect, it, } from "bun:test";
import {
	canonicalStatusText,
	classifyDataikuError,
	DataikuError,
	dataikuErrorCode,
} from "../src/errors.js";

describe("classifyDataikuError", () => {
	describe("network/transport (status=0)", () => {
		it("treats status 0 as transient and retryable", () => {
			const result = classifyDataikuError(0, "",);
			expect(result.category,).toBe("transient",);
			expect(result.retryable,).toBe(true,);
			expect(result.retryHint.length > 0,).toBeTruthy();
		});

		it("classifies TLS certificate failures with trust guidance", () => {
			const result = classifyDataikuError(0, "unable to verify the first certificate",);
			expect(result.category,).toBe("validation",);
			expect(result.retryable,).toBe(false,);
			expect(result.retryHint,).toContain("--ca-cert PATH",);
			expect(result.retryHint,).toContain("NODE_EXTRA_CA_CERTS",);
			expect(result.retryHint,).toContain("--insecure",);
		});
	});

	describe("recipe creation diagnostics", () => {
		it("classifies S3 SQL recipe input mismatch as validation with a fallback hint", () => {
			const result = classifyDataikuError(
				500,
				"S3 dataset long_with_attribute_names is not associated to an Athena connection",
			);
			expect(result.category,).toBe("validation",);
			expect(result.retryable,).toBe(false,);
			expect(result.retryHint,).toContain("SQL/Athena-backed input datasets",);
			expect(result.retryHint,).toContain("Python recipe",);
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

		it("classifies DSS JSON-escaped missing code-env contraction as not_found", () => {
			const result = classifyDataikuError(
				500,
				String
					.raw`{"errorType":"com.dataiku.dip.exceptions.CodedIOException","message":"PYTHON env my_env doesn\u0027t exist"}`,
			);
			expect(result.category,).toBe("not_found",);
			expect(result.retryable,).toBe(false,);
		});

		it("classifies server package and directory filesystem misses as not_found", () => {
			for (
				const body of [
					"Package code-env.zip does not exist",
					"Directory /opt/dataiku/dss/tmp/missing does not exist",
					"Not a file: /opt/dataiku/dss/tmp/desc.json",
				]
			) {
				const result = classifyDataikuError(500, body,);
				expect(result.category,).toBe("not_found",);
				expect(dataikuErrorCode(result.category,),).toBe("not_found",);
				expect(result.retryable,).toBe(false,);
			}
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

		it("classifies project library directory-as-file 500 as validation", () => {
			const result = classifyDataikuError(
				500,
				"Cannot read directory as file: /projects/MYPROJECT/lib/python",
			);
			expect(result.category,).toBe("validation",);
			expect(result.retryable,).toBe(false,);
			expect(dataikuErrorCode(result.category,),).toBe("validation_failed",);
		});
	});

	describe("Athena SQL engine errors", () => {
		it("classifies COLUMN_NOT_FOUND as deterministic validation", () => {
			const result = classifyDataikuError(
				500,
				"COLUMN_NOT_FOUND: Column 'task_name' cannot be resolved",
			);
			expect(result.category,).toBe("validation",);
			expect(result.retryable,).toBe(false,);
			expect(result.retryHint,).toContain("column names",);
		});

		it("classifies TABLE_NOT_FOUND as deterministic validation", () => {
			const result = classifyDataikuError(500, "TABLE_NOT_FOUND: Table analytics.orders not found",);
			expect(result.category,).toBe("validation",);
			expect(result.retryable,).toBe(false,);
			expect(result.retryHint,).toContain("table names",);
		});
	});
	describe("500 + permission-like", () => {
		it("classifies code env permission failures as forbidden", () => {
			const result = classifyDataikuError(
				500,
				"Cannot use code env foo because of permission restrictions",
			);
			expect(result.category,).toBe("forbidden",);
			expect(result.retryable,).toBe(false,);
		});

		it("classifies access-denied server errors as forbidden", () => {
			const result = classifyDataikuError(500, "User is not allowed to access this dataset",);
			expect(result.category,).toBe("forbidden",);
			expect(result.retryable,).toBe(false,);
		});

		it("classifies license-denied server errors as forbidden and non-retryable", () => {
			const result = classifyDataikuError(
				500,
				"Current license does not allow this operation",
			);
			expect(result.category,).toBe("forbidden",);
			expect(dataikuErrorCode(result.category,),).toBe("permission_denied",);
			expect(result.retryable,).toBe(false,);
		});

		it("keeps validation-like 500s classified as validation", () => {
			const result = classifyDataikuError(500, "Invalid permissions payload",);
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

		it("uses Business Apps API hint for root endpoint 404", () => {
			const result = classifyDataikuError(404, "Not Found: /dip/publicapi/business-apps/",);
			expect(result.category,).toBe("not_found",);
			expect(result.retryable,).toBe(false,);
			expect(result.retryHint,).toContain("Business Apps API is not available",);
			expect(result.retryHint,).toContain("classic app commands",);
		});

		it("keeps object-specific Business App 404s on the generic not-found hint", () => {
			const result = classifyDataikuError(
				404,
				"Not Found: /public/api/business-apps/missing/settings",
			);
			expect(result.category,).toBe("not_found",);
			expect(result.retryHint,).not.toContain("Business Apps API is not available",);
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
	});

	describe("unexpected_response", () => {
		it("classifies unhandled 4xx statuses as non-retryable unexpected_response", () => {
			for (const status of [405, 406, 410, 412, 413, 415, 416, 418, 426, 440, 451,]) {
				const result = classifyDataikuError(status, "rejected",);
				expect(result.category, `status ${status}`,).toBe("unexpected_response",);
				expect(result.retryable, `status ${status}`,).toBe(false,);
				expect(dataikuErrorCode(result.category,), `status ${status}`,)
					.toBe("unexpected_response",);
				expect(result.retryHint,).toContain("unrecognized HTTP status",);
			}
		});

		it("keeps the transport-based 5xx catch-all transient after the unexpected-response fallback", () => {
			const result = classifyDataikuError(500, "plain server error",);
			expect(result.category,).toBe("transient",);
			expect(result.retryable,).toBe(true,);
		});

		it("classifies a non-JSON 2xx body as unexpected_response with proxy/login hint", () => {
			const result = classifyDataikuError(
				200,
				"Expected JSON response body but got non-JSON content: <!doctype html>login",
			);
			expect(result.category,).toBe("unexpected_response",);
			expect(result.retryable,).toBe(false,);
			expect(result.retryHint.toLowerCase(),).toContain("proxy",);
			expect(result.retryHint.toLowerCase(),).toContain("login page",);
		});

		it("leaves ordinary 2xx responses out of the non-JSON check", () => {
			const result = classifyDataikuError(200, '{"ok":true}',);
			expect(result.category,).toBe("unknown",);
		});

		it("surfaces unexpected_response on a constructed DataikuError", () => {
			const err = new DataikuError(405, "Method Not Allowed", "method rejected",);
			expect(err.category,).toBe("unexpected_response",);
			expect(err.retryable,).toBe(false,);
			expect(err.safeMessage,).toContain("Error type: unexpected_response",);
			expect(err.safeMessage,).toContain("405 Method Not Allowed",);
		});

		it("maps canonical status text for the newly covered 4xx codes", () => {
			expect(canonicalStatusText(405,),).toBe("Method Not Allowed",);
			expect(canonicalStatusText(406,),).toBe("Not Acceptable",);
			expect(canonicalStatusText(410,),).toBe("Gone",);
			expect(canonicalStatusText(412,),).toBe("Precondition Failed",);
			expect(canonicalStatusText(413,),).toBe("Payload Too Large",);
			expect(canonicalStatusText(415,),).toBe("Unsupported Media Type",);
			expect(canonicalStatusText(416,),).toBe("Range Not Satisfiable",);
			expect(canonicalStatusText(418,),).toBe("I'm a Teapot",);
			expect(canonicalStatusText(426,),).toBe("Upgrade Required",);
			expect(canonicalStatusText(451,),).toBe("Unavailable For Legal Reasons",);
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
	it("provides a body-independent safe message", () => {
		const err = new DataikuError(
			502,
			"REMOTE_STATUS_TEXT_SECRET",
			JSON.stringify({ message: "REMOTE_SECRET", apiKey: "TOKEN_SECRET", },),
		);
		expect(err.safeMessage,).toContain("502 Bad Gateway",);
		expect(err.safeMessage,).toContain("Error type: transient",);
		expect(err.safeMessage,).toContain("Retryable: yes",);
		expect(err.safeMessage,).not.toContain("REMOTE_SECRET",);
		expect(err.safeMessage,).not.toContain("TOKEN_SECRET",);
		expect(err.safeMessage,).not.toContain("REMOTE_STATUS_TEXT_SECRET",);
	});

	it("summarizes HTML error pages without leaking raw markup or server paths", () => {
		const err = new DataikuError(
			500,
			"Internal Server Error",
			"<!doctype html><html><body><h1>Failure in /opt/dataiku/dss/run/backend.log</h1>"
				+ "<pre>/opt/dataiku/dss/run/install.ini</pre></body></html>",
		);
		expect(err.message,).toContain("HTML error page from DSS",);
		expect(err.message,).toContain("[server path]",);
		expect(err.message.toLowerCase(),).not.toContain("<html",);
		expect(err.message.toLowerCase(),).not.toContain("<body",);
		expect(err.message,).not.toContain("/opt/dataiku",);
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
