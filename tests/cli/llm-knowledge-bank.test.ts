import { describe, expect, it, } from "bun:test";
import { cliEnv, dss, dssFailure, sendJson, withCliServer, } from "./_harness.js";

describe("LLM and knowledge-bank CLI commands", () => {
	it("plans llm completions with exact endpoint and payload without contacting DSS", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			const plan = JSON.parse(
				(await dss([
					"llm",
					"completions",
					"--data",
					'{"llmId":"openai:openai1:gpt-4","queries":[{"messages":[{"role":"user","content":"Hi"}]}],"settings":{"temperature":0.9}}',
					"--dry-run",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(plan,).toMatchObject({
				plan: true,
				plannedAndDryRun: true,
				resource: "llm",
				action: "completions",
				llmId: "openai:openai1:gpt-4",
				method: "POST",
				endpoint: "/public/api/projects/TEST/llms/completions",
				idempotency: "none",
				exitCodesOnFailure: { usage: 1, error: 2, transient: 3, },
			},);
			expect((plan["payload"] as Record<string, unknown>)["llmId"],).toBe(
				"openai:openai1:gpt-4",
			);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("plans llm completions and embeddings via --plan with the exact validated body and zero HTTP", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			const completionsPlan = JSON.parse(
				(await dss([
					"llm",
					"completions",
					"--data",
					'{"llmId":"openai:openai1:gpt-4","queries":[{"messages":[{"role":"user","content":"Hi"}]}]}',
					"--plan",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(completionsPlan,).toMatchObject({
				plan: true,
				resource: "llm",
				action: "completions",
				llmId: "openai:openai1:gpt-4",
				method: "POST",
				endpoint: "/public/api/projects/TEST/llms/completions",
			},);
			expect(completionsPlan["plannedAndDryRun"],).toBeUndefined();

			const embeddingsPlan = JSON.parse(
				(await dss([
					"llm",
					"embeddings",
					"--data",
					'{"llmId":"openai:openai1:ada002","queries":[{"text":"Hello"}]}',
					"--plan",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(embeddingsPlan,).toMatchObject({
				plan: true,
				resource: "llm",
				action: "embeddings",
				llmId: "openai:openai1:ada002",
				method: "POST",
				endpoint: "/public/api/projects/TEST/llms/embeddings",
			},);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("keeps read actions (llm.list, knowledge-bank.search) free of --plan", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			for (
				const argv of [["llm", "list", "--plan",], [
					"knowledge-bank",
					"search",
					"my-kb",
					"--plan",
					"--data",
					'{"query":"q"}',
				],]
			) {
				const failure = await dssFailure(argv, { env: cliEnv(url,), },);
				expect(failure.code,).toBe(1,);
				expect(JSON.parse(failure.stdout,)["code"],).toBe("usage_error",);
			}
		},);
		expect(requestCount,).toBe(0,);
	});
	it("rejects invalid bodies under --plan identically to live and dry-run with zero HTTP", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			sendJson(res, { unexpected: true, },);
		}, async (url,) => {
			for (
				const argv of [
					["llm", "completions", "--data", '{"llmId":"","queries":[]}', "--plan",],
					["llm", "embeddings", "--data", '{"llmId":"x"}', "--plan",],
				]
			) {
				const failure = await dssFailure(argv, { env: cliEnv(url,), },);
				expect(failure.code,).toBe(1,);
				expect(JSON.parse(failure.stdout,)["code"],).toBe("invalid_flag_value",);
			}
		},);
		expect(requestCount,).toBe(0,);
	});

	it("plans llm embeddings without contacting DSS", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			const plan = JSON.parse(
				(await dss([
					"llm",
					"embeddings",
					"--data",
					'{"llmId":"openai:openai1:ada002","queries":[{"text":"Who wrote Beethoven 9th?"}]}',
					"--dry-run",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(plan,).toMatchObject({
				plan: true,
				plannedAndDryRun: true,
				resource: "llm",
				action: "embeddings",
				llmId: "openai:openai1:ada002",
				method: "POST",
				endpoint: "/public/api/projects/TEST/llms/embeddings",
			},);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("plans knowledge-bank search with encoded id without contacting DSS", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			const plan = JSON.parse(
				(await dss([
					"knowledge-bank",
					"search",
					"team/kb:v2",
					"--data",
					'{"query":"refund policy","params":{"maxDocuments":5}}',
					"--dry-run",
					"--project-key",
					"MY/PROJ",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(plan,).toMatchObject({
				plan: true,
				plannedAndDryRun: true,
				resource: "knowledge-bank",
				action: "search",
				knowledgeBankId: "team/kb:v2",
				method: "POST",
				endpoint: "/public/api/projects/MY%2FPROJ/knowledge-banks/team%2Fkb%3Av2/search",
			},);
			expect(plan["payload"],).toEqual(
				{ query: "refund policy", params: { maxDocuments: 5, }, },
			);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("plans knowledge-bank clear as destructive without contacting DSS", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			const plan = JSON.parse(
				(await dss(["knowledge-bank", "clear", "my-kb", "--dry-run",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(plan,).toMatchObject({
				plan: true,
				plannedAndDryRun: true,
				resource: "knowledge-bank",
				action: "clear",
				knowledgeBankId: "my-kb",
				method: "POST",
				endpoint: "/public/api/projects/TEST/knowledge-banks/my-kb/clear",
				idempotency: "convergent",
			},);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("executes llm list with purpose query parameter without mutating DSS", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/llms/"
				&& url.searchParams.get("purpose",) === "IMAGE_GENERATION"
			) {
				sendJson(res, [{ id: "openai:openai1:dalle", type: "OPENAI", },],);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss(["llm", "list", "--purpose", "IMAGE_GENERATION",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Array<Record<string, unknown>>;
			expect(result,).toEqual([{ id: "openai:openai1:dalle", type: "OPENAI", },],);
		},);
		expect(requests,).toEqual(["GET /public/api/projects/TEST/llms/?purpose=IMAGE_GENERATION",],);
	});

	it("rejects invalid inference bodies identically for live and dry-run before any request", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			sendJson(res, { unexpected: true, },);
		}, async (url,) => {
			for (
				const argv of [
					["llm", "completions", "--data", '{"queries":[{"messages":[]}]}',],
					["llm", "completions", "--data", '{"llmId":"x"}',],
					["llm", "embeddings", "--data", '{"llmId":"x","queries":"no"}',],
					["knowledge-bank", "search", "kb", "--data", '{"params":{}}',],
					["knowledge-bank", "search", "kb", "--data", '{"query":"  "}',],
				]
			) {
				const failure = await dssFailure(argv, { env: cliEnv(url,), },);
				expect(failure.code,).toBe(1,);
				const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
				expect(report["code"],).toBe("invalid_flag_value",);
			}
		},);
		expect(requestCount,).toBe(0,);
	});

	it("executes llm list, completions, and kb search against a live loopback server", async () => {
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/llms/") {
				sendJson(res, [{ id: "openai:openai1:gpt-4", type: "OPENAI", },],);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/llms/completions") {
				sendJson(res, { responses: [{ ok: true, text: "Hello!", },], },);
				return;
			}
			if (req.method === "POST" && url.pathname.endsWith("/search",)) {
				sendJson(res, { documents: [{ text: "doc", metadata: {}, score: 1, },], },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			const listed = JSON.parse(
				(await dss(["llm", "list",], { env: cliEnv(url,), },)).stdout,
			) as Array<Record<string, unknown>>;
			expect(listed,).toHaveLength(1,);

			const completion = JSON.parse(
				(await dss([
					"llm",
					"completions",
					"--data",
					'{"llmId":"openai:openai1:gpt-4","queries":[{"messages":[{"role":"user","content":"Hi"}]}]}',
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect((completion["responses"] as Array<Record<string, unknown>>)[0]!["text"],).toBe("Hello!",);

			const search = JSON.parse(
				(await dss(["knowledge-bank", "search", "my-kb", "--data", '{"query":"q"}',], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect((search["documents"] as Array<Record<string, unknown>>)[0]!["text"],).toBe("doc",);
		},);
	});

	it("executes knowledge-bank clear against a live loopback server", async () => {
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/knowledge-banks/my-kb/clear",);
			sendJson(res, {},);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss(["knowledge-bank", "clear", "my-kb",], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(result,).toEqual({ cleared: "my-kb", resource: "knowledge-bank", },);
		},);
	});
});
