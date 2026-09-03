import { describe, expect, it, } from "bun:test";
import { mkdtempSync, } from "node:fs";
import { cliEnv, dss, dssFailure, join, sendJson, tmpdir, withCliServer, } from "./_harness.js";

const flowGraph = {
	nodes: {
		raw: { type: "COMPUTABLE_DATASET", name: "raw", successors: ["prepare",], },
		prepare: { type: "RECIPE", name: "prepare", },
	},
};

function visualizationServer(
	req: Parameters<Parameters<typeof withCliServer>[0]>[0],
	res: Parameters<Parameters<typeof withCliServer>[0]>[1],
) {
	const url = new URL(req.url ?? "/", "http://localhost",);
	if (req.method !== "GET") {
		res.statusCode = 500;
		res.end("unexpected mutation",);
		return;
	}
	switch (url.pathname) {
		case "/public/api/projects/TEST/flow/graph/":
			sendJson(res, flowGraph,);
			return;
		case "/public/api/projects/TEST/flow/zones":
			sendJson(res, [
				{
					id: "zone-raw",
					name: "Raw & Sources",
					color: "#64748b",
					position: { x: 100, y: 200, },
					items: [{ objectType: "DATASET", objectId: "raw", },],
				},
			],);
			return;
		case "/public/api/projects/TEST/managedfolders/":
			sendJson(res, [],);
			return;
		case "/public/api/projects/TEST/datasets/":
			sendJson(res, [{ name: "raw", },],);
			return;
		case "/public/api/projects/TEST/recipes/":
			sendJson(res, [{ name: "prepare", },],);
			return;
		default:
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
	}
}

describe("CLI flow visualization", () => {
	it("project map joins zones and returns Mermaid rendering", async () => {
		await withCliServer(visualizationServer, async (url,) => {
			const result = JSON.parse(
				(await dss([
					"project",
					"map",
					"--render",
					"mermaid",
				], { env: cliEnv(url,), },)).stdout,
			) as {
				map: {
					nodes: Array<{ id: string; layer: number; zoneId: string; }>;
					zones: Array<{ id: string; position?: { x: number; y: number; }; }>;
					components: unknown[];
					diagnostics: Array<{ code: string; }>;
					topologyFingerprint: string;
				};
				rendering: { format: string; content: string; };
			};
			const byId = new Map(result.map.nodes.map((node,) => [node.id, node,]),);

			expect(byId.get("raw",),).toMatchObject({ layer: 0, zoneId: "zone-raw", },);
			expect(byId.get("prepare",),).toMatchObject({ layer: 1, zoneId: "default", },);
			expect(result.map.zones.find((zone,) => zone.id === "zone-raw")?.position,).toEqual({
				x: 100,
				y: 200,
			},);
			expect(result.map.components,).toHaveLength(1,);
			expect(result.map.diagnostics.map((diagnostic,) => diagnostic.code),).toEqual([
				"cross_zone_edges",
				"default_zone_items",
			],);
			expect(result.map.topologyFingerprint,).toMatch(/^[0-9a-f]{64}$/,);
			expect(result.rendering,).toMatchObject({ format: "mermaid", },);
			expect(result.rendering.content,).toContain("Raw &amp; Sources",);
		},);
	});

	it("flow-zone plan round-trips through organize without redundant moves", async () => {
		await withCliServer(visualizationServer, async (url,) => {
			const plan = JSON.parse(
				(await dss(["flow-zone", "plan",], { env: cliEnv(url,), },)).stdout,
			) as {
				topologyFingerprint: string;
				zones: Array<{ id: string; name: string; items: unknown[]; }>;
			};
			expect(plan.topologyFingerprint,).toMatch(/^[0-9a-f]{64}$/,);
			expect(plan.zones.find((zone,) => zone.id === "zone-raw"),).toMatchObject({
				name: "Raw & Sources",
				items: [{ objectType: "DATASET", objectId: "raw", },],
			},);

			const preview = JSON.parse(
				(await dss([
					"flow-zone",
					"organize",
					"--data",
					JSON.stringify(plan,),
					"--dry-run",
				], { env: cliEnv(url,), },)).stdout,
			) as {
				dryRun: boolean;
				topologyFingerprint: string;
				planned: Array<{ moveItems: unknown[]; }>;
			};
			expect(preview.dryRun,).toBe(true,);
			expect(preview.topologyFingerprint,).toBe(plan.topologyFingerprint,);
			expect(preview.planned.every((step,) => step.moveItems.length === 0),).toBe(true,);
		},);
	});

	it("round-trips projects with no custom zones as an empty no-op plan", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/zones") {
				sendJson(res, [],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/graph/") {
				sendJson(res, flowGraph,);
				return;
			}
			res.statusCode = 500;
			res.end("unexpected request",);
		}, async (url,) => {
			const plan = JSON.parse(
				(await dss(["flow-zone", "plan",], { env: cliEnv(url,), },)).stdout,
			) as { zones: unknown[]; };
			expect(plan.zones,).toEqual([],);

			const preview = JSON.parse(
				(await dss([
					"flow-zone",
					"organize",
					"--data",
					JSON.stringify(plan,),
					"--dry-run",
				], { env: cliEnv(url,), },)).stdout,
			) as {
				zoneCount: number;
				planned: unknown[];
			};
			expect(preview,).toMatchObject({ zoneCount: 0, planned: [], },);
		},);
	});

	it("rejects a stale organization plan before mutation", async () => {
		await withCliServer(visualizationServer, async (url,) => {
			const stale = {
				topologyFingerprint: "0".repeat(64,),
				zones: [{ id: "zone-raw", name: "Raw & Sources", items: [], },],
			};
			const failure = await dssFailure([
				"flow-zone",
				"organize",
				"--data",
				JSON.stringify(stale,),
			], { env: cliEnv(url,), },);

			expect(failure.code,).toBe(4,);
			expect(JSON.parse(failure.stdout,),).toMatchObject({
				code: "assertion_failed",
				category: "dss",
				exitCode: 4,
				retryable: false,
			},);
			expect(failure.stdout,).toContain("Flow topology changed",);
			expect(failure.stdout,).toContain("Regenerate the plan",);
		},);
	});

	it("rejects unsupported project map render formats locally, before credentials", async () => {
		// Hermetic: no .env, no DATAIKU_* vars, no saved credentials. The usage
		// error must win over "Missing Dataiku URL".
		const failure = await dssFailure(["project", "map", "--render", "svg",], {
			env: {
				...process.env,
				DSS_CONFIG_DIR: mkdtempSync(join(tmpdir(), "dss-cli-render-",),),
				DATAIKU_DISABLE_ENV: "1",
				DATAIKU_URL: "",
				DATAIKU_API_KEY: "",
			},
		},);
		expect(failure.code,).toBe(1,);
		expect(JSON.parse(failure.stdout,),).toMatchObject({ code: "invalid_enum", },);
		expect(failure.stdout,).toContain("--render must be ascii or mermaid",);
	});
});
