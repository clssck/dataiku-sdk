import { describe, expect, it, } from "bun:test";
import { mkdtempSync, } from "node:fs";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	readBody,
	sendJson,
	tmpdir,
	withCliServer,
} from "./_harness.js";

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
			expect(result.rendering.content,).toContain("style z0 fill:#64748b22,stroke:#64748b",);
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

// raw -> prepare -> clean -> report -> summary; "Raw" holds raw, "Prep" holds
// the prepare unit (recipe + its output), report/summary sit in Default.
const unitGraph = {
	nodes: {
		raw: { type: "COMPUTABLE_DATASET", name: "raw", successors: ["prepare",], },
		prepare: { type: "RECIPE", name: "prepare", successors: ["clean",], },
		clean: { type: "COMPUTABLE_DATASET", name: "clean", successors: ["report",], },
		report: { type: "RECIPE", name: "report", successors: ["summary",], },
		summary: { type: "COMPUTABLE_DATASET", name: "summary", },
	},
};

const unitZones = [
	{
		id: "zone-raw",
		name: "Raw",
		color: "#64748b",
		position: { x: 0, y: 0, },
		items: [{ objectType: "DATASET", objectId: "raw", },],
	},
	{
		id: "zone-prep",
		name: "Prep",
		color: "#0ea5e9",
		position: { x: 100, y: 0, },
		tags: ["etl",],
		items: [
			{ objectType: "RECIPE", objectId: "prepare", },
			{ objectType: "DATASET", objectId: "clean", },
		],
		shared: [],
	},
];

type UnitServerOptions = { manualPositioning?: boolean; };

// Mirrors DSS 15: a zone created while manual positioning is on is auto-placed
// (the requested position is dropped); while it is off, the position is stored.
function unitServer(
	requests: string[],
	bodies: Record<string, unknown>,
	options: UnitServerOptions = {},
) {
	let manual = options.manualPositioning ?? true;
	let newZonePosition: unknown;
	return async (
		req: Parameters<Parameters<typeof withCliServer>[0]>[0],
		res: Parameters<Parameters<typeof withCliServer>[0]>[1],
	) => {
		const url = new URL(req.url ?? "/", "http://localhost",);
		const route = `${req.method} ${url.pathname}`;
		requests.push(route,);
		const body = req.method === "GET" ? undefined : JSON.parse((await readBody(req,)) || "null",);
		if (body !== undefined) bodies[route] = body;
		const base = "/public/api/projects/TEST";
		switch (route) {
			case `GET ${base}/flow/graph/`:
				return sendJson(res, unitGraph,);
			case `GET ${base}/flow/zones`:
				return sendJson(res, unitZones,);
			case `GET ${base}/flow/zones/zone-prep`:
				return sendJson(res, unitZones[1],);
			case `GET ${base}/settings`:
				return sendJson(res, {
					owner: "admin",
					settings: {
						flowDisplaySettings: { zonesGraphConnectZones: true, zonesManualPositioning: manual, },
					},
				},);
			case `PUT ${base}/settings`:
				manual = body.settings.flowDisplaySettings.zonesManualPositioning;
				requests[requests.length - 1] = `${route} manual=${manual}`;
				res.statusCode = 204;
				return res.end();
			case `POST ${base}/flow/zones`:
				newZonePosition = manual ? { x: 628, y: 0, width: 428, height: 428, } : body.position;
				return sendJson(res, {
					id: "zone-new",
					...body,
					position: newZonePosition,
					items: [],
					shared: [],
					tags: [],
				},);
			case `PUT ${base}/flow/zones/zone-new`:
			case `DELETE ${base}/flow/zones/zone-prep`:
				res.statusCode = 204;
				return res.end();
			case `POST ${base}/flow/zones/zone-new/add-items`:
			case `GET ${base}/flow/zones/zone-new`:
				return sendJson(res, {
					id: "zone-new",
					name: "Prep",
					color: "#0ea5e9",
					position: newZonePosition,
					tags: ["etl",],
					items: unitZones[1]!.items,
				},);
			default:
				res.statusCode = 500;
				res.end(`unexpected ${route}`,);
		}
	};
}

describe("CLI flow-zone organize recipe units and positions", () => {
	it("rejects a plan that splits a recipe from its output, before any mutation", async () => {
		const requests: string[] = [];
		await withCliServer(unitServer(requests, {},), async (url,) => {
			const plan = {
				zones: [{ name: "Raw", datasets: ["clean",], }, { name: "Prep", recipes: ["prepare",], },],
			};
			const failure = await dssFailure(["flow-zone", "organize", "--data", JSON.stringify(plan,),], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(1,);
			const error = JSON.parse(failure.stdout,) as {
				code: string;
				details: { conflicts: Array<{ recipe: string; zones: Record<string, string[]>; }>; };
			};
			expect(error.code,).toBe("validation_failed",);
			expect(error.details.conflicts,).toEqual([{
				recipe: "prepare",
				zones: { Raw: ["clean",], Prep: ["prepare",], },
			},],);
		},);
		expect(requests.every((request,) => request.startsWith("GET ",)),).toBe(true,);
	});

	it("moves a recipe's outputs with it and never prunes them under --sync", async () => {
		await withCliServer(unitServer([], {},), async (url,) => {
			const plan = {
				zones: [{ name: "Prep", recipes: ["prepare",], }, { name: "Report", recipes: ["report",], },],
			};
			const preview = JSON.parse(
				(await dss(["flow-zone", "organize", "--data", JSON.stringify(plan,), "--sync", "--dry-run",], {
					env: cliEnv(url,),
				},)).stdout,
			) as {
				pruneItemCount: number;
				planned: Array<{ moveItems: unknown[]; impliedItems?: unknown[]; pruneItems?: unknown[]; }>;
			};
			// clean is already in Prep; listing only its recipe must not prune it.
			expect(preview.pruneItemCount,).toBe(0,);
			expect(preview.planned[0],).toMatchObject({
				moveItems: [],
				impliedItems: [{ objectType: "DATASET", objectId: "clean", },],
			},);
			expect(preview.planned[1]!.moveItems,).toEqual([
				{ objectType: "RECIPE", objectId: "report", },
				{ objectType: "DATASET", objectId: "summary", },
			],);
		},);
	});

	it("plan lists default-zone recipes and sources still to place, not recipe outputs", async () => {
		await withCliServer(unitServer([], {},), async (url,) => {
			const plan = JSON.parse(
				(await dss(["flow-zone", "plan",], { env: cliEnv(url,), },)).stdout,
			) as {
				unassigned: unknown[];
			};
			expect(plan.unassigned,).toEqual([{ objectType: "RECIPE", objectId: "report", },],);
		},);
	});

	it("refuses to move an existing zone without --recreate-on-move", async () => {
		const requests: string[] = [];
		await withCliServer(unitServer(requests, {},), async (url,) => {
			const plan = { zones: [{ id: "zone-prep", name: "Prep", position: { x: 5, y: 6, }, },], };
			const failure = await dssFailure(["flow-zone", "organize", "--data", JSON.stringify(plan,),], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(1,);
			const error = JSON.parse(failure.stdout,) as {
				code: string;
				details: { zones: Array<{ id: string; position: unknown; requestedPosition: unknown; }>; };
			};
			expect(error.code,).toBe("validation_failed",);
			expect(error.details.zones,).toMatchObject([{
				id: "zone-prep",
				position: { x: 100, y: 0, },
				requestedPosition: { x: 5, y: 6, },
			},],);
		},);
		expect(requests.every((request,) => request.startsWith("GET ",)),).toBe(true,);
	});

	it("--recreate-on-move places the replacement zone with manual positioning briefly off, keeping items and metadata", async () => {
		const requests: string[] = [];
		const bodies: Record<string, unknown> = {};
		await withCliServer(unitServer(requests, bodies,), async (url,) => {
			const plan = { zones: [{ id: "zone-prep", name: "Prep", position: { x: 5, y: 6, }, },], };
			const { stdout, stderr, } = await dss([
				"flow-zone",
				"organize",
				"--data",
				JSON.stringify(plan,),
				"--recreate-on-move",
			], { env: cliEnv(url,), },);
			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as {
				recreated: Array<{ replacedZoneId: string; zone: { id: string; position: unknown; }; }>;
				moved: unknown[];
			};
			expect(result.recreated,).toMatchObject([{
				replacedZoneId: "zone-prep",
				zone: { id: "zone-new", position: { x: 5, y: 6, }, },
			},],);
			expect(result.moved,).toEqual([],);
		},);
		const base = "/public/api/projects/TEST/flow/zones";
		const settings = "/public/api/projects/TEST/settings";
		expect(requests.filter((request,) => !request.startsWith("GET ",)),).toEqual([
			`PUT ${settings} manual=false`,
			`POST ${base}`,
			`PUT ${base}/zone-new`,
			`POST ${base}/zone-new/add-items`,
			`DELETE ${base}/zone-prep`,
			`PUT ${settings} manual=true`,
		],);
		// Only the positioning flag changes; other settings round-trip.
		expect(bodies[`PUT ${settings}`],).toEqual({
			owner: "admin",
			settings: {
				flowDisplaySettings: { zonesGraphConnectZones: true, zonesManualPositioning: true, },
			},
		},);
		expect(bodies[`POST ${base}`],).toEqual({
			name: "Prep",
			color: "#0ea5e9",
			position: { x: 5, y: 6, },
		},);
		expect(bodies[`PUT ${base}/zone-new`],).toMatchObject({ id: "zone-new", tags: ["etl",], },);
		expect(bodies[`POST ${base}/zone-new/add-items`],).toEqual(unitZones[1]!.items,);
	});

	it("warns when written zone positions will not display", async () => {
		await withCliServer(unitServer([], {}, { manualPositioning: false, },), async (url,) => {
			const plan = {
				zones: [{ name: "Report", position: { x: 300, y: 0, }, recipes: ["report",], },],
			};
			const { stderr, } = await dss([
				"flow-zone",
				"organize",
				"--data",
				JSON.stringify(plan,),
				"--dry-run",
			], {
				env: cliEnv(url,),
			},);
			expect(stderr,).toContain("zone_manual_positioning_disabled",);
			expect(stderr,).toContain("zonesManualPositioning",);
		},);
	});
});
