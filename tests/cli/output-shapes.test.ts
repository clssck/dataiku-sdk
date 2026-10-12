// Discovery `outputShape` must describe what the command really prints. The
// shape is inferred per resource.action (src/cli/contract.ts), not from the
// bare action name, because names like `contents`, `usages`, `download` and
// `diff` mean different things on different resources.
import { describe, expect, it, } from "bun:test";
import { buildCommandRegistry, } from "../../src/cli/contract.js";
import { typeBoxCommandOutputSchemas, } from "../../src/cli/output-schemas.js";
import { cliEnv, dss, sendJson, withCliServer, } from "./_harness.js";

type Shape = "object" | "array" | "string" | "void";

const registry = buildCommandRegistry();

const ADVERTISED_SHAPES: Array<[key: string, shape: Shape, why: string,]> = [
	["folder.contents", "array", "returns FolderItem[]",],
	["folder.download", "string", "returns the local path",],
	["plugin.usages", "object", "returns a PluginUsageReport object",],
	["plugin.download", "object", "returns {path, bytes}",],
	["data-quality.project-timeline", "array", "DataQualityTimeline is an array of entries",],
	["project-library.diff", "object", "returns the structured diff report",],
	["project-library.get", "string", "returns the file text",],
	["dataset.download", "object", "returns the download report",],
	["recipe.diff", "string", "returns a line diff",],
	["job.log", "string", "returns log text",],
	["flow-zone.find", "array", "returns matching zones",],
	["code-env.usages", "array", "returns usage entries",],
	["connection.schemas", "array", "returns schema names",],
];

describe("discovery outputShape is resource-scoped", () => {
	for (const [key, shape, why,] of ADVERTISED_SHAPES) {
		it(`${key} advertises ${shape} (${why})`, () => {
			const [resource, action,] = key.split(".",) as [string, string,];
			expect(registry[resource]?.[action]?.outputShape,).toBe(shape,);
		});
	}

	it("agrees with the top-level type of every generated output schema", () => {
		const mismatches: string[] = [];
		for (const [key, schema,] of Object.entries(typeBoxCommandOutputSchemas(),)) {
			const [resource, action,] = key.split(".",) as [string, string,];
			const declared = registry[resource]?.[action]?.outputShape;
			const record = schema as {
				type?: string;
				anyOf?: Array<{ type?: string; }>;
				oneOf?: Array<{ type?: string; }>;
			};
			const types = new Set(
				record.type !== undefined
					? [record.type,]
					: (record.anyOf ?? record.oneOf ?? []).map((branch,) => branch.type),
			);
			if (types.size !== 1 || !types.has(declared,)) {
				mismatches.push(`${key}: outputShape=${declared} schema=${[...types,].join("|",)}`,);
			}
		}
		expect(mismatches,).toEqual([],);
	});
});

describe("CLI output matches the advertised outputShape", () => {
	it("folder contents prints an array", async () => {
		await withCliServer((req, res,) => {
			const path = new URL(req.url ?? "/", "http://localhost",).pathname;
			if (path.endsWith("/managedfolders/",)) {
				sendJson(res, [{ id: "abc12345", name: "raw", type: "Filesystem", },],);
				return;
			}
			if (path.endsWith("/managedfolders/abc12345/contents/",)) {
				sendJson(res, { items: [{ path: "/a.csv", size: 3, lastModified: 1, },], },);
				return;
			}
			sendJson(res, { message: `unexpected ${req.method} ${path}`, }, 500,);
		}, async (url,) => {
			const { stdout, } = await dss(["folder", "contents", "abc12345",], { env: cliEnv(url,), },);
			const parsed: unknown = JSON.parse(stdout,);
			expect(Array.isArray(parsed,),).toBe(true,);
			expect(registry.folder?.contents?.outputShape,).toBe("array",);
		},);
	});

	it("plugin usages prints an object", async () => {
		await withCliServer((req, res,) => {
			const path = new URL(req.url ?? "/", "http://localhost",).pathname;
			if (path === "/public/api/plugins/my-plugin/actions/listUsages") {
				sendJson(res, { usages: [], missingTypes: [], },);
				return;
			}
			sendJson(res, { message: `unexpected ${req.method} ${path}`, }, 500,);
		}, async (url,) => {
			const { stdout, } = await dss(["plugin", "usages", "my-plugin",], { env: cliEnv(url,), },);
			const parsed: unknown = JSON.parse(stdout,);
			expect(parsed !== null && typeof parsed === "object" && !Array.isArray(parsed,),).toBe(true,);
			expect(registry.plugin?.usages?.outputShape,).toBe("object",);
		},);
	});
});
