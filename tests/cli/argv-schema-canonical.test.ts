import { describe, expect, it, } from "bun:test";
import { buildAgentContract, buildCommandRegistry, } from "../../src/cli/contract.js";
import {
	cliEnv,
	dss,
	join,
	mkdirSync,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

type Schema = Record<string, unknown>;

/**
 * Minimal validator for the JSON Schema subset the argv schemas emit:
 * type/const/enum/pattern/not/anyOf/allOf, array items/prefixItems/minItems/
 * contains+minContains, and object required/properties/additionalProperties.
 * Deterministic and dependency-free: it validates the structures the contract
 * actually produces, not a specific validator implementation.
 */
function validates(schema: Schema, value: unknown,): boolean {
	const type = schema["type"];
	if (schema["enum"] !== undefined) {
		if (
			!(schema["enum"] as unknown[]).some((entry,) =>
				JSON.stringify(entry,) === JSON.stringify(value,)
			)
		) {
			return false;
		}
	}
	if (schema["const"] !== undefined) {
		if (JSON.stringify(schema["const"],) !== JSON.stringify(value,)) return false;
	}
	if (typeof value === "string" && typeof schema["pattern"] === "string") {
		let regex: RegExp;
		try {
			regex = new RegExp(schema["pattern"],);
		} catch {
			return false;
		}
		if (!regex.test(value,)) return false;
	}
	if (type === "string") {
		if (typeof value !== "string") return false;
	} else if (type === "array") {
		if (!Array.isArray(value,)) return false;
		const prefixItems = schema["prefixItems"] as Schema[] | undefined;
		if (prefixItems) {
			for (let index = 0; index < prefixItems.length; index++) {
				if (index >= value.length) break;
				if (!validates(prefixItems[index]!, value[index],)) return false;
			}
		}
		if (typeof schema["minItems"] === "number" && value.length < (schema["minItems"] as number)) {
			return false;
		}
		if (typeof schema["items"] === "object" && schema["items"] !== null) {
			for (const item of value as unknown[]) {
				if (!validates(schema["items"] as Schema, item,)) return false;
			}
		}
		if (typeof schema["contains"] === "object" && schema["contains"] !== null) {
			const min = typeof schema["minContains"] === "number" ? (schema["minContains"] as number) : 1;
			const matches = (value as unknown[]).filter((item,) =>
				validates(schema["contains"] as Schema, item,)
			).length;
			if (matches < min) return false;
		}
	} else if (type === "object") {
		if (typeof value !== "object" || value === null || Array.isArray(value,)) return false;
		const record = value as Record<string, unknown>;
		for (const key of (schema["required"] as string[] | undefined) ?? []) {
			if (!(key in record)) return false;
		}
		const properties = (schema["properties"] as Record<string, Schema> | undefined) ?? {};
		for (const [key, sub,] of Object.entries(properties,)) {
			if (key in record && !validates(sub, record[key],)) return false;
		}
		const additional = schema["additionalProperties"];
		if (additional === false) {
			for (const key of Object.keys(record,)) {
				if (!(key in properties)) return false;
			}
		} else if (typeof additional === "object" && additional !== null) {
			for (const [key, val,] of Object.entries(record,)) {
				if (!(key in properties) && !validates(additional as Schema, val,)) return false;
			}
		}
	} else if (type !== undefined && typeof value !== type) {
		return false;
	}
	if (schema["not"] !== undefined) {
		if (validates(schema["not"] as Schema, value,)) return false;
	}
	if (schema["allOf"] !== undefined) {
		for (const sub of schema["allOf"] as Schema[]) {
			if (!validates(sub, value,)) return false;
		}
	}
	if (schema["anyOf"] !== undefined) {
		const subs = schema["anyOf"] as Schema[];
		if (subs.length > 0 && !subs.some((sub,) => validates(sub, value,))) return false;
	}
	return true;
}

function argvValidates(argvSchema: Schema | undefined, argv: string[],): boolean {
	if (!argvSchema) throw new Error("missing argv schema",);
	return validates(argvSchema, argv,);
}

function argvSchemaOf(entry: { schemas?: { argv?: Schema; }; } | undefined,): Schema | undefined {
	return (entry?.schemas?.argv?.properties as Schema | undefined)?.["argv"] as Schema | undefined;
}

function prefixOf(argv: Schema | undefined,): Schema[] {
	return (argv?.prefixItems as Schema[] | undefined) ?? [];
}

describe("argv schema canonical form", () => {
	const registry = buildCommandRegistry();

	it("every structured example argv validates against its own generated schema", () => {
		const failures: string[] = [];
		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const [action, entry,] of Object.entries(actions,)) {
				for (const example of entry.structuredExamples ?? []) {
					if (!example.argv) continue;
					if (!validates(entry.schemas.argv, { argv: example.argv, },)) {
						failures.push(`${resource}.${action}: ${example.argv.join(" ",)}`,);
					}
				}
			}
		}
		expect(failures,).toEqual([],);
	});

	it("rejects bare value flags and advertises only the canonical = form", () => {
		const argvSchema = argvSchemaOf(registry.project?.export,);
		expect(argvSchema,).toBeDefined();
		expect(
			argvValidates(argvSchema, ["project", "export", "KEY", "--output=export.zip",],),
		).toBe(true,);
		expect(argvValidates(argvSchema, ["project", "export", "KEY", "--output",],),).toBe(
			false,
		);
		// The schema pins the machine-canonical = form; the runtime additionally
		// accepts the human space-separated form, which the examples keep.
		expect(
			argvValidates(argvSchema, ["project", "export", "KEY", "--output", "export.zip",],),
		).toBe(false,);
		expect(argvValidates(argvSchema, ["project", "export", "KEY", "-o",],),).toBe(false,);
	});

	it("allows empty = values where the runtime accepts them and keeps booleans standalone", () => {
		const setVersion = argvSchemaOf(registry.app?.["set-manifest-version"],);
		expect(setVersion,).toBeDefined();
		expect(
			argvValidates(setVersion, ["app", "set-manifest-version", "--version-notes=",],),
		).toBe(true,);
		const wikiUpdate = argvSchemaOf(registry.wiki?.update,);
		expect(wikiUpdate,).toBeDefined();
		expect(argvValidates(wikiUpdate, ["wiki", "update", "ARTICLE", "--content=",],),).toBe(true,);
		const insightUpdate = argvSchemaOf(registry.insight?.update,);
		expect(insightUpdate,).toBeDefined();
		expect(
			argvValidates(insightUpdate, ["insight", "update", "INSIGHT", "--content=",],),
		).toBe(true,);
		const successor = argvSchemaOf(registry.app?.["create-successor-instance"],);
		expect(successor,).toBeDefined();
		expect(
			argvValidates(
				successor,
				[
					"app",
					"create-successor-instance",
					"MYAPP",
					"--from=A",
					"--to=B",
					"--copy-permissions",
					"--dry-run",
				],
			),
		).toBe(true,);
	});

	it("rejects empty = values for required non-empty flags and identifiers", () => {
		const setVersion = argvSchemaOf(registry.app?.["set-manifest-version"],);
		expect(setVersion,).toBeDefined();
		// Required-choice members must reject the empty form even though the
		// group allows --version-notes= to clear the notes.
		expect(
			argvValidates(setVersion, ["app", "set-manifest-version", "--manifest-version=",],),
		).toBe(false,);
		expect(
			argvValidates(
				setVersion,
				["app", "set-manifest-version", "--manifest-version=", "--version-notes=",],
			),
		).toBe(false,);
		const successor = argvSchemaOf(registry.app?.["create-successor-instance"],);
		expect(successor,).toBeDefined();
		expect(
			argvValidates(successor, ["app", "create-successor-instance", "MYAPP", "--from=", "--to=B",],),
		).toBe(false,);
		expect(
			argvValidates(successor, ["app", "create-successor-instance", "MYAPP", "--from=A", "--to=",],),
		).toBe(false,);
		const exportSchema = argvSchemaOf(registry.project?.export,);
		expect(exportSchema,).toBeDefined();
		expect(argvValidates(exportSchema, ["project", "export", "KEY", "--output=",],),).toBe(false,);
		const datasetList = argvSchemaOf(registry.dataset?.list,);
		expect(datasetList,).toBeDefined();
		expect(argvValidates(datasetList, ["dataset", "list", "--project-key=",],),).toBe(false,);
	});

	it("plans app set-manifest-version locally without reading the saved credentials file", async () => {
		const tmpDir = join(tmpdir(), `dss-plan-credentials-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		writeFileSync(
			join(tmpDir, "credentials.json",),
			'{ "url": "https://saved.example", "apiKey": "SENTINEL_SAVED_KEY", ',
			"utf-8",
		);
		try {
			const { stdout, stderr, } = await dss(
				[
					"app",
					"set-manifest-version",
					"--manifest-version",
					"2.0.0",
					"--project-key",
					"MYAPP_TEMPLATE",
					"--plan",
				],
				{
					env: {
						PATH: process.env.PATH,
						HOME: tmpDir,
						DSS_CONFIG_DIR: tmpDir,
						DATAIKU_DISABLE_ENV: "1",
						DATAIKU_URL: "",
						DATAIKU_API_KEY: "",
						DATAIKU_PROJECT_KEY: "",
					},
				},
			);
			expect(stderr,).toBe("",);
			expect(stdout,).not.toContain("SENTINEL_SAVED_KEY",);
			expect(stdout,).not.toMatch(/api.?key/i,);
			const plan = JSON.parse(stdout,) as Record<string, unknown>;
			expect(plan["plan"],).toBe(true,);
			expect(plan["resource"],).toBe("app",);
			expect(plan["action"],).toBe("set-manifest-version",);
			expect(JSON.stringify(plan,),).toContain("MYAPP_TEMPLATE",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("synthetic single-action commands use actionless argv prefixes", () => {
		for (const resource of ["batch", "version", "install-skill", "cleanup", "fixtures",] as const) {
			const entry = registry[resource]?.run;
			expect(entry, resource,).toBeDefined();
			const prefixItems = prefixOf(argvSchemaOf(entry,),);
			expect(prefixItems[0], resource,).toEqual({ const: resource, },);
			expect(prefixItems[1], resource,).not.toEqual({ const: "run", },);
			for (const example of entry?.structuredExamples ?? []) {
				if (example.argv) {
					expect(example.argv[0], `${resource} example`,).toBe(resource,);
					expect(example.argv[1], `${resource} example`,).not.toBe("run",);
				}
			}
		}
	});

	it("real run commands keep the run action token", () => {
		for (const [resource, action,] of [["commands", "run",], ["code", "run",],] as const) {
			const entry = registry[resource]?.[action];
			expect(entry, `${resource}.${action}`,).toBeDefined();
			const prefixItems = prefixOf(argvSchemaOf(entry,),);
			expect(prefixItems[0],).toEqual({ const: resource, },);
			expect(prefixItems[1],).toEqual({ const: action, },);
			for (const example of entry?.structuredExamples ?? []) {
				if (example.argv) expect(example.argv.slice(0, 2,),).toEqual([resource, action,],);
			}
		}
	});

	it("successor --plan --copy-permissions discloses the ACL read/write/verify path", async () => {
		const { stdout, stderr, } = await dss(
			[
				"app",
				"create-successor-instance",
				"MYAPP",
				"--from=OLD",
				"--to=NEW",
				"--copy-permissions",
				"--plan",
			],
		);
		expect(stderr,).toBe("",);
		const plan = JSON.parse(stdout,) as Record<string, unknown>;
		expect(plan,).toMatchObject({
			plan: true,
			method: "POST",
			endpoint: "/public/api/apps/MYAPP/instances",
			copyPermissions: true,
			preflightExecuted: false,
			preflightWillRunDuringApply: true,
			payload: { targetProjectKey: "NEW", targetProjectName: "NEW", },
			wait: true,
		},);
		const permissionRequests = plan["permissionRequests"] as unknown[] | undefined;
		expect(permissionRequests,).toMatchObject([
			{ method: "GET", endpoint: "/public/api/projects/OLD/permissions", },
			{ method: "GET", endpoint: "/public/api/projects/NEW/permissions", },
			{ method: "GET", endpoint: "/public/api/projects/NEW/", },
			{ method: "GET", endpoint: "/public/api/projects/OLD/permissions", },
			{ method: "GET", endpoint: "/public/api/projects/NEW/", },
			{ method: "PUT", endpoint: "/public/api/projects/NEW/permissions", },
			{ method: "GET", endpoint: "/public/api/projects/NEW/permissions", },
			{ method: "GET", endpoint: "/public/api/projects/NEW/", },
		],);
		expect(permissionRequests,).toHaveLength(8,);
		expect(plan["postFutureRequests"],).toMatchObject([
			{ method: "GET", endpoint: "/public/api/projects/NEW/", },
		],);
		expect(JSON.stringify(plan,),).not.toContain("api-key",);

		const bare = JSON.parse(
			(await dss([
				"app",
				"create-successor-instance",
				"MYAPP",
				"--from",
				"OLD",
				"--to",
				"NEW",
				"--plan",
			],))
				.stdout,
		) as Record<string, unknown>;
		expect(bare["copyPermissions"],).toBe(false,);
		expect(bare["permissionRequests"],).toBeUndefined();

		// String forms must behave exactly like the runtime's parseBooleanOption:
		// only "true"-family values enable the copy and its ACL read/write/verify path.
		const stringTrue = JSON.parse(
			(await dss([
				"app",
				"create-successor-instance",
				"MYAPP",
				"--from",
				"OLD",
				"--to",
				"NEW",
				"--copy-permissions=true",
				"--plan",
			],))
				.stdout,
		) as Record<string, unknown>;
		expect(stringTrue["copyPermissions"],).toBe(true,);
		expect(stringTrue["permissionRequests"],).toHaveLength(8,);

		const stringFalse = JSON.parse(
			(await dss([
				"app",
				"create-successor-instance",
				"MYAPP",
				"--from",
				"OLD",
				"--to",
				"NEW",
				"--copy-permissions=false",
				"--plan",
			],))
				.stdout,
		) as Record<string, unknown>;
		expect(stringFalse["copyPermissions"],).toBe(false,);
		expect(stringFalse["permissionRequests"],).toBeUndefined();
	});

	it("strict agent-contract command schemas accept the generated registry", () => {
		const contractSchemas = buildAgentContract()["schemas"] as Record<string, Schema>;
		const registrySchema = contractSchemas["commandRegistry"] as Schema | undefined;
		expect(registrySchema,).toBeDefined();
		expect(validates(registrySchema, registry,),).toBe(true,);

		const entrySchema = (registrySchema["additionalProperties"] as Schema)[
			"additionalProperties"
		] as Schema;
		const flagSchema = (entrySchema["properties"] as Record<string, Schema>)["flags"] as Schema;
		expect(
			validates(flagSchema["items"] as Schema, {
				name: "version-notes",
				kind: "value",
				allowEmptyValue: true,
			},),
		).toBe(true,);
		// The strict flag schema rejects unknown properties, so a generated
		// allowEmptyValue would fail the whole registry unless advertised.
		expect(
			validates(flagSchema["items"] as Schema, {
				name: "x",
				kind: "value",
				unknownProperty: true,
			},),
		).toBe(false,);
	});

	it("runtime accepts both = and space forms for value flags", async () => {
		const eq = await dss(["commands", "run", "--fields=version.run",],);
		const spaced = await dss(["commands", "run", "--fields", "version.run",],);
		expect(eq.stderr,).toBe("",);
		expect(spaced.stderr,).toBe("",);
		expect(JSON.parse(eq.stdout,),).toEqual(JSON.parse(spaced.stdout,),);
	});

	it("successor usage, hash example, and shell examples stay accurate", () => {
		const successor = registry.app?.["create-successor-instance"];
		expect(successor?.usage,).toContain("--record-cleanup PATH",);
		expect(
			successor?.flags.some((flag,) => flag.name === "record-cleanup" && flag.kind === "value"),
		).toBe(true,);
		const setter = registry.app?.["set-manifest-version"];
		expect(setter,).toBeDefined();
		expect(setter?.structuredExamples[1]?.argv,).toContain(
			"--expect-hash=480039fce035bfcd98740cb4fd9e67763b8ba68cac43d16a6e25bd9abb7548e8",
		);
		// Human shell examples keep the space-separated form the runtime accepts.
		expect(setter?.examples?.[0],).toContain("--manifest-version 2.0.0",);
		const firstArgv = setter?.structuredExamples[0]?.argv;
		expect(firstArgv,).toContain("--manifest-version=2.0.0",);
		expect(firstArgv,).not.toContain("--manifest-version",);
	});

	it("verify-instance success output marks the visual gate unverified", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, {
					projectKey: "MYAPP_TEMPLATE",
					projectAppType: "APP_TEMPLATE",
					version: "2.0.0",
					versionNotes: null,
					homepageSections: [],
				},);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "RELEASE_INSTANCE", },],);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/RELEASE_INSTANCE/app-manifest"
			) {
				sendJson(res, {
					projectKey: "RELEASE_INSTANCE",
					projectAppType: "APP_INSTANCE",
					version: "2.0.0",
					versionNotes: null,
					homepageSections: [],
				},);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss(["app", "verify-instance", "MYAPP", "--project-key", "RELEASE_INSTANCE",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(result["apiReady"],).toBe(true,);
			expect(result["status"],).toBe("API_VERIFIED_UI_PENDING",);
			expect(result["uiPublicationVerified"],).toBe(false,);
			expect(result["published"],).toBeUndefined();
			expect(result["ready"],).toBeUndefined();
		},);
	});
});
