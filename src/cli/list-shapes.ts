/**
 * Compact `RESOURCE.list` item shapes, as plain data shared by the runtime
 * mapper (src/cli/list-output.ts) and the discovery output schemas
 * (src/cli/output-schemas.ts), so the advertised schema is the emitted shape.
 */

/** Where a compact field comes from in the DSS object, and its JSON Schema. */
export interface CompactListField {
	/** Dotted path into the DSS list item. */
	path: string;
	/** Flatten recipe roles (`{main:{items:[{ref}]}}`) to the dataset refs. */
	roleRefs?: true;
	schema: Record<string, unknown>;
}

const STRING = { type: ["string", "null",], };
const BOOLEAN = { type: ["boolean", "null",], };
const NUMBER = { type: ["number", "null",], };
const STRINGS = { type: "array", items: { type: "string", }, };

const field = (path: string, schema: Record<string, unknown>,): CompactListField => ({
	path,
	schema,
});

/**
 * Lists whose DSS objects are large: the identifier the follow-up get/build
 * commands take, the kind, and the one or two attributes that decide what to
 * call next. Lists not named here keep the DSS objects.
 */
export const COMPACT_LIST_FIELDS: Record<string, Record<string, CompactListField>> = {
	project: {
		projectKey: field("projectKey", STRING,),
		name: field("name", STRING,),
		ownerLogin: field("ownerLogin", STRING,),
	},
	dataset: {
		name: field("name", STRING,),
		type: field("type", STRING,),
		managed: field("managed", BOOLEAN,),
		connection: field("params.connection", STRING,),
	},
	recipe: {
		name: field("name", STRING,),
		type: field("type", STRING,),
		inputs: { path: "inputs", roleRefs: true, schema: STRINGS, },
		outputs: { path: "outputs", roleRefs: true, schema: STRINGS, },
	},
	scenario: {
		id: field("id", STRING,),
		name: field("name", STRING,),
		type: field("type", STRING,),
		active: field("active", BOOLEAN,),
		running: field("running", BOOLEAN,),
	},
	folder: {
		id: field("id", STRING,),
		name: field("name", STRING,),
		type: field("type", STRING,),
		connection: field("params.connection", STRING,),
	},
	job: {
		id: field("def.id", STRING,),
		name: field("def.name", STRING,),
		type: field("def.type", STRING,),
		state: field("state", STRING,),
		startTime: field("startTime", NUMBER,),
		endTime: field("endTime", NUMBER,),
	},
	"code-env": {
		envName: field("envName", STRING,),
		envLang: field("envLang", STRING,),
		deploymentMode: field("deploymentMode", STRING,),
	},
	future: {
		jobId: field("jobId", STRING,),
		owner: field("owner", STRING,),
		alive: field("alive", BOOLEAN,),
		hasResult: field("hasResult", BOOLEAN,),
		runningTime: field("runningTime", NUMBER,),
	},
	user: {
		login: field("login", STRING,),
		displayName: field("displayName", STRING,),
		userProfile: field("userProfile", STRING,),
		groups: field("groups", STRINGS,),
		enabled: field("enabled", BOOLEAN,),
	},
	group: {
		name: field("name", STRING,),
		description: field("description", STRING,),
		admin: field("admin", BOOLEAN,),
	},
	plugin: {
		id: field("id", STRING,),
		version: field("version", STRING,),
		label: field("meta.label", STRING,),
	},
	macro: {
		runnableType: field("runnableType", STRING,),
		ownerPluginId: field("ownerPluginId", STRING,),
		label: field("meta.label", STRING,),
	},
	meaning: {
		id: field("id", STRING,),
		label: field("label", STRING,),
		type: field("type", STRING,),
	},
};

/** Discovery output schemas of the compact `RESOURCE.list` defaults. */
export function compactListOutputSchemas(): Record<string, Record<string, unknown>> {
	return Object.fromEntries(
		Object.entries(COMPACT_LIST_FIELDS,).map(([resource, fields,],) => [
			`${resource}.list`,
			{
				type: "array",
				items: {
					type: "object",
					additionalProperties: false,
					properties: Object.fromEntries(
						Object.entries(fields,).map(([name, spec,],) => [name, spec.schema,]),
					),
				},
			},
		]),
	);
}
