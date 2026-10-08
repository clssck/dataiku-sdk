/**
 * Plain data shared by the compact read shapers (src/cli/result-shapes.ts) and
 * the discovery output schemas (src/cli/output-schemas.ts), so the advertised
 * schema is the emitted shape. Imports nothing: the schema generator loads it
 * without the CLI runtime.
 */

/**
 * Settings-object noise `dataset get` / `project get` drop: metrics and check
 * configuration, version stamps and checklists that agents never edit.
 */
export const DATASET_GET_DROPPED_KEYS = [
	"metrics",
	"metricsChecks",
	"versionTag",
	"creationTag",
	"checklists",
] as const;

export const PROJECT_GET_DROPPED_KEYS = [
	...DATASET_GET_DROPPED_KEYS,
	"objectImgHash",
	"isProjectImg",
	"defaultImgColor",
	"imgPattern",
] as const;

const STRING = { type: "string", };
const NUMBER = { type: "number", };
const BOOLEAN = { type: "boolean", };

/** Discovery output schemas of the `job get` and `dataset info` compact defaults. */
export function compactReadOutputSchemas(): Record<string, Record<string, unknown>> {
	const errorSchema = {
		type: "object",
		additionalProperties: false,
		properties: { type: STRING, message: STRING, },
	};
	return {
		"job.get": {
			type: "object",
			additionalProperties: false,
			required: ["id", "state",],
			properties: {
				id: STRING,
				name: STRING,
				type: STRING,
				state: STRING,
				initiator: STRING,
				triggeredFrom: STRING,
				startTime: NUMBER,
				endTime: NUMBER,
				outputs: {
					type: "array",
					items: {
						type: "object",
						additionalProperties: false,
						properties: { type: STRING, id: STRING, projectKey: STRING, partition: STRING, },
					},
				},
				progress: { type: "object", additionalProperties: NUMBER, },
				error: errorSchema,
				activities: {
					type: "array",
					items: {
						type: "object",
						additionalProperties: false,
						properties: {
							id: STRING,
							recipe: STRING,
							recipeType: STRING,
							state: STRING,
							totalTime: NUMBER,
							warnings: NUMBER,
							error: errorSchema,
							message: STRING,
						},
					},
				},
			},
		},
		"dataset.info": {
			type: "object",
			additionalProperties: false,
			required: ["name",],
			properties: {
				name: STRING,
				type: STRING,
				managed: BOOLEAN,
				connection: STRING,
				formatType: STRING,
				partitioned: BOOLEAN,
				schema: {
					type: "array",
					items: {
						type: "object",
						additionalProperties: false,
						required: ["name", "type",],
						properties: { name: STRING, type: STRING, meaning: STRING, comment: STRING, },
					},
				},
				recipes: { type: "array", items: STRING, },
				buildable: BOOLEAN,
				upstreamBuildable: BOOLEAN,
				downstreamBuildable: BOOLEAN,
				tags: { type: "array", items: STRING, },
				dataQuality: { type: "object", additionalProperties: true, },
				lastBuild: {
					type: "object",
					additionalProperties: false,
					properties: { jobId: STRING, success: BOOLEAN, startTime: NUMBER, endTime: NUMBER, },
				},
				lastModifiedOn: NUMBER,
				lastModifiedBy: STRING,
			},
		},
	};
}
