import type { DataikuClient, } from "../../client.js";
import { ClientValidationError, DataikuError, } from "../../errors.js";
import type { DatasetSchema, } from "../../schemas.js";
import { enqueueCliWarning, } from "../output.js";

/** POSIX-shell quoting for the display form of a resume command. */
function shellArg(value: string,): string {
	return /^[\w.:@%+=,/-]+$/.test(value,) ? value : `'${value.replaceAll("'", "'\\''",)}'`;
}

/**
 * A wait that timed out only stopped polling: the job is still running in DSS.
 * Attach the command that resumes waiting: `resumeArgv` (run it without a
 * shell, on any OS) and `resume` (POSIX display form); other results pass through.
 */
export function withWaitResume<T extends { jobId: string; timedOut?: boolean; },>(
	result: T,
	projectKey: string | undefined,
): T | (T & { resume: string; resumeArgv: string[]; }) {
	if (result.timedOut !== true) return result;
	const resumeArgv = [
		"dss",
		"job",
		"wait",
		result.jobId,
		...(projectKey ? ["--project-key", projectKey,] : []),
	];
	return { ...result, resume: resumeArgv.map(shellArg,).join(" ",), resumeArgv, };
}

/**
 * With an empty output schema DSS ends a dataset build DONE after writing no
 * columns and no rows, without an error. Flag that after a successful build so
 * it is not mistaken for real output.
 */
export async function warnIfBuiltDatasetHasNoColumns(
	c: DataikuClient,
	target: string,
	targetType: string,
	success: boolean,
	projectKey: string | undefined,
): Promise<void> {
	if (!success || targetType !== "DATASET") return;
	let schema: DatasetSchema;
	try {
		schema = await c.datasets.schema(target, projectKey,);
	} catch (error) {
		if (!(error instanceof DataikuError || error instanceof ClientValidationError)) throw error;
		enqueueCliWarning({
			code: "built_dataset_schema_unchecked",
			dataset: target,
			error: error.message.split("\n",)[0],
			hint: "The build succeeded but the dataset schema could not be read to check it is not empty.",
		},);
		return;
	}
	if (schema.columns.length > 0) return;
	enqueueCliWarning({
		code: "built_dataset_has_no_columns",
		dataset: target,
		hint:
			"The build succeeded but the dataset schema is empty, so no data was written. For a visual or SQL query recipe, run dss recipe update-schema RECIPE (or rebuild with --auto-update-schema).",
	},);
}
