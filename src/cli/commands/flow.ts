import type { SchemaPropagationOptions, } from "../../resources/flow.js";
import { writeResponseToFile, } from "../../utils/response-file.js";
import { splitCsvFlag, } from "../coerce.js";
import {
	documentationTemplateFromFlags,
	runDocumentationCommand,
	taskWaitOptions,
} from "../helpers/documentation.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

/** Propagation options from flags; shared with the `--plan` builder. */
export function schemaPropagationOptionsFromFlags(
	flags: Record<string, string | boolean>,
): SchemaPropagationOptions {
	const excludedRecipes = splitCsvFlag(flags["stop-at"],);
	const markAsOkRecipes = splitCsvFlag(flags["mark-ok"],);
	return {
		...(flags["no-auto-rebuild"] === true ? { autoRebuild: false, } : {}),
		...(excludedRecipes.length > 0 ? { excludedRecipes, } : {}),
		...(markAsOkRecipes.length > 0 ? { markAsOkRecipes, } : {}),
	};
}

export const flowCommands: Record<string, CommandMeta> = withUsage("flow", {
	"propagate-schema": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("flow", "propagate-schema",),);
			const opts = {
				...schemaPropagationOptionsFromFlags(f,),
				projectKey: f["project-key"] as string | undefined,
			};
			if (f["wait"] === true) {
				return c.flow.propagateSchemaAndWait(a[0]!, { ...opts, ...taskWaitOptions(f,), },);
			}
			return c.flow.propagateSchema(a[0]!, opts,);
		},
		description:
			"Propagate a dataset's schema change through all downstream recipes and datasets (UI schema propagation). Rebuilds as needed unless --no-auto-rebuild; --stop-at / --mark-ok RECIPES limit it. Returns the DSS task; --wait waits. One recipe only: dss recipe update-schema.",
		examples: [
			"dss flow propagate-schema orders_raw --wait",
			"dss flow propagate-schema orders_raw --stop-at compute_report --no-auto-rebuild --plan",
		],
	},
	"generate-documentation": {
		handler: async (c, _a, f,) => {
			const pk = f["project-key"] as string | undefined;
			const task = await c.flow.generateDocumentation(documentationTemplateFromFlags(f,), pk,);
			return runDocumentationCommand(
				c,
				f,
				task,
				(exportId,) => c.flow.downloadDocumentation(exportId, pk,),
			);
		},
		description:
			"Generate the flow documentation (Word docx) from the DSS default template, an uploaded --template-file, or a template in a managed folder (--folder FOLDER_ID --path PATH). Returns the DSS future; --wait waits; --output PATH waits and downloads the document.",
		examples: [
			"dss flow generate-documentation --output flow.docx",
			"dss flow generate-documentation --template-file template.docx --wait",
		],
	},
	"download-documentation": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("flow", "download-documentation",),);
			const output = f["output"] as string;
			const res = await c.flow.downloadDocumentation(a[0]!, f["project-key"] as string | undefined,);
			return { path: output, bytes: await writeResponseToFile(output, res,), };
		},
		description:
			"Download a generated flow documentation by the exportId from a finished generate-documentation future.",
		examples: ["dss flow download-documentation EXPORT_ID --output flow.docx",],
	},
},);
