import { ClientValidationError, } from "../../errors.js";
import { deepMerge, } from "../../utils/deep-merge.js";
import { SHA256_HEX_PATTERN, stableHash, } from "../../utils/stable-hash.js";
import { num, requiredJsonInput, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

const UPDATE_SETTINGS_USAGE = commandUsage("webapp", "update-settings",);
const RESTART_USAGE = commandUsage("webapp", "restart-backend",);

function validateExpectHash(value: string | boolean | undefined,): string | undefined {
	if (value === undefined || value === false) return undefined;
	if (typeof value !== "string" || !SHA256_HEX_PATTERN.test(value,)) {
		throw new ClientValidationError(
			"Expected webapp hash must be a 64-character SHA-256 hex digest.",
			"validation_failed",
			"Run this update with --dry-run and capture currentHash before retrying.",
		);
	}
	return value;
}

export const webappCommands: Record<string, CommandMeta> = withUsage("webapp", {
	list: {
		handler: (c, _a, f,) => c.webapps.list(f["project-key"] as string | undefined,),
		description: "List webapps in a project.",
		examples: ["dss webapp list",],
	},
	"get-settings": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("webapp", "get-settings",),);
			return c.webapps.getSettings(a[0], f["project-key"] as string | undefined,);
		},
		description: "Get a webapp's settings.",
		examples: ["dss webapp get-settings WEBAPP_ID",],
	},
	create: {
		handler: (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (webapp definition).",
			);
			return c.webapps.create(body, f["project-key"] as string | undefined,);
		},
		description: "Create a webapp from a JSON definition.",
		examples: ["dss webapp create --data-file webapp.json",],
	},
	"update-settings": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, UPDATE_SETTINGS_USAGE,);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (webapp settings).",
			);
			const projectKey = f["project-key"] as string | undefined;
			const expectHash = validateExpectHash(f["expect-hash"],);
			if (executionMode(f,).dryRun) {
				const current = await c.webapps.getSettings(a[0], projectKey,);
				const currentHash = stableHash(current,);
				if (expectHash !== undefined && currentHash !== expectHash.toLowerCase()) {
					throw new ClientValidationError(
						`The webapp ${JSON.stringify(a[0],)} changed since it was read.`,
						"validation_failed",
						"Re-read the webapp settings and retry with the current hash value.",
						{
							id: a[0],
							expectedHash: expectHash.toLowerCase(),
							actualHash: currentHash,
						},
					);
				}
				const next = deepMerge(current, body,);
				return {
					dryRun: true,
					action: "update-settings",
					resource: "webapp",
					id: a[0],
					current,
					next,
					currentHash,
					nextHash: stableHash(next,),
					...(expectHash !== undefined
						? { expectHash: expectHash.toLowerCase(), provenanceVerified: true, }
						: {}),
				};
			}
			const updated = await c.webapps.updateSettings(a[0], body, projectKey, { expectHash, },);
			return { updated: a[0], ...updated, };
		},
		description:
			"Merge supplied fields into a webapp's settings (GET-merge-PUT) and PUT the full object, so fields outside the patch are never dropped. --expect-hash refuses the write when the object changed since it was fetched; --dry-run reports current, next, and both hashes without writing.",
		examples: [
			"dss webapp update-settings WEBAPP_ID --data-file webapp.json",
			'dss webapp update-settings WEBAPP_ID --dry-run --data=\'{"params":{"designOptions":{"namingRule":"x"}}}\'',
		],
	},
	"stop-backend": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("webapp", "stop-backend",),);
			await c.webapps.stopBackend(a[0], f["project-key"] as string | undefined,);
			return { stopped: true, };
		},
		description: "Stop a webapp's backend.",
		examples: ["dss webapp stop-backend WEBAPP_ID",],
	},
	"restart-backend": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, RESTART_USAGE,);
			const projectKey = f["project-key"] as string | undefined;
			if (f["wait"] === true) {
				return c.webapps.restartBackendAndWait(a[0], projectKey, {
					pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
					timeoutMs: num(f["timeout"], "--timeout",),
				},);
			}
			const future = await c.webapps.startOrRestartBackend(a[0], projectKey,);
			return { restarted: true, future, };
		},
		description:
			"Start or restart a webapp's backend. The documented endpoint returns a restart future; --wait settles it and returns the FutureWaitResult.",
		examples: [
			"dss webapp restart-backend WEBAPP_ID",
			"dss webapp restart-backend WEBAPP_ID --wait --timeout=60000",
		],
	},
	"backend-state": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("webapp", "backend-state",),);
			return c.webapps.getBackendState(a[0], f["project-key"] as string | undefined,);
		},
		description: "Get a webapp backend's runtime state.",
		examples: ["dss webapp backend-state WEBAPP_ID",],
	},
},);
