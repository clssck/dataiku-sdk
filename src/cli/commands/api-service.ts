import { ClientValidationError, } from "../../errors.js";
import { stableHash, } from "../../utils/stable-hash.js";
import { requiredJsonInput, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

function validateExpectHash(value: string | boolean | undefined,): string | undefined {
	if (value === undefined || value === false) return undefined;
	if (typeof value !== "string" || !/^[0-9a-fA-F]{64}$/.test(value,)) {
		throw new ClientValidationError(
			"Expected API service settings hash must be a 64-character SHA-256 hex digest.",
			"validation_failed",
			"Run this update with --dry-run and capture currentHash before retrying.",
		);
	}
	return value;
}

export const apiServiceCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.apiServices.list(f["project-key"] as string | undefined,),
		usage: "dss api-service list [--project-key KEY]",
		description: "List API services in a project.",
		examples: ["dss api-service list",],
	},
	create: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss api-service create <serviceId> [--project-key KEY]",);
			return c.apiServices.create(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss api-service create <serviceId> [--project-key KEY]",
		description: "Create an empty API service.",
		examples: ["dss api-service create my-service",],
	},
	"get-settings": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss api-service get-settings <serviceId> [--project-key KEY]",);
			return c.apiServices.getSettings(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss api-service get-settings <serviceId> [--project-key KEY]",
		description: "Get an API service's settings (endpoint definitions).",
		examples: ["dss api-service get-settings my-service",],
	},
	"save-settings": {
		handler: async (c, a, f,) => {
			const usage =
				"dss api-service save-settings <serviceId> (--data JSON|--data-file PATH|--stdin) [--expect-hash SHA256] [--dry-run] [--project-key KEY]";
			requireArgs(a, 1, usage,);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (service settings).",
			);
			const projectKey = f["project-key"] as string | undefined;
			const expectHash = validateExpectHash(f["expect-hash"],);
			if (f["dry-run"] === true) {
				const current = await c.apiServices.getSettings(a[0], projectKey,);
				const currentHash = stableHash(current,);
				if (expectHash !== undefined && currentHash !== expectHash.toLowerCase()) {
					throw new ClientValidationError(
						`The API service ${JSON.stringify(a[0],)} changed since it was read.`,
						"validation_failed",
						"Re-read the service settings and retry with the current hash value.",
						{
							serviceId: a[0],
							expectedHash: expectHash.toLowerCase(),
							actualHash: currentHash,
						},
					);
				}
				return {
					dryRun: true,
					action: "save-settings",
					resource: "api-service",
					serviceId: a[0],
					current,
					next: body,
					currentHash,
					nextHash: stableHash(body,),
					...(expectHash !== undefined
						? { expectHash: expectHash.toLowerCase(), provenanceVerified: true, }
						: {}),
				};
			}
			const saved = await c.apiServices.saveSettings(a[0], body, projectKey, { expectHash, },);
			return { saved: a[0], ...saved, };
		},
		usage:
			"dss api-service save-settings <serviceId> (--data JSON|--data-file PATH|--stdin) [--expect-hash SHA256] [--dry-run] [--project-key KEY]",
		description:
			"Replace an API service's settings wholesale (explicit replacement: the supplied object is PUT as-is, so fetch get-settings first). --expect-hash refuses the write when the stored settings changed since they were fetched; --dry-run reports current, next, and both hashes without writing.",
		examples: ["dss api-service save-settings my-service --data-file service.json --dry-run",],
	},
	"add-prediction-endpoint": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				3,
				"dss api-service add-prediction-endpoint <serviceId> <endpointId> <savedModelId> [--dry-run] [--project-key KEY]",
			);
			const endpoint = {
				id: a[1],
				type: "STD_PREDICTION",
				modelRef: a[2],
			};
			if (f["dry-run"] === true) return endpoint;
			return c.apiServices.addPredictionEndpoint(
				a[0],
				a[1],
				a[2],
				f["project-key"] as string | undefined,
			);
		},
		usage:
			"dss api-service add-prediction-endpoint <serviceId> <endpointId> <savedModelId> [--dry-run] [--project-key KEY]",
		description: "Add a saved-model prediction endpoint to an API service.",
		examples: [
			"dss api-service add-prediction-endpoint my-service predict churn-model",
			"dss api-service add-prediction-endpoint my-service predict churn-model --dry-run",
		],
	},

	"list-packages": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss api-service list-packages <serviceId> [--project-key KEY]",);
			const projectKey = f["project-key"] as string | undefined;
			return c.apiServices.listPackages(a[0], projectKey,);
		},
		usage: "dss api-service list-packages <serviceId> [--project-key KEY]",
		description: "List deployable packages of an API service.",
		examples: ["dss api-service list-packages my-service",],
	},
	"package-summary": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss api-service package-summary <serviceId> <packageId> [--project-key KEY]",
			);
			return c.apiServices.getPackageSummary(a[0], a[1], f["project-key"] as string | undefined,);
		},
		usage: "dss api-service package-summary <serviceId> <packageId> [--project-key KEY]",
		description: "Get a package summary.",
		examples: ["dss api-service package-summary my-service v1",],
	},
	"create-package": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss api-service create-package <serviceId> <packageId> [--release-notes TEXT] [--project-key KEY]",
			);
			return c.apiServices.createPackage(
				a[0],
				a[1],
				f["project-key"] as string | undefined,
				typeof f["release-notes"] === "string" ? { releaseNotes: f["release-notes"], } : {},
			);
		},
		usage:
			"dss api-service create-package <serviceId> <packageId> [--release-notes TEXT] [--project-key KEY]",
		description:
			"Build a deployable package from the current service state; --release-notes forwards the documented releaseNotes query parameter.",
		examples: [
			"dss api-service create-package my-service v1",
			"dss api-service create-package my-service v1 --release-notes='Adds churn endpoint'",
		],
	},
	"delete-package": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss api-service delete-package <serviceId> <packageId> [--project-key KEY]",
			);
			await c.apiServices.deletePackage(a[0], a[1], f["project-key"] as string | undefined,);
			return { deleted: true, };
		},
		usage: "dss api-service delete-package <serviceId> <packageId> [--project-key KEY]",
		description: "Delete an API service package.",
		examples: ["dss api-service delete-package my-service v1",],
	},
	"download-package": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss api-service download-package <serviceId> <packageId> --output PATH [--project-key KEY]",
			);
			const out = f["output"] as string | undefined;
			if (!out) throw new UsageError("--output PATH is required.", "missing_required_flag",);
			const res = await c.apiServices.downloadPackageArchive(
				a[0],
				a[1],
				f["project-key"] as string | undefined,
			);
			if (!res.body) {
				throw new Error("apiServices.downloadPackageArchive response did not include a body",);
			}
			const bytes = await Bun.write(out, new Response(res.body,), { createPath: false, },);
			return { path: out, bytes, };
		},
		usage:
			"dss api-service download-package <serviceId> <packageId> --output PATH [--project-key KEY]",
		description: "Download an API service package archive to a local file.",
		examples: ["dss api-service download-package my-service v1 --output ./pkg.zip",],
	},
	"publish-package": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss api-service publish-package <serviceId> <packageId> [--published-service-id ID] [--project-key KEY]",
			);
			return c.apiServices.publishPackage(
				a[0],
				a[1],
				f["project-key"] as string | undefined,
				typeof f["published-service-id"] === "string"
					? { publishedServiceId: f["published-service-id"], }
					: {},
			);
		},
		usage:
			"dss api-service publish-package <serviceId> <packageId> [--published-service-id ID] [--project-key KEY]",
		description:
			"Publish a package to the API Deployer; --published-service-id forwards the documented publishedServiceId query parameter (a new published service is created when no match exists; the service's own id is the server-side default).",
		examples: [
			"dss api-service publish-package my-service v1",
			"dss api-service publish-package my-service v1 --published-service-id=prod-churn",
		],
	},
};
