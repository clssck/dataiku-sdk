import { requiredJsonInput, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const apiDeployerCommands: Record<string, CommandMeta> = {
	"list-infras": {
		handler: (c,) => c.apiDeployer.listInfras(),
		usage: "dss api-deployer list-infras",
		description: "List API Deployer infrastructures.",
		examples: ["dss api-deployer list-infras",],
	},
	"create-infra": {
		handler: (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (infra settings).",
			);
			return c.apiDeployer.createInfra(body,);
		},
		usage: "dss api-deployer create-infra (--data JSON|--data-file PATH|--stdin)",
		description: "Create an API Deployer infrastructure.",
		examples: ["dss api-deployer create-infra --data-file infra.json",],
	},
	"get-infra": {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss api-deployer get-infra <infraId>",);
			return c.apiDeployer.getInfra(a[0],);
		},
		usage: "dss api-deployer get-infra <infraId>",
		description: "Get an API Deployer infrastructure status.",
		examples: ["dss api-deployer get-infra prod-infra",],
	},
	"delete-infra": {
		handler: async (c, a,) => {
			requireArgs(a, 1, "dss api-deployer delete-infra <infraId>",);
			await c.apiDeployer.deleteInfra(a[0],);
			return { deleted: true, };
		},
		usage: "dss api-deployer delete-infra <infraId>",
		description: "Delete an API Deployer infrastructure.",
		examples: ["dss api-deployer delete-infra prod-infra",],
	},
	"list-stages": {
		handler: (c,) => c.apiDeployer.listStages(),
		usage: "dss api-deployer list-stages",
		description: "List API Deployer lifecycle stages.",
		examples: ["dss api-deployer list-stages",],
	},
	"list-services": {
		handler: (c,) => c.apiDeployer.listServices(),
		usage: "dss api-deployer list-services",
		description: "List published API Deployer services.",
		examples: ["dss api-deployer list-services",],
	},
	"create-service": {
		handler: (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (service definition).",
			);
			return c.apiDeployer.createService(body,);
		},
		usage: "dss api-deployer create-service (--data JSON|--data-file PATH|--stdin)",
		description: "Create a published API Deployer service.",
		examples: ['dss api-deployer create-service --data \'{"id":"my-service"}\'',],
	},
	"get-service": {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss api-deployer get-service <serviceId>",);
			return c.apiDeployer.getService(a[0],);
		},
		usage: "dss api-deployer get-service <serviceId>",
		description: "Get a published service's status (versions + deployments).",
		examples: ["dss api-deployer get-service my-service",],
	},
	"delete-service": {
		handler: async (c, a,) => {
			requireArgs(a, 1, "dss api-deployer delete-service <serviceId>",);
			await c.apiDeployer.deleteService(a[0],);
			return { deleted: true, };
		},
		usage: "dss api-deployer delete-service <serviceId>",
		description: "Delete a published API Deployer service.",
		examples: ["dss api-deployer delete-service my-service",],
	},
	"publish-version": {
		handler: async (c, a,) => {
			requireArgs(a, 2, "dss api-deployer publish-version <serviceId> <archive.zip>",);
			await c.apiDeployer.publishServiceVersion(a[0], a[1],);
			return { published: true, };
		},
		usage: "dss api-deployer publish-version <serviceId> <archive.zip>",
		description: "Publish (upload) a service version package to the API Deployer.",
		examples: ["dss api-deployer publish-version my-service ./pkg.zip",],
	},
	"delete-version": {
		handler: async (c, a,) => {
			requireArgs(a, 2, "dss api-deployer delete-version <serviceId> <version>",);
			await c.apiDeployer.deleteServiceVersion(a[0], a[1],);
			return { deleted: true, };
		},
		usage: "dss api-deployer delete-version <serviceId> <version>",
		description: "Delete a published service version.",
		examples: ["dss api-deployer delete-version my-service v1",],
	},
	"list-deployments": {
		handler: (c,) => c.apiDeployer.listDeployments(),
		usage: "dss api-deployer list-deployments",
		description: "List API Deployer deployments.",
		examples: ["dss api-deployer list-deployments",],
	},
	"create-deployment": {
		handler: (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (deployment settings).",
			);
			return c.apiDeployer.createDeployment(body,);
		},
		usage: "dss api-deployer create-deployment (--data JSON|--data-file PATH|--stdin)",
		description: "Create an API Deployer deployment (maps a service version to an infra).",
		examples: ["dss api-deployer create-deployment --data-file deployment.json",],
	},
	"get-deployment": {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss api-deployer get-deployment <deploymentId>",);
			return c.apiDeployer.getDeployment(a[0],);
		},
		usage: "dss api-deployer get-deployment <deploymentId>",
		description: "Get an API Deployer deployment.",
		examples: ["dss api-deployer get-deployment my-deployment",],
	},
	"deployment-status": {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss api-deployer deployment-status <deploymentId>",);
			return c.apiDeployer.getDeploymentStatus(a[0],);
		},
		usage: "dss api-deployer deployment-status <deploymentId>",
		description: "Get an API Deployer deployment's full health/status.",
		examples: ["dss api-deployer deployment-status my-deployment",],
	},
	"deployment-settings": {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss api-deployer deployment-settings <deploymentId>",);
			return c.apiDeployer.getDeploymentSettings(a[0],);
		},
		usage: "dss api-deployer deployment-settings <deploymentId>",
		description: "Get an API Deployer deployment's settings.",
		examples: ["dss api-deployer deployment-settings my-deployment",],
	},
	"save-deployment-settings": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss api-deployer save-deployment-settings <deploymentId> (--data JSON|--data-file PATH|--stdin)",
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (deployment settings).",
			);
			await c.apiDeployer.saveDeploymentSettings(a[0], body,);
			return { saved: true, };
		},
		usage:
			"dss api-deployer save-deployment-settings <deploymentId> (--data JSON|--data-file PATH|--stdin)",
		description: "Save an API Deployer deployment's settings.",
		examples: ["dss api-deployer save-deployment-settings my-deployment --data-file settings.json",],
	},
	deploy: {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss api-deployer deploy <deploymentId>",);
			return c.apiDeployer.startDeploymentUpdate(a[0],);
		},
		usage: "dss api-deployer deploy <deploymentId>",
		description: "Apply a deployment's settings to its infrastructure (start update).",
		examples: ["dss api-deployer deploy my-deployment",],
	},
	"delete-deployment": {
		handler: async (c, a,) => {
			requireArgs(a, 1, "dss api-deployer delete-deployment <deploymentId>",);
			await c.apiDeployer.deleteDeployment(a[0],);
			return { deleted: true, };
		},
		usage: "dss api-deployer delete-deployment <deploymentId>",
		description: "Delete an API Deployer deployment.",
		examples: ["dss api-deployer delete-deployment my-deployment",],
	},
};
