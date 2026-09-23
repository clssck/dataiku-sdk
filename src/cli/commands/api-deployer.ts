import { requiredJsonInput, } from "../coerce.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const apiDeployerCommands: Record<string, CommandMeta> = withUsage("api-deployer", {
	"list-infras": {
		handler: (c,) => c.apiDeployer.listInfras(),
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
		description: "Create an API Deployer infrastructure.",
		examples: ["dss api-deployer create-infra --data-file infra.json",],
	},
	"get-infra": {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("api-deployer", "get-infra",),);
			return c.apiDeployer.getInfra(a[0],);
		},
		description: "Get an API Deployer infrastructure status.",
		examples: ["dss api-deployer get-infra prod-infra",],
	},
	"delete-infra": {
		handler: async (c, a,) => {
			requireArgs(a, 1, commandUsage("api-deployer", "delete-infra",),);
			await c.apiDeployer.deleteInfra(a[0],);
			return { deleted: true, };
		},
		description: "Delete an API Deployer infrastructure.",
		examples: ["dss api-deployer delete-infra prod-infra",],
	},
	"list-stages": {
		handler: (c,) => c.apiDeployer.listStages(),
		description: "List API Deployer lifecycle stages.",
		examples: ["dss api-deployer list-stages",],
	},
	"list-services": {
		handler: (c,) => c.apiDeployer.listServices(),
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
		description: "Create a published API Deployer service.",
		examples: ['dss api-deployer create-service --data \'{"id":"my-service"}\'',],
	},
	"get-service": {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("api-deployer", "get-service",),);
			return c.apiDeployer.getService(a[0],);
		},
		description: "Get a published service's status (versions + deployments).",
		examples: ["dss api-deployer get-service my-service",],
	},
	"delete-service": {
		handler: async (c, a,) => {
			requireArgs(a, 1, commandUsage("api-deployer", "delete-service",),);
			await c.apiDeployer.deleteService(a[0],);
			return { deleted: true, };
		},
		description: "Delete a published API Deployer service.",
		examples: ["dss api-deployer delete-service my-service",],
	},
	"publish-version": {
		handler: async (c, a,) => {
			requireArgs(a, 2, commandUsage("api-deployer", "publish-version",),);
			await c.apiDeployer.publishServiceVersion(a[0], a[1],);
			return { published: true, };
		},
		description: "Publish (upload) a service version package to the API Deployer.",
		examples: ["dss api-deployer publish-version my-service ./pkg.zip",],
	},
	"delete-version": {
		handler: async (c, a,) => {
			requireArgs(a, 2, commandUsage("api-deployer", "delete-version",),);
			await c.apiDeployer.deleteServiceVersion(a[0], a[1],);
			return { deleted: true, };
		},
		description: "Delete a published service version.",
		examples: ["dss api-deployer delete-version my-service v1",],
	},
	"list-deployments": {
		handler: (c,) => c.apiDeployer.listDeployments(),
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
		description: "Create an API Deployer deployment (maps a service version to an infra).",
		examples: ["dss api-deployer create-deployment --data-file deployment.json",],
	},
	"get-deployment": {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("api-deployer", "get-deployment",),);
			return c.apiDeployer.getDeployment(a[0],);
		},
		description: "Get an API Deployer deployment.",
		examples: ["dss api-deployer get-deployment my-deployment",],
	},
	"deployment-status": {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("api-deployer", "deployment-status",),);
			return c.apiDeployer.getDeploymentStatus(a[0],);
		},
		description: "Get an API Deployer deployment's full health/status.",
		examples: ["dss api-deployer deployment-status my-deployment",],
	},
	"deployment-settings": {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("api-deployer", "deployment-settings",),);
			return c.apiDeployer.getDeploymentSettings(a[0],);
		},
		description: "Get an API Deployer deployment's settings.",
		examples: ["dss api-deployer deployment-settings my-deployment",],
	},
	"save-deployment-settings": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("api-deployer", "save-deployment-settings",),
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (deployment settings).",
			);
			await c.apiDeployer.saveDeploymentSettings(a[0], body,);
			return { saved: true, };
		},
		description: "Save an API Deployer deployment's settings.",
		examples: ["dss api-deployer save-deployment-settings my-deployment --data-file settings.json",],
	},
	deploy: {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("api-deployer", "deploy",),);
			return c.apiDeployer.startDeploymentUpdate(a[0],);
		},
		description: "Apply a deployment's settings to its infrastructure (start update).",
		examples: ["dss api-deployer deploy my-deployment",],
	},
	"delete-deployment": {
		handler: async (c, a,) => {
			requireArgs(a, 1, commandUsage("api-deployer", "delete-deployment",),);
			await c.apiDeployer.deleteDeployment(a[0],);
			return { deleted: true, };
		},
		description: "Delete an API Deployer deployment.",
		examples: ["dss api-deployer delete-deployment my-deployment",],
	},
},);
