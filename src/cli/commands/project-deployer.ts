import { requiredJsonInput, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const projectDeployerCommands: Record<string, CommandMeta> = {
	"list-projects": {
		handler: (c,) => c.projectDeployer.listProjects(),
		usage: "dss project-deployer list-projects",
		description: "List published projects on the Project Deployer.",
		examples: ["dss project-deployer list-projects",],
	},
	"create-project": {
		handler: (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (published project settings).",
			);
			return c.projectDeployer.createProject(body,);
		},
		usage: "dss project-deployer create-project (--data JSON|--data-file PATH|--stdin)",
		description: "Create a published project on the Project Deployer.",
		examples: ["dss project-deployer create-project --data-file project.json",],
	},
	"upload-bundle": {
		handler: async (c, a,) => {
			requireArgs(a, 1, "dss project-deployer upload-bundle <filePath>",);
			await c.projectDeployer.uploadBundle(a[0],);
			return { uploaded: true, };
		},
		usage: "dss project-deployer upload-bundle <filePath>",
		description: "Upload a project bundle archive to the Project Deployer.",
		examples: ["dss project-deployer upload-bundle ./v1.zip",],
	},
	"project-status": {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss project-deployer project-status <publishedProjectKey>",);
			return c.projectDeployer.getProjectStatus(a[0],);
		},
		usage: "dss project-deployer project-status <publishedProjectKey>",
		description: "Get a published project's status and available bundles.",
		examples: ["dss project-deployer project-status MYPROJ",],
	},
	"list-deployments": {
		handler: (c,) => c.projectDeployer.listDeployments(),
		usage: "dss project-deployer list-deployments",
		description: "List Project Deployer deployments.",
		examples: ["dss project-deployer list-deployments",],
	},
	"create-deployment": {
		handler: (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (deployment settings).",
			);
			return c.projectDeployer.createDeployment(body,);
		},
		usage: "dss project-deployer create-deployment (--data JSON|--data-file PATH|--stdin)",
		description: "Create a Project Deployer deployment (bundle to infra mapping).",
		examples: ["dss project-deployer create-deployment --data-file deployment.json",],
	},
	"get-deployment": {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss project-deployer get-deployment <deploymentId>",);
			return c.projectDeployer.getDeployment(a[0],);
		},
		usage: "dss project-deployer get-deployment <deploymentId>",
		description: "Get a Project Deployer deployment.",
		examples: ["dss project-deployer get-deployment my-deployment",],
	},
	"deployment-status": {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss project-deployer deployment-status <deploymentId>",);
			return c.projectDeployer.getDeploymentStatus(a[0],);
		},
		usage: "dss project-deployer deployment-status <deploymentId>",
		description: "Get a Project Deployer deployment's full health/status.",
		examples: ["dss project-deployer deployment-status my-deployment",],
	},
	"save-deployment-settings": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss project-deployer save-deployment-settings <deploymentId> (--data JSON|--data-file PATH|--stdin)",
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (deployment settings).",
			);
			await c.projectDeployer.saveDeploymentSettings(a[0], body,);
			return { saved: true, };
		},
		usage:
			"dss project-deployer save-deployment-settings <deploymentId> (--data JSON|--data-file PATH|--stdin)",
		description: "Save a Project Deployer deployment's settings (e.g. bundleId).",
		examples: [
			"dss project-deployer save-deployment-settings my-deployment --data-file settings.json",
		],
	},
	deploy: {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss project-deployer deploy <deploymentId>",);
			return c.projectDeployer.startUpdate(a[0],);
		},
		usage: "dss project-deployer deploy <deploymentId>",
		description: "Apply a deployment to the Automation node (start update).",
		examples: ["dss project-deployer deploy my-deployment",],
	},
	"delete-deployment": {
		handler: async (c, a,) => {
			requireArgs(a, 1, "dss project-deployer delete-deployment <deploymentId>",);
			await c.projectDeployer.deleteDeployment(a[0],);
			return { deleted: true, };
		},
		usage: "dss project-deployer delete-deployment <deploymentId>",
		description: "Delete a Project Deployer deployment.",
		examples: ["dss project-deployer delete-deployment my-deployment",],
	},
	"list-infras": {
		handler: (c,) => c.projectDeployer.listInfras(),
		usage: "dss project-deployer list-infras",
		description: "List Project Deployer infrastructures.",
		examples: ["dss project-deployer list-infras",],
	},
	"create-infra": {
		handler: (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (infra settings).",
			);
			return c.projectDeployer.createInfra(body,);
		},
		usage: "dss project-deployer create-infra (--data JSON|--data-file PATH|--stdin)",
		description: "Create a Project Deployer infrastructure.",
		examples: ["dss project-deployer create-infra --data-file infra.json",],
	},
};
