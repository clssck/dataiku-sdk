import { requiredJsonInput, } from "../coerce.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const projectDeployerCommands: Record<string, CommandMeta> = withUsage("project-deployer", {
	"list-projects": {
		handler: (c,) => c.projectDeployer.listProjects(),
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
		description: "Create a published project on the Project Deployer.",
		examples: ["dss project-deployer create-project --data-file project.json",],
	},
	"upload-bundle": {
		handler: async (c, a,) => {
			requireArgs(a, 1, commandUsage("project-deployer", "upload-bundle",),);
			await c.projectDeployer.uploadBundle(a[0],);
			return { uploaded: true, };
		},
		description: "Upload a project bundle archive to the Project Deployer.",
		examples: ["dss project-deployer upload-bundle ./v1.zip",],
	},
	"project-status": {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("project-deployer", "project-status",),);
			return c.projectDeployer.getProjectStatus(a[0],);
		},
		description: "Get a published project's status and available bundles.",
		examples: ["dss project-deployer project-status MYPROJ",],
	},
	"list-deployments": {
		handler: (c,) => c.projectDeployer.listDeployments(),
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
		description: "Create a Project Deployer deployment (bundle to infra mapping).",
		examples: ["dss project-deployer create-deployment --data-file deployment.json",],
	},
	"get-deployment": {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("project-deployer", "get-deployment",),);
			return c.projectDeployer.getDeployment(a[0],);
		},
		description: "Get a Project Deployer deployment.",
		examples: ["dss project-deployer get-deployment my-deployment",],
	},
	"deployment-status": {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("project-deployer", "deployment-status",),);
			return c.projectDeployer.getDeploymentStatus(a[0],);
		},
		description: "Get a Project Deployer deployment's full health/status.",
		examples: ["dss project-deployer deployment-status my-deployment",],
	},
	"save-deployment-settings": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("project-deployer", "save-deployment-settings",),
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (deployment settings).",
			);
			await c.projectDeployer.saveDeploymentSettings(a[0], body,);
			return { saved: true, };
		},
		description: "Save a Project Deployer deployment's settings (e.g. bundleId).",
		examples: [
			"dss project-deployer save-deployment-settings my-deployment --data-file settings.json",
		],
	},
	deploy: {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("project-deployer", "deploy",),);
			return c.projectDeployer.startUpdate(a[0],);
		},
		description: "Apply a deployment to the Automation node (start update).",
		examples: ["dss project-deployer deploy my-deployment",],
	},
	"delete-deployment": {
		handler: async (c, a,) => {
			requireArgs(a, 1, commandUsage("project-deployer", "delete-deployment",),);
			await c.projectDeployer.deleteDeployment(a[0],);
			return { deleted: true, };
		},
		description: "Delete a Project Deployer deployment.",
		examples: ["dss project-deployer delete-deployment my-deployment",],
	},
	"list-infras": {
		handler: (c,) => c.projectDeployer.listInfras(),
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
		description: "Create a Project Deployer infrastructure.",
		examples: ["dss project-deployer create-infra --data-file infra.json",],
	},
	"list-stages": {
		handler: (c,) => c.projectDeployer.listStages(),
		description: "List the Project Deployer stages (e.g. Development, Test, Production).",
		examples: ["dss project-deployer list-stages",],
	},
	"get-infra": {
		handler: (c, a,) => c.projectDeployer.getInfra(a[0]!,),
		description: "Get a Project Deployer infrastructure (stage, automation node, deployments).",
		examples: ["dss project-deployer get-infra prod-infra",],
	},
	"infra-settings": {
		handler: (c, a,) => c.projectDeployer.getInfraSettings(a[0]!,),
		description:
			"Get an infrastructure's settings (permissions, automation node URL, connection remappings).",
		examples: ["dss project-deployer infra-settings prod-infra",],
	},
	"save-infra-settings": {
		handler: async (c, a, f,) => {
			await c.projectDeployer.saveInfraSettings(
				a[0]!,
				requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (settings from infra-settings).",
				),
			);
			return { updated: a[0], resource: "project-deployer", };
		},
		description: "Save infrastructure settings obtained from infra-settings, edited.",
		examples: ["dss project-deployer save-infra-settings prod-infra --data-file infra.json",],
	},
	"delete-infra": {
		handler: async (c, a,) => {
			await c.projectDeployer.deleteInfra(a[0]!,);
			return { deleted: a[0], resource: "project-deployer", };
		},
		description: "Delete an infrastructure that no deployment uses.",
		examples: ["dss project-deployer delete-infra old-infra --plan",],
	},
	"deployment-settings": {
		handler: (c, a,) => c.projectDeployer.getDeploymentSettings(a[0]!,),
		description:
			"Get a deployment's settings (infra, bundle, remappings); edit and send to save-deployment-settings.",
		examples: ["dss project-deployer deployment-settings v1-on-prod",],
	},
	"governance-status": {
		handler: (c, a, f,) =>
			c.projectDeployer.getGovernanceStatus(a[0]!, f["bundle-id"] as string | undefined,),
		description:
			"Governance status of a deployment's bundle (or --bundle-id): messages on whether it may be deployed.",
		examples: ["dss project-deployer governance-status v1-on-prod",],
	},
	"delete-project": {
		handler: async (c, a,) => {
			await c.projectDeployer.deleteProject(a[0]!,);
			return { deleted: a[0], resource: "project-deployer", };
		},
		description: "Delete a published project that no deployment uses.",
		examples: ["dss project-deployer delete-project MYPROJECT --plan",],
	},
	"project-settings": {
		handler: (c, a,) => c.projectDeployer.getProjectSettings(a[0]!,),
		description: "Get a published project's settings (owner, permissions).",
		examples: ["dss project-deployer project-settings MYPROJECT",],
	},
	"save-project-settings": {
		handler: async (c, a, f,) => {
			await c.projectDeployer.saveProjectSettings(
				a[0]!,
				requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (settings from project-settings).",
				),
			);
			return { updated: a[0], resource: "project-deployer", };
		},
		description: "Save published project settings obtained from project-settings, edited.",
		examples: ["dss project-deployer save-project-settings MYPROJECT --data-file published.json",],
	},
	"delete-bundle": {
		handler: async (c, a,) => {
			await c.projectDeployer.deleteBundle(a[0]!, a[1]!,);
			return { deleted: a[1], resource: "project-deployer", project: a[0], };
		},
		description: "Delete a bundle of a published project that no deployment uses.",
		examples: ["dss project-deployer delete-bundle MYPROJECT v1 --plan",],
	},
},);
