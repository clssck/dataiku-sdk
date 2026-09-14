import type { CommandMeta, } from "../types.js";

// Bun caches these synchronous module loads. Enumerating resource names stays
// cheap; looking up one resource loads only its command definitions.
export const commands: Record<string, Record<string, CommandMeta>> = {
	get "project"() {
		return (require("./project.js",) as typeof import("./project.js")).projectCommands;
	},
	get "analysis"() {
		return (require("./analysis.js",) as typeof import("./analysis.js")).analysisCommands;
	},
	get "ml-task"() {
		return (require("./ml-task.js",) as typeof import("./ml-task.js")).mlTaskCommands;
	},
	get "saved-model"() {
		return (require("./saved-model.js",) as typeof import("./saved-model.js")).savedModelCommands;
	},
	get "model-evaluation-store"() {
		return (require("./model-evaluation-store.js",) as typeof import("./model-evaluation-store.js"))
			.modelEvaluationStoreCommands;
	},
	get "app"() {
		return (require("./app.js",) as typeof import("./app.js")).appCommands;
	},
	get "business-app"() {
		return (require("./business-app.js",) as typeof import("./business-app.js")).businessAppCommands;
	},
	get "webapp"() {
		return (require("./webapp.js",) as typeof import("./webapp.js")).webappCommands;
	},
	get "api-service"() {
		return (require("./api-service.js",) as typeof import("./api-service.js")).apiServiceCommands;
	},
	get "api-deployer"() {
		return (require("./api-deployer.js",) as typeof import("./api-deployer.js")).apiDeployerCommands;
	},
	get "bundle"() {
		return (require("./bundle.js",) as typeof import("./bundle.js")).bundleCommands;
	},
	get "project-deployer"() {
		return (require("./project-deployer.js",) as typeof import("./project-deployer.js"))
			.projectDeployerCommands;
	},
	get "project-git"() {
		return (require("./project-git.js",) as typeof import("./project-git.js")).projectGitCommands;
	},
	get "project-folder"() {
		return (require("./project-folder.js",) as typeof import("./project-folder.js"))
			.projectFolderCommands;
	},
	get "project-library"() {
		return (require("./project-library.js",) as typeof import("./project-library.js"))
			.projectLibraryCommands;
	},
	get "streaming-endpoint"() {
		return (require("./streaming-endpoint.js",) as typeof import("./streaming-endpoint.js"))
			.streamingEndpointCommands;
	},
	get "continuous-activity"() {
		return (require("./continuous-activity.js",) as typeof import("./continuous-activity.js"))
			.continuousActivityCommands;
	},
	get "statistics"() {
		return (require("./statistics.js",) as typeof import("./statistics.js")).statisticsCommands;
	},
	get "discussion"() {
		return (require("./discussion.js",) as typeof import("./discussion.js")).discussionCommands;
	},
	get "meaning"() {
		return (require("./meaning.js",) as typeof import("./meaning.js")).meaningCommands;
	},
	get "workspace"() {
		return (require("./workspace.js",) as typeof import("./workspace.js")).workspaceCommands;
	},
	get "metrics"() {
		return (require("./metrics.js",) as typeof import("./metrics.js")).metricsCommands;
	},
	get "doctor"() {
		return (require("./doctor.js",) as typeof import("./doctor.js")).doctorCommands;
	},
	get "wiki"() {
		return (require("./wiki.js",) as typeof import("./wiki.js")).wikiCommands;
	},
	get "dashboard"() {
		return (require("./dashboard.js",) as typeof import("./dashboard.js")).dashboardCommands;
	},
	get "insight"() {
		return (require("./insight.js",) as typeof import("./insight.js")).insightCommands;
	},
	get "data-quality"() {
		return (require("./data-quality.js",) as typeof import("./data-quality.js")).dataQualityCommands;
	},
	get "future"() {
		return (require("./future.js",) as typeof import("./future.js")).futureCommands;
	},
	get "flow-zone"() {
		return (require("./flow-zone.js",) as typeof import("./flow-zone.js")).flowZoneCommands;
	},
	get "dataset"() {
		return (require("./dataset.js",) as typeof import("./dataset.js")).datasetCommands;
	},
	get "recipe"() {
		return (require("./recipe.js",) as typeof import("./recipe.js")).recipeCommands;
	},
	get "job"() {
		return (require("./job.js",) as typeof import("./job.js")).jobCommands;
	},
	get "scenario"() {
		return (require("./scenario.js",) as typeof import("./scenario.js")).scenarioCommands;
	},
	get "folder"() {
		return (require("./folder.js",) as typeof import("./folder.js")).folderCommands;
	},
	get "variable"() {
		return (require("./variable.js",) as typeof import("./variable.js")).variableCommands;
	},
	get "connection"() {
		return (require("./connection.js",) as typeof import("./connection.js")).connectionCommands;
	},
	get "code-env"() {
		return (require("./code-env.js",) as typeof import("./code-env.js")).codeEnvCommands;
	},
	get "sql"() {
		return (require("./sql.js",) as typeof import("./sql.js")).sqlCommands;
	},
	get "code"() {
		return (require("./code.js",) as typeof import("./code.js")).codeCommands;
	},
	get "notebook"() {
		return (require("./notebook.js",) as typeof import("./notebook.js")).notebookCommands;
	},
	get "user"() {
		return (require("./user.js",) as typeof import("./user.js")).userCommands;
	},
	get "group"() {
		return (require("./group.js",) as typeof import("./group.js")).groupCommands;
	},
	get "plugin"() {
		return (require("./plugin.js",) as typeof import("./plugin.js")).pluginCommands;
	},
	get "llm"() {
		return (require("./llm.js",) as typeof import("./llm.js")).llmCommands;
	},
	get "knowledge-bank"() {
		return (require("./knowledge-bank.js",) as typeof import("./knowledge-bank.js"))
			.knowledgeBankCommands;
	},
	get "macro"() {
		return (require("./macro.js",) as typeof import("./macro.js")).macroCommands;
	},
	get "data-collection"() {
		return (require("./data-collection.js",) as typeof import("./data-collection.js"))
			.dataCollectionCommands;
	},
};
