import type { CommandMeta, } from "../types.js";
import { analysisCommands, } from "./analysis.js";
import { apiDeployerCommands, } from "./api-deployer.js";
import { apiServiceCommands, } from "./api-service.js";
import { appCommands, } from "./app.js";
import { bundleCommands, } from "./bundle.js";
import { businessAppCommands, } from "./business-app.js";
import { codeEnvCommands, } from "./code-env.js";
import { codeCommands, } from "./code.js";
import { connectionCommands, } from "./connection.js";
import { continuousActivityCommands, } from "./continuous-activity.js";
import { dashboardCommands, } from "./dashboard.js";
import { dataQualityCommands, } from "./data-quality.js";
import { datasetCommands, } from "./dataset.js";
import { discussionCommands, } from "./discussion.js";
import { doctorCommands, } from "./doctor.js";
import { flowZoneCommands, } from "./flow-zone.js";
import { folderCommands, } from "./folder.js";
import { futureCommands, } from "./future.js";
import { insightCommands, } from "./insight.js";
import { jobCommands, } from "./job.js";
import { meaningCommands, } from "./meaning.js";
import { metricsCommands, } from "./metrics.js";
import { mlTaskCommands, } from "./ml-task.js";
import { modelEvaluationStoreCommands, } from "./model-evaluation-store.js";
import { notebookCommands, } from "./notebook.js";
import { projectDeployerCommands, } from "./project-deployer.js";
import { projectGitCommands, } from "./project-git.js";
import { projectLibraryCommands, } from "./project-library.js";
import { projectCommands, } from "./project.js";
import { recipeCommands, } from "./recipe.js";
import { savedModelCommands, } from "./saved-model.js";
import { scenarioCommands, } from "./scenario.js";
import { sqlCommands, } from "./sql.js";
import { statisticsCommands, } from "./statistics.js";
import { streamingEndpointCommands, } from "./streaming-endpoint.js";
import { variableCommands, } from "./variable.js";
import { webappCommands, } from "./webapp.js";
import { wikiCommands, } from "./wiki.js";
import { workspaceCommands, } from "./workspace.js";

export const commands: Record<string, Record<string, CommandMeta>> = {
	project: projectCommands,
	analysis: analysisCommands,
	"ml-task": mlTaskCommands,
	"saved-model": savedModelCommands,
	"model-evaluation-store": modelEvaluationStoreCommands,
	app: appCommands,
	"business-app": businessAppCommands,
	webapp: webappCommands,
	"api-service": apiServiceCommands,
	"api-deployer": apiDeployerCommands,
	bundle: bundleCommands,
	"project-deployer": projectDeployerCommands,
	"project-git": projectGitCommands,
	"project-library": projectLibraryCommands,
	"streaming-endpoint": streamingEndpointCommands,
	"continuous-activity": continuousActivityCommands,
	statistics: statisticsCommands,
	discussion: discussionCommands,
	meaning: meaningCommands,
	workspace: workspaceCommands,
	metrics: metricsCommands,
	doctor: doctorCommands,
	wiki: wikiCommands,
	dashboard: dashboardCommands,
	insight: insightCommands,
	"data-quality": dataQualityCommands,
	future: futureCommands,
	"flow-zone": flowZoneCommands,
	dataset: datasetCommands,
	recipe: recipeCommands,
	job: jobCommands,
	scenario: scenarioCommands,
	folder: folderCommands,
	variable: variableCommands,
	connection: connectionCommands,
	"code-env": codeEnvCommands,
	sql: sqlCommands,
	code: codeCommands,
	notebook: notebookCommands,
};
