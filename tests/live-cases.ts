/** Selectable cases and internal setup checks; profiles always include core. */
export const LIVE_CASES = {
	"core.dataset.baseline": { profile: "core", phase: "run", },
	"core.dataset.lifecycle": { profile: "core", phase: "run", },
	"core.folder.roundtrip": { profile: "core", phase: "run", },
	"core.recipe.graph": { profile: "core", phase: "run", },
	"core.recipe.runs": { profile: "core", phase: "run", },
	"core.recipe.lifecycle": { profile: "core", phase: "run", },
	"collab.setup.variables": { profile: "core", phase: "setup", },
	"collab.setup.project-library": { profile: "core", phase: "setup", },
	"collab.setup.flow-zone": { profile: "core", phase: "setup", },
	"collab.setup.scenario": { profile: "core", phase: "setup", },
	"collab.setup.insight": { profile: "core", phase: "setup", },
	"collab.setup.dashboard": { profile: "core", phase: "setup", },
	"collab.setup.notebook": { profile: "core", phase: "setup", },
	"collab.setup.wiki": { profile: "core", phase: "setup", },
	"collab.setup.data-quality": { profile: "core", phase: "setup", },
	"collab.setup.metrics": { profile: "core", phase: "setup", },
	"collab.variables": { profile: "core", phase: "run", },
	"collab.project-metadata": { profile: "core", phase: "run", },
	"collab.flow-zone": { profile: "core", phase: "run", },
	"collab.scenario": { profile: "core", phase: "run", },
	"collab.wiki": { profile: "core", phase: "run", },
	"collab.wiki-lifecycle": { profile: "core", phase: "run", },
	"collab.notebook": { profile: "core", phase: "run", },
	"collab.insight": { profile: "core", phase: "run", },
	"collab.insight-lifecycle": { profile: "core", phase: "run", },
	"collab.dashboard": { profile: "core", phase: "run", },
	"collab.dashboard-lifecycle": { profile: "core", phase: "run", },
	"collab.project-library": { profile: "core", phase: "run", },
	"collab.jobs": { profile: "core", phase: "run", },
	"collab.metrics": { profile: "core", phase: "run", },
	"collab.data-quality": { profile: "core", phase: "run", },
	"project.lifecycle": { profile: "core", phase: "run", },
	"ml.lifecycle": { profile: "ml", phase: "run", },
	"ml.clustering-task": { profile: "ml", phase: "run", },
	"applications.template-prerequisite": { profile: "applications", phase: "run", },
	"applications.instance-lifecycle": { profile: "applications", phase: "run", },
	"infrastructure.sql-select": { profile: "infrastructure", phase: "run", },
} as const;

export type LiveCaseId = keyof typeof LIVE_CASES;

export function matchesLiveCase(id: string, selection: string,): boolean {
	return id === selection || selection.endsWith("*",) && id.startsWith(selection.slice(0, -1,),);
}
