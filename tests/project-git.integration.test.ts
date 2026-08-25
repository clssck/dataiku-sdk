import { describe, expect, it, } from "bun:test";
import {
	createClient,
	dssRaw,
	hasCredentials,
	integrationEnabled,
	parseJsonOutput,
} from "./integration-harness.js";

const gitProjectKey = process.env.DATAIKU_GIT_PROJECT_KEY?.trim();
const describeProjectGitIntegration = integrationEnabled
		&& hasCredentials
		&& process.env.RUN_DATAIKU_GIT_INTEGRATION === "1"
		&& gitProjectKey
	? describe
	: describe.skip;

describeProjectGitIntegration("Project Git live integration", () => {
	it("returns the same status through the SDK and CLI", async () => {
		const sdkStatus = await createClient().projectGit.status(gitProjectKey!,);
		const cli = await dssRaw(["project-git", "status", "--project-key", gitProjectKey!,],);

		expect(cli.code, cli.stderr,).toBe(0,);
		expect(parseJsonOutput(cli.stdout,),).toEqual(sdkStatus,);
	});

	it("reads the current branch and local branches without mutation", async () => {
		const client = createClient();
		const [sdkCurrentBranch, sdkBranches,] = await Promise.all([
			client.projectGit.currentBranch(gitProjectKey!,),
			client.projectGit.listBranches(gitProjectKey!,),
		],);
		const [cliCurrentBranch, cliBranches,] = await Promise.all([
			dssRaw(["project-git", "current-branch", "--project-key", gitProjectKey!,],),
			dssRaw(["project-git", "branches", "--project-key", gitProjectKey!,],),
		],);

		expect(cliCurrentBranch.code, cliCurrentBranch.stderr,).toBe(0,);
		expect(cliBranches.code, cliBranches.stderr,).toBe(0,);
		expect(parseJsonOutput(cliCurrentBranch.stdout,),).toEqual(sdkCurrentBranch,);
		expect(parseJsonOutput(cliBranches.stdout,),).toEqual(sdkBranches,);
	});
},);
