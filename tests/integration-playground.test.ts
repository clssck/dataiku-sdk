import { describe, expect, it, } from "bun:test";
import { validateCredentials, } from "../src/auth.js";
import {
	createClient,
	describeIntegration,
	describeMutatingProjectIntegration,
	describeProjectIntegration,
	dss,
	parseJsonOutput,
	parseTlsRejectUnauthorized,
	uniqueTestName,
} from "./integration-harness.js";

function tlsSettings(): {
	caCertPath?: string;
	tlsRejectUnauthorized?: boolean;
} {
	return {
		caCertPath: process.env.NODE_EXTRA_CA_CERTS,
		tlsRejectUnauthorized: parseTlsRejectUnauthorized(process.env.NODE_TLS_REJECT_UNAUTHORIZED,),
	};
}

describeIntegration("Dataiku playground integration: authentication", () => {
	it("validates credentials from .env", async () => {
		const result = await validateCredentials(
			process.env.DATAIKU_URL!,
			process.env.DATAIKU_API_KEY!,
			tlsSettings(),
		);
		expect(result.valid, result.error,).toBe(true,);
	});

	it("lists accessible projects through SDK and CLI", async () => {
		const client = createClient();
		const projects = await client.projects.list();
		expect(Array.isArray(projects,),).toBe(true,);
		expect(projects.length,).toBeGreaterThan(0,);

		const cliProjects = await dss(["project", "list",],);
		expect(JSON.parse(cliProjects.stdout,),).toHaveLength(projects.length,);
		expect(cliProjects.stderr,).toBe("",);
	});
},);

describeProjectIntegration("Dataiku playground integration: read-only SDK resources", () => {
	it("exercises project-scoped resource list/read endpoints", async () => {
		const client = createClient();

		const project = await client.projects.get();
		expect(project.projectKey,).toBe(process.env.DATAIKU_PROJECT_KEY,);
		expect(await client.projects.metadata(),).toBeDefined();
		expect(await client.projects.flow(),).toBeDefined();
		expect(await client.projects.map({ maxNodes: 25, maxEdges: 50, },),).toBeDefined();
		const flowZones = await client.flowZones.list();

		expect(Array.isArray(flowZones,),).toBe(true,);
		const [
			datasets,
			recipes,
			jobs,
			scenarios,
			folders,
			variables,
			jupyter,
			sqlNotebooks,
			connections,
			dashboards,
			insights,
			wikiSettings,
		] = await Promise.all([
			client.datasets.list(),
			client.recipes.list(),
			client.jobs.list(),
			client.scenarios.list(),
			client.folders.list(),
			client.variables.get(),
			client.notebooks.listJupyter(),
			client.notebooks.listSql(),
			client.connections.infer(),
			client.dashboards.list(),
			client.insights.list(),
			client.wiki.settings(),
		],);

		expect(Array.isArray(datasets,),).toBe(true,);
		expect(Array.isArray(recipes,),).toBe(true,);
		expect(Array.isArray(jobs,),).toBe(true,);
		expect(Array.isArray(scenarios,),).toBe(true,);
		expect(Array.isArray(folders,),).toBe(true,);
		expect(variables,).toHaveProperty("standard",);
		expect(variables,).toHaveProperty("local",);
		expect(Array.isArray(jupyter,),).toBe(true,);
		expect(Array.isArray(sqlNotebooks,),).toBe(true,);
		expect(Array.isArray(connections,),).toBe(true,);
		expect(Array.isArray(dashboards,),).toBe(true,);
		expect(Array.isArray(insights,),).toBe(true,);
		expect(wikiSettings,).toHaveProperty("projectKey",);
		const firstDatasetName = datasets.find((dataset,) => typeof dataset.name === "string")?.name;
		if (firstDatasetName) {
			expect(Array.isArray(await client.dataQuality.listRules(firstDatasetName,),),).toBe(true,);
			expect(await client.dataQuality.statusByPartition(firstDatasetName,),).toBeDefined();
			expect(Array.isArray(await client.dataQuality.lastResults(firstDatasetName,),),).toBe(true,);
			expect(Array.isArray(await client.dataQuality.history(firstDatasetName,),),).toBe(true,);
		}
	});
},);

describeProjectIntegration("Dataiku playground integration: read-only CLI commands", () => {
	it("runs resource list/info commands against the configured project", async () => {
		const datasetName = (await createClient().datasets.list()).find((dataset,) =>
			typeof dataset.name === "string"
		)?.name;
		const commands: string[][] = [
			["project", "get",],
			["project", "metadata",],
			["project", "map", "--max-nodes", "25", "--max-edges", "50",],
			["flow-zone", "list",],
			["dataset", "list",],
			["recipe", "list",],
			["job", "list",],
			["scenario", "list",],
			["folder", "list",],
			["variable", "get",],
			["connection", "infer",],
			["notebook", "list-jupyter",],
			["notebook", "list-sql",],
			["dashboard", "list",],
			["insight", "list",],
			["wiki", "settings",],
			["wiki", "list",],
			["data-quality", "project-status",],
			["data-quality", "project-timeline",],
		];
		if (datasetName) {
			commands.push(
				["data-quality", "rules", datasetName,],
				["data-quality", "status-by-partition", datasetName,],
				["data-quality", "last-results", datasetName,],
				["data-quality", "history", datasetName,],
			);
		}

		for (const command of commands) {
			const result = await dss(command,);
			expect(result.stderr, command.join(" ",),).toBe("",);
			expect(() => JSON.parse(result.stdout,), command.join(" ",),).not.toThrow();
		}
	}, 30_000,);
},);

describeMutatingProjectIntegration("Dataiku playground integration: flow zone mutations", () => {
	it("creates, updates, moves a dataset into, and deletes a temporary flow zone", async () => {
		const client = createClient();
		const zoneName = uniqueTestName("sdk_cli_integration",);
		let zoneId: string | undefined;

		try {
			const created = await client.flowZones.create({ name: zoneName, color: "#2ab1ac", },);
			zoneId = created.id;
			expect(created.name,).toBe(zoneName,);

			const updated = await client.flowZones.update(zoneId, {
				name: `${zoneName}_renamed`,
				color: "#cc0000",
			},);
			expect(updated.name,).toBe(`${zoneName}_renamed`,);
			expect(updated.color,).toBe("#cc0000",);

			const datasets = await client.datasets.list();
			const firstDataset = datasets.find((dataset,) => typeof dataset.name === "string");
			if (firstDataset?.name) {
				const moved = await client.flowZones.moveItems(zoneId, [
					{ objectType: "DATASET", objectId: firstDataset.name, },
				],);
				expect(
					moved.items?.some((item,) =>
						item.objectType === "DATASET" && item.objectId === firstDataset.name
					),
				).toBe(true,);
			}

			const { stdout, } = await dss([
				"flow-zone",
				"get",
				zoneId,
			],);
			expect(parseJsonOutput<{ id: string; }>(stdout,).id,).toBe(zoneId,);
		} finally {
			if (zoneId) await client.flowZones.delete(zoneId,);
		}
	});
},);
