import { describe, expect, it, } from "bun:test";
import { writeFileSync, } from "node:fs";
import { createServer, } from "node:http";
import { type AddressInfo, } from "node:net";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { DataikuClient, } from "../src/client.js";

describe("Project Deployer and bundle endpoints", () => {
	it("call the documented routes", async () => {
		const seen: string[] = [];
		const server = createServer(async (req, res,) => {
			const drained = Promise.withResolvers<void>();
			req.resume().on("end", () => drained.resolve(),);
			await drained.promise;
			seen.push(`${req.method} ${req.url}`,);
			res.setHeader("Content-Type", "application/json",);
			res.end(req.url?.includes("stages",) ? "[]" : "{}",);
		},);
		const listening = Promise.withResolvers<void>();
		server.listen(0, "127.0.0.1", () => listening.resolve(),);
		await listening.promise;
		const { port, } = server.address() as AddressInfo;
		const client = new DataikuClient({
			url: `http://127.0.0.1:${port}`,
			apiKey: "k",
			projectKey: "P",
			retryMaxAttempts: 1,
		},);
		const bundleFile = join(tmpdir(), `dss-bundle-${Date.now()}.zip`,);
		writeFileSync(bundleFile, "zip",);
		try {
			const pd = client.projectDeployer;
			await pd.listStages();
			await pd.getInfra("prod",);
			await pd.getInfraSettings("prod",);
			await pd.saveInfraSettings("prod", { id: "prod", },);
			await pd.deleteInfra("prod",);
			await pd.getDeploymentSettings("d1",);
			await pd.getGovernanceStatus("d1", "v2",);
			await pd.deleteProject("PUB",);
			await pd.getProjectSettings("PUB",);
			await pd.saveProjectSettings("PUB", { id: "PUB", },);
			await pd.deleteBundle("PUB", "v1",);
			await client.bundles.getExported("v1",);
			await client.bundles.createProjectFromBundle({ archivePath: "/b/v1.zip", }, "F1",);
			await client.bundles.createProjectFromBundle({ filePath: bundleFile, },);
		} finally {
			const closed = Promise.withResolvers<void>();
			server.close(() => closed.resolve());
			await closed.promise;
		}
		expect(seen,).toEqual([
			"GET /public/api/project-deployer/stages",
			"GET /public/api/project-deployer/infras/prod",
			"GET /public/api/project-deployer/infras/prod/settings",
			"PUT /public/api/project-deployer/infras/prod/settings",
			"DELETE /public/api/project-deployer/infras/prod",
			"GET /public/api/project-deployer/deployments/d1/settings",
			"GET /public/api/project-deployer/deployments/d1/governance-status?bundleId=v2",
			"DELETE /public/api/project-deployer/projects/PUB",
			"GET /public/api/project-deployer/projects/PUB/settings",
			"PUT /public/api/project-deployer/projects/PUB/settings",
			"DELETE /public/api/project-deployer/projects/PUB/bundles/v1",
			"GET /public/api/projects/P/bundles/exported/v1",
			"POST /public/api/projectsFromBundle/fromArchive?archivePath=%2Fb%2Fv1.zip&projectFolderId=F1",
			"POST /public/api/projectsFromBundle/",
		],);
	});
});
