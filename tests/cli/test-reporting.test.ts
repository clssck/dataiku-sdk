import { describe, expect, it, } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { buildCommandRegistry, } from "../../src/cli/contract.js";
import { cliEnv, dss, dssFailure, sendJson, withCliServer, } from "./_harness.js";

const JUNIT =
	'<?xml version="1.0"?><testsuites><testsuite name="nightly" tests="2" failures="0"/></testsuites>';
const HTML = "<!DOCTYPE html><html><body><h1>Test report</h1></body></html>";

describe("project test reports", () => {
	it("returns the JUnit XML in the report field and honours --bundle-id", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			res.setHeader("Content-Type", "application/xml",);
			res.end(JUNIT,);
		}, async (url,) => {
			const env = cliEnv(url,);
			const plain = JSON.parse((await dss(["project", "test-report",], { env, },)).stdout,);
			expect(plain,).toEqual({ format: "junit", report: JUNIT, },);
			await dss(["project", "test-report", "--bundle-id", "v 2", "--project-key", "OTHER",], {
				env,
			},);
		},);
		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/scenarios/last-test-scenario-runs-report",
			"GET /public/api/projects/OTHER/scenarios/last-test-scenario-runs-report?bundleId=v%202",
		],);
	});

	it("writes the HTML report to --output and requires --output for html", async () => {
		const dir = mkdtempSync(join(tmpdir(), "dss-test-report-",),);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method} ${req.url}`,);
				res.setHeader("Content-Type", "text/html",);
				res.end(HTML,);
			}, async (url,) => {
				const env = cliEnv(url,);
				const target = join(dir, "report.html",);
				const out = JSON.parse(
					(await dss(["project", "test-report", "--format", "html", "--output", target,], { env, },))
						.stdout,
				);
				expect(out,).toEqual({ path: target, bytes: Buffer.byteLength(HTML,), format: "html", },);
				expect(readFileSync(target, "utf-8",),).toBe(HTML,);
				const xmlTarget = join(dir, "report.xml",);
				await dss(["project", "test-report", "--output", xmlTarget,], { env, },);
				const failure = await dssFailure(["project", "test-report", "--format", "html",], { env, },);
				expect(failure.code,).toBe(1,);
				const badFormat = await dssFailure(["project", "test-report", "--format", "pdf",], { env, },);
				expect(badFormat.code,).toBe(1,);
			},);
			expect(requests,).toEqual([
				"GET /public/api/projects/TEST/scenarios/last-test-scenario-runs-html-report",
				"GET /public/api/projects/TEST/scenarios/last-test-scenario-runs-report",
			],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("reads the project testing status", async () => {
		const requests: string[] = [];
		const status = { nbTotalRanScenarios: 1, nbScenariosPerOutcome: { SUCCESS: 1, }, };
		await withCliServer((req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			sendJson(res, status,);
		}, async (url,) => {
			const out = await dss(["project", "testing-status", "--bundle-id", "b1",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(out.stdout,),).toEqual(status,);
		},);
		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/scenarios/testing-status?bundleId=b1",
		],);
	});
});

describe("project-deployer testing", () => {
	it("reads the testing status with optional bundle and automation node filters", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			sendJson(res, { nbTotalRanScenarios: 0, },);
		}, async (url,) => {
			const env = cliEnv(url,);
			await dss(["project-deployer", "testing-status", "dep/1",], { env, },);
			await dss([
				"project-deployer",
				"testing-status",
				"dep1",
				"--bundle-id",
				"v1",
				"--automation-node-id",
				"node a",
			], { env, },);
		},);
		expect(requests,).toEqual([
			"GET /public/api/project-deployer/deployments/dep%2F1/testing-status",
			"GET /public/api/project-deployer/deployments/dep1/testing-status?bundleId=v1&automationNodeId=node+a",
		],);
	});

	it("runs the test scenarios with a bodyless POST, and plan/dry-run send nothing", async () => {
		const requests: Array<{ line: string; body: string; }> = [];
		await withCliServer(async (req, res,) => {
			let body = "";
			for await (const chunk of req) body += chunk.toString();
			requests.push({ line: `${req.method} ${req.url}`, body, },);
			sendJson(res, { nbTotalRanScenarios: 2, },);
		}, async (url,) => {
			const env = cliEnv(url,);
			const argv = ["project-deployer", "run-test-scenarios", "dep1", "--automation-node-id", "n1",];
			for (const mode of ["--plan", "--dry-run",]) {
				const out = JSON.parse((await dss([...argv, mode,], { env, },)).stdout,);
				expect(out,).toMatchObject({
					method: "POST",
					endpoint:
						"/public/api/project-deployer/deployments/dep1/run-test-scenarios?automationNodeId=n1",
				},);
			}
			expect(requests,).toEqual([],);
			const out = JSON.parse((await dss(argv, { env, },)).stdout,);
			expect(out,).toEqual({ nbTotalRanScenarios: 2, },);
			await dss(["project-deployer", "run-test-scenarios", "dep1",], { env, },);
		},);
		expect(requests,).toEqual([
			{
				line:
					"POST /public/api/project-deployer/deployments/dep1/run-test-scenarios?automationNodeId=n1",
				body: "",
			},
			{ line: "POST /public/api/project-deployer/deployments/dep1/run-test-scenarios", body: "", },
		],);
	});

	it("classifies reads as reads and run-test-scenarios as a plannable write", () => {
		const projectDeployer = buildCommandRegistry("project-deployer",)["project-deployer"]!;
		const project = buildCommandRegistry("project",)["project"]!;
		expect(projectDeployer["testing-status"]!.sideEffect,).toBe("read",);
		expect(projectDeployer["run-test-scenarios"]!.sideEffect,).toBe("write",);
		expect(projectDeployer["run-test-scenarios"]!.dryRun,).toBe(true,);
		expect(project["test-report"]!.sideEffect,).toBe("read",);
		expect(project["test-report"]!.producesLocalFile,).toBe(true,);
		expect(project["testing-status"]!.sideEffect,).toBe("read",);
	});
});
