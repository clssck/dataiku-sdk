import { describe, expect, it, } from "bun:test";
import { cliEnv, dss, dssFailure, readBody, sendJson, withCliServer, } from "./_harness.js";

describe("CLI code-env management commands", () => {
	it("code-env create posts deployment mode and params", async () => {
		let requestBody: Record<string, unknown> | undefined;
		let requestUrl: URL | undefined;

		await withCliServer(async (req, res,) => {
			requestUrl = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(requestUrl.pathname,).toBe("/public/api/admin/code-envs/PYTHON/omp_test_env",);
			requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			sendJson(res, { messages: { success: true, }, },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"create",
				"PYTHON",
				"omp_test_env",
				"--deployment-mode",
				"DESIGN_MANAGED",
				"--python-interpreter",
				"PYTHON311",
				"--no-wait",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({
				created: "omp_test_env",
				resource: "code-env",
				envLang: "PYTHON",
				messages: { success: true, },
			},);
		},);

		expect(requestUrl?.searchParams.get("wait",),).toBe("false",);
		expect(requestBody,).toEqual({
			pythonInterpreter: "PYTHON311",
			deploymentMode: "DESIGN_MANAGED",
		},);
	});

	it("code-env set-packages preserves definition fields", async () => {
		const requests: string[] = [];
		let updateBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);

			if (req.method === "GET" && url.pathname === "/public/api/admin/code-envs/PYTHON/omp_test_env") {
				sendJson(res, {
					envName: "omp_test_env",
					envLang: "PYTHON",
					specPackageList: "oldpkg",
					desc: { installCorePackages: false, pythonInterpreter: "PYTHON311", },
				},);
				return;
			}

			if (req.method === "PUT" && url.pathname === "/public/api/admin/code-envs/PYTHON/omp_test_env") {
				updateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { updated: true, },);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"set-packages",
				"PYTHON",
				"omp_test_env",
				"--packages",
				"tabulate\nopenpyxl>=3.1.5,<3.2",
				"--install-core-packages",
				"true",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({ updated: true, },);
		},);

		expect(requests,).toEqual([
			"GET /public/api/admin/code-envs/PYTHON/omp_test_env",
			"PUT /public/api/admin/code-envs/PYTHON/omp_test_env",
		],);
		expect(updateBody,).toEqual({
			envName: "omp_test_env",
			envLang: "PYTHON",
			specPackageList: "tabulate\nopenpyxl>=3.1.5,<3.2",
			desc: { installCorePackages: true, pythonInterpreter: "PYTHON311", },
		},);
	});

	it("code-env update-packages posts rebuild query flags", async () => {
		let requestUrl: URL | undefined;

		await withCliServer((req, res,) => {
			requestUrl = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(requestUrl.pathname,).toBe("/public/api/admin/code-envs/PYTHON/omp_test_env/packages",);
			sendJson(res, { messages: { success: true, }, },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"update-packages",
				"PYTHON",
				"omp_test_env",
				"--force-rebuild",
				"--env-version",
				"bundle-v1",
				"--no-wait",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({ messages: { success: true, }, },);
		},);

		expect(requestUrl?.searchParams.get("forceRebuildEnv",),).toBe("true",);
		expect(requestUrl?.searchParams.get("versionToUpdate",),).toBe("bundle-v1",);
		expect(requestUrl?.searchParams.get("wait",),).toBe("false",);
	});

	it("code-env set-jupyter posts active and wait flags", async () => {
		let requestUrl: URL | undefined;

		await withCliServer((req, res,) => {
			requestUrl = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(requestUrl.pathname,).toBe("/public/api/admin/code-envs/PYTHON/omp_test_env/jupyter",);
			sendJson(res, { messages: { success: true, }, },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"set-jupyter",
				"PYTHON",
				"omp_test_env",
				"--active",
				"false",
				"--no-wait",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({ messages: { success: true, }, },);
		},);

		expect(requestUrl?.searchParams.get("active",),).toBe("false",);
		expect(requestUrl?.searchParams.get("wait",),).toBe("false",);
	});

	it("code-env delete calls DSS with wait flag", async () => {
		let requestUrl: URL | undefined;

		await withCliServer((req, res,) => {
			requestUrl = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("DELETE",);
			expect(requestUrl.pathname,).toBe("/public/api/admin/code-envs/PYTHON/omp_test_env",);
			sendJson(res, { messages: { success: true, }, },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"delete",
				"PYTHON",
				"omp_test_env",
				"--no-wait",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({
				deleted: "omp_test_env",
				envLang: "PYTHON",
			},);
		},);

		expect(requestUrl?.searchParams.get("wait",),).toBe("false",);
	});

	it("code-env delete treats DSS JSON-escaped missing env 500 as not_found", async () => {
		const missingEnvBody = String
			.raw`{"errorType":"com.dataiku.dip.exceptions.CodedIOException","message":"PYTHON env my_env doesn\u0027t exist"}`;
		const requests: string[] = [];

		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? ""} ${url.pathname}`,);
			expect(req.method,).toBe("DELETE",);
			expect(url.pathname,).toBe("/public/api/admin/code-envs/PYTHON/my_env",);
			res.statusCode = 500;
			res.end(missingEnvBody,);
		}, async (url,) => {
			const skipped = JSON.parse(
				(await dss(["code-env", "delete", "PYTHON", "my_env", "--if-exists",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(skipped,).toMatchObject({
				skipped: "my_env",
				reason: "missing",
			},);

			const failure = await dssFailure(["code-env", "delete", "PYTHON", "my_env",], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(2,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report.code,).toBe("not_found",);
		},);

		expect(requests,).toEqual([
			"DELETE /public/api/admin/code-envs/PYTHON/my_env",
			"DELETE /public/api/admin/code-envs/PYTHON/my_env",
		],);
	});

	it("code-env delete dry-run reads current state without deleting", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/admin/code-envs/PYTHON/omp_test_env/",);
			sendJson(res, { envName: "omp_test_env", envLang: "PYTHON", },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"delete",
				"PYTHON",
				"omp_test_env",
				"--dry-run",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toMatchObject({
				dryRun: true,
				action: "delete",
				resource: "code-env",
				envLang: "PYTHON",
				envName: "omp_test_env",
				wait: true,
				current: { envName: "omp_test_env", envLang: "PYTHON", },
			},);
		},);
	});
});

describe("CLI wait command exit codes", () => {
	it("job wait exits non-zero when the wait result times out", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-timeout/",);
			sendJson(res, {
				baseStatus: {
					def: { id: "job-timeout", type: "DATASET_BUILD", },
					state: "RUNNING",
				},
				globalState: { done: 0, failed: 0, running: 1, total: 1, },
			},);
		}, async (url,) => {
			const failure = await dssFailure(["job", "wait", "job-timeout", "--timeout", "10",], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(4,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				ok: false,
				code: "long_running_failure",
				category: "dss",
				exitCode: 4,
				resource: "job",
				action: "wait",
			},);
			const details = report.details as { result: Record<string, unknown>; };
			expect(details.result.success,).toBe(false,);
			expect(details.result.timedOut,).toBe(true,);
			expect(details.result.state,).toBe("RUNNING",);
		},);
	});
});
