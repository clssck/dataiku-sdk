import { describe, expect, it, } from "bun:test";
import { cliEnv, dss, dssFailure, readBody, sendJson, withCliServer, } from "./_harness.js";

type RecordedRequest = {
	method: string;
	path: string;
	body?: unknown;
};

describe("Visual ML CLI endpoints", () => {
	it("maps Visual ML writes and prediction bridging to exact requests", async () => {
		const requests: RecordedRequest[] = [];
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			const text = await readBody(req,);
			requests.push({
				method: req.method ?? "GET",
				path: `${url.pathname}${url.search}`,
				...(text.length > 0 ? { body: JSON.parse(text,), } : {}),
			},);

			if (req.method === "GET" && url.pathname.endsWith("/apiservices/service%2F1/settings",)) {
				sendJson(res, {
					authRealm: "project-default",
					endpoints: [{ id: "existing", type: "PY_FUNCTION", },],
					versionTag: { versionNumber: 4, },
				},);
				return;
			}
			if (url.pathname === "/public/api/projects/PROJECT%20KEY/lab/") {
				sendJson(res, { id: "analysis-created", },);
				return;
			}
			if (url.pathname.endsWith("/lab/analysis%2F1/models/",)) {
				sendJson(res, { mlTaskId: "task-created", },);
				return;
			}
			if (url.pathname === "/public/api/projects/PROJECT%20KEY/evaluationstores/") {
				sendJson(res, { id: "store-created", },);
				return;
			}
			sendJson(res, { saved: true, },);
		}, async (url,) => {
			const env = cliEnv(url,);
			await dss([
				"analysis",
				"create",
				"--input-dataset",
				"input/name",
				"--project-key",
				"PROJECT KEY",
			], { env, },);
			await dss([
				"ml-task",
				"create",
				"analysis/1",
				"--task-type",
				"prediction",
				"--target",
				"churn flag",
				"--prediction-type",
				"BINARY_CLASSIFICATION",
				"--guess-policy",
				"DEFAULT",
				"--project-key",
				"PROJECT KEY",
			], { env, },);
			await dss([
				"saved-model",
				"set-active",
				"saved/1",
				"v 1",
				"--project-key",
				"PROJECT KEY",
			], { env, },);
			await dss([
				"model-evaluation-store",
				"create",
				"--name",
				"Evaluation store",
				"--project-key",
				"PROJECT KEY",
			], { env, },);
			await dss([
				"api-service",
				"add-prediction-endpoint",
				"service/1",
				"predict churn",
				"saved/1",
				"--project-key",
				"PROJECT KEY",
			], { env, },);
		},);

		expect(requests,).toEqual([
			{
				method: "POST",
				path: "/public/api/projects/PROJECT%20KEY/lab/",
				body: { inputDataset: "input/name", },
			},
			{
				method: "POST",
				path: "/public/api/projects/PROJECT%20KEY/lab/analysis%2F1/models/",
				body: {
					taskType: "PREDICTION",
					targetVariable: "churn flag",
					predictionType: "BINARY_CLASSIFICATION",
					backendType: "PY_MEMORY",
					guessPolicy: "DEFAULT",
				},
			},
			{
				method: "POST",
				path:
					"/public/api/projects/PROJECT%20KEY/savedmodels/saved%2F1/versions/v%201/actions/setActive",
				body: {},
			},
			{
				method: "POST",
				path: "/public/api/projects/PROJECT%20KEY/evaluationstores/?flavor=TABULAR",
				body: { projectKey: "PROJECT KEY", name: "Evaluation store", },
			},
			{
				method: "GET",
				path: "/public/api/projects/PROJECT%20KEY/apiservices/service%2F1/settings",
			},
			{
				method: "PUT",
				path: "/public/api/projects/PROJECT%20KEY/apiservices/service%2F1/settings",
				body: {
					authRealm: "project-default",
					endpoints: [
						{ id: "existing", type: "PY_FUNCTION", },
						{ id: "predict churn", type: "STD_PREDICTION", modelRef: "saved/1", },
					],
					versionTag: { versionNumber: 4, },
				},
			},
		],);
	});

	it("rejects prediction tasks without a target before making a request", async () => {
		const failure = await dssFailure([
			"ml-task",
			"create",
			"analysis",
			"--task-type",
			"prediction",
		], { env: cliEnv("http://127.0.0.1:1",), },);

		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		expect(failure.stdout,).toContain("--target is required for PREDICTION ML tasks.",);
	});
});
