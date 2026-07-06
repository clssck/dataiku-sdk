import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { DataikuClient, } from "../src/client.js";
import { DataikuError, } from "../src/errors.js";
import { ContinuousActivitiesResource, } from "../src/resources/continuous-activities.js";

async function readBody(req: IncomingMessage,): Promise<string> {
	let body = "";
	for await (const chunk of req) {
		body += chunk.toString();
	}
	return body;
}

function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

function createClient(url: string,): DataikuClient {
	return new DataikuClient({
		url,
		apiKey: "test-key",
		projectKey: "TEST",
	},);
}

async function withServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server = createServer((req, res,) => {
		void Promise.resolve(handler(req, res,),).catch((error: unknown,) => {
			res.statusCode = 500;
			res.end(error instanceof Error ? error.message : String(error,),);
		},);
	},);

	await new Promise<void>((resolvePromise, rejectPromise,) => {
		server.listen(0, "127.0.0.1", (error?: Error,) => {
			if (error) {
				rejectPromise(error,);
				return;
			}
			resolvePromise();
		},);
	},);

	const address = server.address();
	if (!address || typeof address === "string") {
		throw new Error("Test server did not bind to a TCP address.",);
	}
	const url = `http://127.0.0.1:${String(address.port,)}`;
	try {
		await run(url,);
	} finally {
		await new Promise<void>((resolvePromise, rejectPromise,) => {
			server.close((error,) => {
				if (error) {
					rejectPromise(error,);
					return;
				}
				resolvePromise();
			},);
		},);
	}
}

describe("ContinuousActivitiesResource", () => {
	it("lists continuous activities from the project collection endpoint", async () => {
		const activities = [
			{ projectKey: "TEST", recipeId: "continuous_recipe", desiredState: "STARTED", },
			{ projectKey: "TEST", recipeId: "other_recipe", desiredState: "STOPPED", },
		];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			sendJson(res, activities,);
		}, async (url,) => {
			const resource = new ContinuousActivitiesResource(createClient(url,),);
			await expect(resource.list(),).resolves.toEqual(activities,);
		},);

		expect(requests,).toEqual(["GET /public/api/projects/TEST/continuous-activities/",],);
	});

	it("gets status with encoded project and recipe ids", async () => {
		const status = {
			desiredState: "STOPPED",
			mainLoopState: { futureId: "future-1", },
		};
		let observedMethod = "";
		let observedPath = "";

		await withServer((req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			sendJson(res, status,);
		}, async (url,) => {
			const resource = new ContinuousActivitiesResource(createClient(url,),);
			await expect(resource.getStatus("recipe/slash", "ALT PROJECT",),).resolves.toEqual(status,);
		},);

		expect(observedMethod,).toBe("GET",);
		expect(observedPath,).toBe(
			"/public/api/projects/ALT%20PROJECT/continuous-activities/recipe%2Fslash/",
		);
	});

	it("starts an activity with loop parameters", async () => {
		const loop = {
			abortAfterCrashes: 3,
			initialRestartDelayMS: 100,
			restartDelayIncMS: 200,
			maxRestartDelayMS: 1_000,
		};
		const result = { desiredState: "STARTED", mainLoopState: { futureId: "future-2", }, };
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, result,);
		}, async (url,) => {
			const resource = new ContinuousActivitiesResource(createClient(url,),);
			await expect(resource.start("continuous recipe", loop,),).resolves.toEqual(result,);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/continuous-activities/continuous%20recipe/start",
		);
		expect(observedBody,).toEqual(loop,);
	});

	it("stops an existing activity after verifying it exists", async () => {
		const requests: string[] = [];
		let observedBody = "";

		await withServer(async (req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (
				req.method === "GET"
				&& req.url === "/public/api/projects/TEST/continuous-activities/continuous%20recipe/"
			) {
				sendJson(res, { desiredState: "STARTED", },);
				return;
			}
			if (
				req.method === "POST"
				&& req.url === "/public/api/projects/TEST/continuous-activities/continuous%20recipe/stop"
			) {
				observedBody = await readBody(req,);
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ContinuousActivitiesResource(createClient(url,),);
			await expect(resource.stop("continuous recipe",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/continuous-activities/continuous%20recipe/",
			"POST /public/api/projects/TEST/continuous-activities/continuous%20recipe/stop",
		],);
		expect(observedBody,).toBe("",);
	});

	it("propagates not_found when stopping a missing activity without posting stop", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (
				req.method === "GET"
				&& req.url === "/public/api/projects/TEST/continuous-activities/missing%20recipe/"
			) {
				sendJson(res, { message: "Continuous activity not found", }, 404,);
				return;
			}
			if (
				req.method === "POST"
				&& req.url === "/public/api/projects/TEST/continuous-activities/missing%20recipe/stop"
			) {
				res.statusCode = 500;
				res.end("unexpected POST",);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ContinuousActivitiesResource(createClient(url,),);
			const error = await resource.stop("missing recipe",).catch((caught: unknown,) => caught);
			expect(error,).toBeInstanceOf(DataikuError,);
			if (!(error instanceof DataikuError)) throw error;
			expect(error.category,).toBe("not_found",);
			expect(error.status,).toBe(404,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/continuous-activities/missing%20recipe/",
		],);
	});
});
