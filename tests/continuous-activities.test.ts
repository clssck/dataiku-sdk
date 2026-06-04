import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
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

	const { port, } = server.address() as AddressInfo;
	const url = `http://127.0.0.1:${String(port,)}`;
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

	it("starts an activity with empty loop parameters by default", async () => {
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { desiredState: "STARTED", },);
		}, async (url,) => {
			const resource = new ContinuousActivitiesResource(createClient(url,),);
			await expect(resource.start("recipe-default",),).resolves.toEqual({
				desiredState: "STARTED",
			},);
		},);

		expect(observedBody,).toEqual({},);
	});

	it("stops an activity through the stop action endpoint without a body", async () => {
		let observedMethod = "";
		let observedPath = "";
		let observedBody = "";

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = await readBody(req,);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ContinuousActivitiesResource(createClient(url,),);
			await expect(resource.stop("continuous recipe",),).resolves.toBeUndefined();
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/continuous-activities/continuous%20recipe/stop",
		);
		expect(observedBody,).toBe("",);
	});
});
