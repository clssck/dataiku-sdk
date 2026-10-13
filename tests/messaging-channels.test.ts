import { describe, expect, it, } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { DataikuClient, } from "../src/client.js";
import { ClientValidationError, DataikuError, } from "../src/errors.js";
import {
	mailMessagePayload,
	MessagingChannelsResource,
} from "../src/resources/messaging-channels.js";
import {
	messagingChannelSecretsFromBody,
	messagingChannelSettingsHash,
	sanitizeMessagingChannelSecrets,
} from "../src/utils/messaging-channel-secrets.js";
import { readBody, sendJson, withCliServer, } from "./cli/_harness.js";

type Seen = { method: string; path: string; body: string; type: string; };

async function withChannels(
	respond: (seen: Seen, status: { code: number; },) => unknown,
	run: (channels: MessagingChannelsResource, seen: Seen[],) => Promise<void>,
): Promise<void> {
	const seen: Seen[] = [];
	await withCliServer(async (req, res,) => {
		const entry: Seen = {
			method: req.method ?? "",
			path: req.url ?? "",
			body: await readBody(req,),
			type: req.headers["content-type"] ?? "",
		};
		seen.push(entry,);
		const status = { code: 200, };
		const body = respond(entry, status,);
		if (body === undefined) {
			res.statusCode = status.code;
			res.end();
		} else {
			sendJson(res, body, status.code,);
		}
	}, async (url,) => {
		const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
		await run(new MessagingChannelsResource(client,), seen,);
	},);
}

describe("MessagingChannelsResource", () => {
	it("lists with encoded filters and rejects a non-array response", async () => {
		await withChannels(
			(entry,) => entry.path.includes("bad",) ? { oops: true, } : [{ id: "a", },],
			async (channels, seen,) => {
				await expect(channels.list(),).resolves.toEqual([{ id: "a", },],);
				await channels.list({ channelType: "aws-ses-mail", channelFamily: "mail", },);
				expect(seen.map((entry,) => `${entry.method} ${entry.path}`),).toEqual([
					"GET /public/api/messaging-channels/",
					"GET /public/api/messaging-channels/?channelType=aws-ses-mail&channelFamily=mail",
				],);
			},
		);
		await withChannels(() => ({ oops: true, }), async (channels,) => {
			await expect(channels.list(),).rejects.toThrow(/did not return an array/,);
		},);
	});

	it("gets, reads and replaces settings and permissions, and deletes, with encoded ids", async () => {
		await withChannels(
			(entry,) => entry.method === "GET" ? { id: "x", } : undefined,
			async (channels, seen,) => {
				await channels.get("my/channel",);
				await channels.getSettings("my/channel",);
				await channels.saveSettings("my/channel", { sender: "a@example.com", },);
				await channels.getPermissions("my/channel",).catch(() => undefined);
				await channels.setPermissions("my/channel", [{ group: "g", canUse: true, },],);
				await channels.delete("my/channel",);
				expect(seen.map((entry,) => `${entry.method} ${entry.path}`),).toEqual([
					"GET /public/api/messaging-channels/my%2Fchannel",
					"GET /public/api/messaging-channels/my%2Fchannel/settings",
					"PUT /public/api/messaging-channels/my%2Fchannel/settings",
					"GET /public/api/messaging-channels/my%2Fchannel/permissions",
					"PUT /public/api/messaging-channels/my%2Fchannel/permissions",
					"DELETE /public/api/messaging-channels/my%2Fchannel",
				],);
				expect(JSON.parse(seen[2]!.body,),).toEqual({ sender: "a@example.com", },);
				expect(JSON.parse(seen[4]!.body,),).toEqual([{ group: "g", canUse: true, },],);
			},
		);
	});

	it("creates with the official client's body: empty id and null permissions by default", async () => {
		await withChannels(() => ({ id: "gen", type: "shell", }), async (channels, seen,) => {
			await channels.create({ type: "shell", },);
			await channels.create({
				type: "smtp",
				id: "mail",
				configuration: { host: "h", },
				permissions: [{ user: "u", canUse: true, },],
			},);
			expect(seen.map((entry,) => JSON.parse(entry.body,)),).toEqual([
				{ id: "", type: "shell", configuration: {}, permissions: null, },
				{
					id: "mail",
					type: "smtp",
					configuration: { host: "h", },
					permissions: [{ user: "u", canUse: true, },],
				},
			],);
			await expect(channels.create({ type: " ", },),).rejects.toBeInstanceOf(ClientValidationError,);
		},);
	});

	it("sends a multipart message part and one attachments part per file, never retrying", async () => {
		const dir = mkdtempSync(join(tmpdir(), "dss-channels-sdk-",),);
		try {
			const first = join(dir, "a.csv",);
			const second = join(dir, "b.txt",);
			writeFileSync(first, "x,y\n1,2\n",);
			writeFileSync(second, "hello",);
			await withChannels(() => undefined, async (channels, seen,) => {
				await channels.send(
					"smtp",
					{ to: ["a@example.com",], subject: "S", body: "<b>B</b>", bcc: ["z@example.com",], },
					{
						projectKey: "PROJ",
						sendAsSingleMessage: true,
						attachments: [{ path: first, }, { path: second, fileName: "renamed.txt", },],
					},
				);
				expect(seen.length,).toBe(1,);
				expect(seen[0]!.path,).toBe(
					"/public/api/messaging-channels/smtp/actions/send?projectKey=PROJ&sendAsSingleMessage=true",
				);
				expect(seen[0]!.type,).toStartWith("multipart/form-data",);
				expect(seen[0]!.body,).toMatch(
					/name="message"; filename="send-payload\.json"\r\nContent-Type: application\/json/,
				);
				expect(seen[0]!.body,).toMatch(/name="attachments"; filename="a\.csv"/,);
				expect(seen[0]!.body,).toMatch(/name="attachments"; filename="renamed\.txt"/,);
				expect(seen[0]!.body,).toContain("x,y\n1,2\n",);
				expect(seen[0]!.body,).toContain("hello",);
				expect(seen[0]!.body,).toContain(JSON.stringify(mailMessagePayload({
					to: ["a@example.com",],
					subject: "S",
					body: "<b>B</b>",
					bcc: ["z@example.com",],
				},),),);
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("defaults the project key to the client's and the acknowledgement may be non-JSON", async () => {
		const seen: Seen[] = [];
		await withCliServer(async (req, res,) => {
			seen.push({
				method: req.method ?? "",
				path: req.url ?? "",
				body: await readBody(req,),
				type: "",
			},);
			res.setHeader("Content-Type", "text/plain",);
			res.end("OK",);
		}, async (url,) => {
			const channels = new MessagingChannelsResource(
				new DataikuClient({ url, apiKey: "k", projectKey: "DEFAULT", },),
			);
			await channels.send("c", { to: ["a@example.com",], subject: "s", body: "b", },);
		},);
		expect(seen[0]!.path,).toBe(
			"/public/api/messaging-channels/c/actions/send?projectKey=DEFAULT&sendAsSingleMessage=false",
		);
	});

	it("fails before sending when an attachment cannot be read", async () => {
		await withChannels(() => undefined, async (channels, seen,) => {
			await expect(
				channels.send("c", { to: ["a@example.com",], subject: "s", body: "b", }, {
					attachments: [{ path: "/nonexistent/file.bin", },],
				},),
			).rejects.toBeInstanceOf(ClientValidationError,);
			expect(seen,).toEqual([],);
		},);
	});

	it("does not retry a failed send", async () => {
		await withChannels((_entry, status,) => {
			status.code = 503;
			return { message: "down", };
		}, async (channels, seen,) => {
			await expect(
				channels.send("c", { to: ["a@example.com",], subject: "s", body: "b", },),
			).rejects.toBeInstanceOf(DataikuError,);
			expect(seen.length,).toBe(1,);
		},);
	});
});

describe("messaging channel secret redaction", () => {
	const secrets = {
		password: "pw-1",
		secretKey: "sk-2",
		clientSecret: "cs-3",
		authorizationToken: "at-4",
		authToken: "au-5",
		webhookUrl: "https://hooks.example/T/B/SECRET6",
		webhookKey: "wk-7",
		webhookToken: "wt-8",
		accessKey: "ak-9",
	};

	it("redacts every credential field at any depth and leaves identifiers visible", () => {
		const out = JSON.stringify(
			sanitizeMessagingChannelSecrets({
				id: "c",
				type: "smtp",
				login: "mailer",
				sender: "noreply@example.com",
				useProxy: true,
				configuration: secrets,
				list: [secrets,],
			},),
		);
		for (const value of Object.values(secrets,)) expect(out,).not.toContain(value,);
		expect(out,).toContain("mailer",);
		expect(out,).toContain("noreply@example.com",);
	});

	it("redacts secret session properties by flag or name, keeping the others", () => {
		const out = sanitizeMessagingChannelSecrets({
			sessionProperties: [
				{ key: "mail.custom", value: "flagged", secret: true, },
				{ key: "mail.smtp.password", value: "named", secret: false, },
				{ key: "mail.debug", value: "true", secret: false, },
			],
		},);
		expect(out.sessionProperties,).toEqual([
			{ key: "mail.custom", value: "[redacted]", secret: true, },
			{ key: "mail.smtp.password", value: "[redacted]", secret: false, },
			{ key: "mail.debug", value: "true", secret: false, },
		],);
	});

	it("collects exact secret values from a body, session properties included", () => {
		const found = messagingChannelSecretsFromBody({
			configuration: { ...secrets, sessionProperties: [{ key: "x.token", value: "sp-10", },], },
		},);
		for (const value of [...Object.values(secrets,), "sp-10",]) expect(found,).toContain(value,);
	});

	it("hashes the redacted view: stable across credential edits, sensitive to everything else", () => {
		const base = {
			host: "h",
			password: "one",
			sessionProperties: [{ key: "a.password", value: "v", },],
		};
		expect(messagingChannelSettingsHash(base,),).toBe(
			messagingChannelSettingsHash({
				...base,
				password: "two",
				sessionProperties: [{ key: "a.password", value: "w", },],
			},),
		);
		expect(messagingChannelSettingsHash(base,),).not.toBe(
			messagingChannelSettingsHash({ ...base, host: "other", },),
		);
	});
});
