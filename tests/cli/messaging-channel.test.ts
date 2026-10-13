import { describe, expect, it, } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { buildCommandRegistry, } from "../../src/cli/contract.js";
import {
	cliEnv,
	dss,
	dssFailure,
	type IncomingMessage,
	readBody,
	sendJson,
	type ServerResponse,
	withCliServer,
} from "./_harness.js";

type Seen = { method: string; path: string; body: string; };

const WEBHOOK = "https://hooks.slack.example/services/T000/B000/XXXXSECRETXXXX";
const PASSWORD = "S3cretPassw0rd";
const SESSION_SECRET = "SessionSecretValue";

/** Stored SMTP settings as DSS returns them: credentials in clear, `secret` flags on session properties. */
const smtpSettings = {
	host: "smtp.example.com",
	port: 587,
	login: "mailer",
	password: PASSWORD,
	sender: "noreply@example.com",
	sessionProperties: [
		{ key: "mail.smtp.password", value: SESSION_SECRET, secret: false, },
		{ key: "mail.debug", value: "true", secret: false, },
	],
};

function mockDss(
	seen: Seen[],
	respond: (req: IncomingMessage, res: ServerResponse,) => void,
): (req: IncomingMessage, res: ServerResponse,) => Promise<void> {
	return async (req, res,) => {
		seen.push({ method: req.method ?? "", path: req.url ?? "", body: await readBody(req,), },);
		respond(req, res,);
	};
}

function noSecretsIn(output: string,): void {
	for (const secret of [WEBHOOK, PASSWORD, SESSION_SECRET, "XXXXSECRETXXXX",]) {
		expect(output,).not.toContain(secret,);
	}
}

async function settingsGet(url: string,): Promise<string> {
	return (await dss(["messaging-channel", "settings-get", "smtp-main",], { env: cliEnv(url,), },))
		.stdout;
}

describe("messaging-channel reads", () => {
	it("lists with type and family filters, compact by default and redacted with --full", async () => {
		const seen: Seen[] = [];
		const channels = [
			{ id: "smtp-main", type: "smtp", family: "mail", sender: "noreply@example.com", },
			{ id: "chat", type: "slack", webhookUrl: WEBHOOK, },
		];
		await withCliServer(mockDss(seen, (_req, res,) => sendJson(res, channels,),), async (url,) => {
			const env = cliEnv(url,);
			const compact = JSON.parse(
				(await dss(["messaging-channel", "list", "--type", "smtp", "--family", "mail",], { env, },))
					.stdout,
			);
			expect(compact,).toEqual([
				{ id: "smtp-main", type: "smtp", family: "mail", },
				{ id: "chat", type: "slack", },
			],);
			const full = (await dss(["messaging-channel", "list", "--full",], { env, },)).stdout;
			noSecretsIn(full,);
			expect(JSON.parse(full,),).toEqual([channels[0], {
				...channels[1],
				webhookUrl: "[redacted]",
			},],);
		},);
		expect(seen.map((request,) => `${request.method} ${request.path}`),).toEqual([
			"GET /public/api/messaging-channels/?channelType=smtp&channelFamily=mail",
			"GET /public/api/messaging-channels/",
		],);
	});

	it("redacts credentials in get and settings-get and hashes only the redacted view", async () => {
		let stored = { ...smtpSettings, };
		await withCliServer((req, res,) => {
			if (req.url?.endsWith("/settings",)) sendJson(res, stored,);
			else sendJson(res, { id: "smtp-main", type: "smtp", webhookUrl: WEBHOOK, },);
		}, async (url,) => {
			const env = cliEnv(url,);
			const got = (await dss(["messaging-channel", "get", "smtp-main",], { env, },)).stdout;
			noSecretsIn(got,);
			const settingsOut = await settingsGet(url,);
			noSecretsIn(settingsOut,);
			const first = JSON.parse(settingsOut,);
			expect(first.settings.password,).toBe("[redacted]",);
			expect(first.settings.sessionProperties.map((property: { value: string; },) => property.value),)
				.toEqual(["[redacted]", "true",],);
			stored = { ...stored, password: "another-password", };
			expect(JSON.parse(await settingsGet(url,),).settingsHash,).toBe(first.settingsHash,);
			stored = { ...stored, host: "smtp.other.example", };
			expect(JSON.parse(await settingsGet(url,),).settingsHash,).not.toBe(first.settingsHash,);
		},);
	});

	it("reads permissions untouched", async () => {
		const permissions = [{ group: "analysts", canUse: true, }, { user: "jdoe", canUse: false, },];
		await withCliServer((_req, res,) => sendJson(res, permissions,), async (url,) => {
			const out = await dss(["messaging-channel", "permissions-get", "smtp-main",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(out.stdout,),).toEqual(permissions,);
		},);
	});
});

describe("messaging-channel settings-set", () => {
	it("merges the patch into the stored settings and PUTs the whole object, keeping credentials", async () => {
		const seen: Seen[] = [];
		await withCliServer(
			mockDss(seen, (req, res,) => {
				if (req.method === "GET") sendJson(res, smtpSettings,);
				else res.end();
			},),
			async (url,) => {
				const out = await dss([
					"messaging-channel",
					"settings-set",
					"smtp-main",
					"--data",
					'{"sender":"ops@example.com"}',
				], { env: cliEnv(url,), },);
				noSecretsIn(out.stdout,);
				expect(JSON.parse(out.stdout,),).toMatchObject({
					updated: "smtp-main",
					changed: ["sender",],
				},);
			},
		);
		expect(seen.map((request,) => `${request.method} ${request.path}`),).toEqual([
			"GET /public/api/messaging-channels/smtp-main/settings",
			"PUT /public/api/messaging-channels/smtp-main/settings",
		],);
		expect(JSON.parse(seen[1]!.body,),).toEqual({ ...smtpSettings, sender: "ops@example.com", },);
	});

	it("refuses a stale --expect-hash before writing and accepts the current one", async () => {
		const seen: Seen[] = [];
		await withCliServer(
			mockDss(seen, (req, res,) => {
				if (req.method === "GET") sendJson(res, smtpSettings,);
				else res.end();
			},),
			async (url,) => {
				const env = cliEnv(url,);
				const hash = JSON.parse(await settingsGet(url,),).settingsHash;
				seen.length = 0;
				const stale = await dssFailure([
					"messaging-channel",
					"settings-set",
					"smtp-main",
					"--data",
					'{"sender":"ops@example.com"}',
					"--expect-hash",
					"0".repeat(64,),
				], { env, },);
				expect(stale.code,).toBe(1,);
				expect(JSON.parse(stale.stdout || stale.stderr,),).toMatchObject({
					code: "validation_failed",
					details: { expectedHash: "0".repeat(64,), actualHash: hash, },
				},);
				expect(seen.map((request,) => request.method),).toEqual(["GET",],);
				await dss([
					"messaging-channel",
					"settings-set",
					"smtp-main",
					"--data",
					'{"sender":"ops@example.com"}',
					"--expect-hash",
					hash,
				], { env, },);
				expect(seen.map((request,) => request.method),).toEqual(["GET", "GET", "PUT",],);
			},
		);
	});

	it("rejects the [redacted] placeholder before any request", async () => {
		const seen: Seen[] = [];
		await withCliServer(mockDss(seen, (_req, res,) => sendJson(res, {},),), async (url,) => {
			const failure = await dssFailure([
				"messaging-channel",
				"settings-set",
				"smtp-main",
				"--data",
				'{"password":"[redacted]"}',
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(1,);
			expect(failure.stdout + failure.stderr,).toContain("placeholder",);
		},);
		expect(seen,).toEqual([],);
	});

	it("dry-run previews redacted current and next settings and never writes", async () => {
		const seen: Seen[] = [];
		await withCliServer(
			mockDss(seen, (_req, res,) => sendJson(res, smtpSettings,),),
			async (url,) => {
				const out = await dss([
					"messaging-channel",
					"settings-set",
					"smtp-main",
					"--data",
					'{"sender":"ops@example.com","password":"brand-new-password"}',
					"--dry-run",
				], { env: cliEnv(url,), },);
				noSecretsIn(out.stdout,);
				expect(out.stdout,).not.toContain("brand-new-password",);
				expect(JSON.parse(out.stdout,),).toMatchObject({
					dryRun: true,
					changed: ["password", "sender",],
					next: { sender: "ops@example.com", password: "[redacted]", },
				},);
			},
		);
		expect(seen.map((request,) => request.method),).toEqual(["GET",],);
	});

	it("plans offline without contacting DSS or printing credentials", async () => {
		const seen: Seen[] = [];
		await withCliServer(mockDss(seen, (_req, res,) => sendJson(res, {},),), async (url,) => {
			const out = await dss([
				"messaging-channel",
				"settings-set",
				"smtp-main",
				"--data",
				`{"password":"${PASSWORD}"}`,
				"--plan",
			], { env: cliEnv(url,), },);
			noSecretsIn(out.stdout,);
			expect(JSON.parse(out.stdout,),).toMatchObject({
				plan: true,
				exact: false,
				endpoint: "/public/api/messaging-channels/smtp-main/settings",
			},);
		},);
		expect(seen,).toEqual([],);
	});

	it("scrubs credentials from a DSS error that echoes the saved settings", async () => {
		await withCliServer(async (req, res,) => {
			if (req.method === "GET") {
				sendJson(res, smtpSettings,);
				return;
			}
			sendJson(res, { message: `Invalid settings: ${await readBody(req,)}`, }, 400,);
		}, async (url,) => {
			const failure = await dssFailure([
				"messaging-channel",
				"settings-set",
				"smtp-main",
				"--data",
				`{"webhookUrl":"${WEBHOOK}"}`,
			], { env: cliEnv(url,), },);
			expect(failure.code,).not.toBe(0,);
			noSecretsIn(failure.stdout + failure.stderr,);
		},);
	});
});

describe("messaging-channel create, permissions-set, delete", () => {
	const shellConfiguration = { type: "COMMAND", command: "echo hi", };

	it("creates with the Python-client body shape and redacts the echoed channel", async () => {
		const seen: Seen[] = [];
		await withCliServer(
			mockDss(
				seen,
				(_req, res,) => sendJson(res, { id: "ops-shell", type: "shell", webhookUrl: WEBHOOK, },),
			),
			async (url,) => {
				const out = await dss([
					"messaging-channel",
					"create",
					"--type",
					"shell",
					"--id",
					"ops-shell",
					"--data",
					JSON.stringify(shellConfiguration,),
					"--permissions",
					'[{"group":"analysts","canUse":true}]',
				], { env: cliEnv(url,), },);
				noSecretsIn(out.stdout,);
				expect(JSON.parse(out.stdout,),).toMatchObject({ id: "ops-shell", type: "shell", },);
			},
		);
		expect(seen.map((request,) => `${request.method} ${request.path}`),).toEqual([
			"POST /public/api/messaging-channels/",
		],);
		expect(JSON.parse(seen[0]!.body,),).toEqual({
			id: "ops-shell",
			type: "shell",
			configuration: shellConfiguration,
			permissions: [{ group: "analysts", canUse: true, },],
		},);
	});

	it("sends an empty id and null permissions when omitted", async () => {
		const seen: Seen[] = [];
		await withCliServer(
			mockDss(seen, (_req, res,) => sendJson(res, { id: "gen", type: "shell", },),),
			async (url,) => {
				await dss(["messaging-channel", "create", "--type", "shell",], { env: cliEnv(url,), },);
			},
		);
		expect(JSON.parse(seen[0]!.body,),).toEqual({
			id: "",
			type: "shell",
			configuration: {},
			permissions: null,
		},);
	});

	it("plan equals the request create sends", async () => {
		const seen: Seen[] = [];
		await withCliServer(
			mockDss(seen, (_req, res,) => sendJson(res, { id: "ops-shell", },),),
			async (url,) => {
				const env = cliEnv(url,);
				const argv = [
					"messaging-channel",
					"create",
					"--type",
					"shell",
					"--id",
					"ops-shell",
					"--data",
					JSON.stringify(shellConfiguration,),
				];
				const plan = JSON.parse((await dss([...argv, "--plan",], { env, },)).stdout,);
				expect(seen,).toEqual([],);
				await dss(argv, { env, },);
				expect(seen[0]!.path,).toBe(plan.endpoint,);
				expect(JSON.parse(seen[0]!.body,),).toEqual(plan.payload,);
			},
		);
	});

	it("plans and dry-runs creation with credentials redacted and no request", async () => {
		const seen: Seen[] = [];
		await withCliServer(mockDss(seen, (_req, res,) => sendJson(res, {},),), async (url,) => {
			const env = cliEnv(url,);
			for (const mode of ["--plan", "--dry-run",]) {
				const out = await dss([
					"messaging-channel",
					"create",
					"--type",
					"slack",
					"--data",
					JSON.stringify({ webhookUrl: WEBHOOK, useProxy: false, },),
					mode,
				], { env, },);
				noSecretsIn(out.stdout,);
				expect(JSON.parse(out.stdout,),).toMatchObject({
					method: "POST",
					payload: { type: "slack", configuration: { webhookUrl: "[redacted]", useProxy: false, }, },
				},);
			}
		},);
		expect(seen,).toEqual([],);
	});

	it("scrubs credentials from a DSS error that echoes the creation body", async () => {
		await withCliServer(async (req, res,) => {
			sendJson(res, { message: `Cannot create channel from ${await readBody(req,)}`, }, 400,);
		}, async (url,) => {
			const failure = await dssFailure([
				"messaging-channel",
				"create",
				"--type",
				"slack",
				"--data",
				JSON.stringify({ webhookUrl: WEBHOOK, },),
			], { env: cliEnv(url,), },);
			expect(failure.code,).not.toBe(0,);
			noSecretsIn(failure.stdout + failure.stderr,);
		},);
	});

	it("requires --type and refuses the [redacted] placeholder in a configuration", async () => {
		const missing = await dssFailure(["messaging-channel", "create", "--plan",], {
			env: cliEnv("http://127.0.0.1:1",),
		},);
		expect(missing.code,).toBe(1,);
		const placeholder = await dssFailure([
			"messaging-channel",
			"create",
			"--type",
			"smtp",
			"--data",
			'{"password":"[redacted]"}',
			"--plan",
		], { env: cliEnv("http://127.0.0.1:1",), },);
		expect(placeholder.code,).toBe(1,);
	});

	it("PUTs the permission list and validates its shape offline", async () => {
		const seen: Seen[] = [];
		await withCliServer(mockDss(seen, (_req, res,) => res.end(),), async (url,) => {
			const env = cliEnv(url,);
			const permissions = [{ group: "analysts", canUse: true, }, { user: "jdoe", canUse: false, },];
			await dss([
				"messaging-channel",
				"permissions-set",
				"smtp-main",
				"--data",
				JSON.stringify(permissions,),
			], { env, },);
			expect(seen.map((request,) => `${request.method} ${request.path}`),).toEqual([
				"PUT /public/api/messaging-channels/smtp-main/permissions",
			],);
			expect(JSON.parse(seen[0]!.body,),).toEqual(permissions,);
			seen.length = 0;
			const failure = await dssFailure([
				"messaging-channel",
				"permissions-set",
				"smtp-main",
				"--data",
				'{"group":"analysts"}',
			], { env, },);
			expect(failure.code,).toBe(1,);
			expect(seen,).toEqual([],);
		},);
	});

	it("delete --if-exists skips a missing channel, dry-run previews, and a live delete sends DELETE", async () => {
		const seen: Seen[] = [];
		let exists = false;
		await withCliServer(
			mockDss(seen, (req, res,) => {
				if (req.method === "GET") {
					if (exists) sendJson(res, { id: "ops-shell", type: "shell", },);
					else sendJson(res, { message: "not found", }, 404,);
					return;
				}
				res.end();
			},),
			async (url,) => {
				const env = cliEnv(url,);
				const skipped = JSON.parse(
					(await dss(["messaging-channel", "delete", "ops-shell", "--if-exists",], { env, },)).stdout,
				);
				expect(skipped,).toMatchObject({ skipped: "ops-shell", reason: "missing", },);
				expect(seen.map((request,) => request.method),).toEqual(["GET",],);
				exists = true;
				seen.length = 0;
				const preview = JSON.parse(
					(await dss(["messaging-channel", "delete", "ops-shell", "--dry-run",], { env, },)).stdout,
				);
				expect(preview,).toMatchObject({
					plannedAndDryRun: true,
					method: "DELETE",
					endpoint: "/public/api/messaging-channels/ops-shell",
				},);
				expect(seen.map((request,) => request.method),).toEqual(["GET",],);
				seen.length = 0;
				const deleted = JSON.parse(
					(await dss(["messaging-channel", "delete", "ops-shell",], { env, },)).stdout,
				);
				expect(deleted,).toEqual({ deleted: "ops-shell", resource: "messaging-channel", },);
				expect(seen.map((request,) => `${request.method} ${request.path}`),).toEqual([
					"DELETE /public/api/messaging-channels/ops-shell",
				],);
			},
		);
	});

	it("records a cleanup ledger entry that deletes the created channel", async () => {
		const dir = mkdtempSync(join(tmpdir(), "dss-channel-ledger-",),);
		try {
			await withCliServer(
				(_req, res,) => sendJson(res, { id: "ops-shell", type: "shell", },),
				async (url,) => {
					const ledger = join(dir, "cleanup.jsonl",);
					await dss([
						"messaging-channel",
						"create",
						"--type",
						"shell",
						"--record-cleanup",
						ledger,
					], { env: cliEnv(url,), },);
					const entry = JSON.parse((await Bun.file(ledger,).text()).trim().split("\n",)[0]!,);
					expect(entry,).toMatchObject({
						cleanup: { argv: ["messaging-channel", "delete", "ops-shell", "--if-exists",], },
					},);
				},
			);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});
});

describe("messaging-channel send", () => {
	const sendArgs = [
		"messaging-channel",
		"send",
		"smtp-main",
		"--to",
		"a@example.com, b@example.com",
		"--cc",
		"c@example.com",
		"--subject",
		"Nightly build",
		"--body",
		"<b>done</b>",
	];

	it("posts a multipart message part plus attachment parts with the project and single-message query", async () => {
		const dir = mkdtempSync(join(tmpdir(), "dss-channel-send-",),);
		try {
			const attachment = join(dir, "report.csv",);
			writeFileSync(attachment, "a,b\n1,2\n",);
			const seen: Seen[] = [];
			let contentType = "";
			await withCliServer(
				mockDss(seen, (req, res,) => {
					contentType = req.headers["content-type"] ?? "";
					res.end();
				},),
				async (url,) => {
					const out = await dss([
						...sendArgs,
						"--attachment",
						attachment,
						"--single-message",
						"--plain-text",
						"--from",
						"me@example.com",
					], { env: cliEnv(url,), },);
					expect(JSON.parse(out.stdout,),).toMatchObject({
						sent: "smtp-main",
						projectKey: "TEST",
						to: ["a@example.com", "b@example.com",],
						attachments: ["report.csv",],
						sendAsSingleMessage: true,
					},);
				},
			);
			expect(seen.map((request,) => `${request.method} ${request.path}`),).toEqual([
				"POST /public/api/messaging-channels/smtp-main/actions/send?projectKey=TEST&sendAsSingleMessage=true",
			],);
			expect(contentType,).toStartWith("multipart/form-data; boundary=",);
			const body = seen[0]!.body;
			expect(body,).toMatch(
				/name="message"; filename="send-payload\.json"\r\nContent-Type: application\/json/,
			);
			expect(body,).toMatch(/name="attachments"; filename="report\.csv"/,);
			expect(body,).toContain("a,b\n1,2\n",);
			const payload = JSON.parse(/\{"from".*?\}(?=\r\n--)/s.exec(body,)![0],);
			expect(payload,).toEqual({
				from: "me@example.com",
				to: ["a@example.com", "b@example.com",],
				cc: ["c@example.com",],
				bcc: null,
				subject: "Nightly build",
				body: "<b>done</b>",
				plainText: true,
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("reads the body from a file and sends one message per recipient by default", async () => {
		const dir = mkdtempSync(join(tmpdir(), "dss-channel-body-",),);
		try {
			const bodyFile = join(dir, "body.html",);
			writeFileSync(bodyFile, "<p>from file</p>",);
			const seen: Seen[] = [];
			await withCliServer(mockDss(seen, (_req, res,) => res.end(),), async (url,) => {
				await dss([
					"messaging-channel",
					"send",
					"smtp-main",
					"--to",
					"a@example.com",
					"--subject",
					"s",
					"--body-file",
					bodyFile,
					"--project-key",
					"OTHER",
				], { env: cliEnv(url,), },);
			},);
			expect(seen[0]!.path,).toBe(
				"/public/api/messaging-channels/smtp-main/actions/send?projectKey=OTHER&sendAsSingleMessage=false",
			);
			expect(seen[0]!.body,).toContain("<p>from file</p>",);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("dry-run and plan describe the multipart request without sending anything", async () => {
		const dir = mkdtempSync(join(tmpdir(), "dss-channel-plan-",),);
		try {
			const attachment = join(dir, "report.csv",);
			writeFileSync(attachment, "x",);
			const seen: Seen[] = [];
			await withCliServer(mockDss(seen, (_req, res,) => res.end(),), async (url,) => {
				const env = cliEnv(url,);
				for (const mode of ["--dry-run", "--plan",]) {
					const out = JSON.parse(
						(await dss([...sendArgs, "--attachment", attachment, mode,], { env, },)).stdout,
					);
					expect(out,).toMatchObject({
						plan: true,
						method: "POST",
						endpoint:
							"/public/api/messaging-channels/smtp-main/actions/send?projectKey=TEST&sendAsSingleMessage=false",
						payload: {
							upload: "multipart",
							message: { to: ["a@example.com", "b@example.com",], cc: ["c@example.com",], },
							attachments: [{ path: attachment, fileName: "report.csv", },],
						},
					},);
				}
			},);
			expect(seen,).toEqual([],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("fails before sending when the attachment is unreadable or required input is missing", async () => {
		const seen: Seen[] = [];
		await withCliServer(mockDss(seen, (_req, res,) => res.end(),), async (url,) => {
			const env = cliEnv(url,);
			const missingAttachment = await dssFailure([
				...sendArgs,
				"--attachment",
				"/nonexistent/report.csv",
			], { env, },);
			expect(missingAttachment.code,).not.toBe(0,);
			expect(missingAttachment.stdout + missingAttachment.stderr,).toContain(
				"Attachment could not be read",
			);
			const dryRunMissing = await dssFailure([
				...sendArgs,
				"--attachment",
				"/nonexistent/report.csv",
				"--dry-run",
			], { env, },);
			expect(dryRunMissing.code,).toBe(1,);
			const noRecipient = await dssFailure([
				"messaging-channel",
				"send",
				"smtp-main",
				"--subject",
				"s",
				"--body",
				"b",
			], { env, },);
			expect(noRecipient.code,).toBe(1,);
			const noBody = await dssFailure([
				"messaging-channel",
				"send",
				"smtp-main",
				"--to",
				"a@example.com",
				"--subject",
				"s",
			], { env, },);
			expect(noBody.code,).toBe(1,);
		},);
		expect(seen,).toEqual([],);
	});
});

describe("messaging-channel contract", () => {
	const registry = buildCommandRegistry("messaging-channel",)["messaging-channel"]!;

	it("classifies send as an irreversible external write and the reads as reads", () => {
		const send = registry["send"]!;
		expect(send.sideEffect,).toBe("write",);
		expect(send.destructive,).toBe("destructive",);
		expect(send.dryRun,).toBe(true,);
		expect(send.requiresProject,).toBe(true,);
		for (const action of ["list", "get", "settings-get", "permissions-get",]) {
			expect(registry[action]!.sideEffect, action,).toBe("read",);
		}
		for (const action of ["create", "settings-set", "permissions-set", "delete", "send",]) {
			expect(registry[action]!.sideEffect, action,).toBe("write",);
			expect(registry[action]!.dryRun, action,).toBe(true,);
		}
		expect(registry["permissions-get"]!.outputShape,).toBe("array",);
		expect(registry["delete"]!.idempotency,).toBe("if-exists",);
	});
});
