import { describe, expect, it, } from "bun:test";
import { createHash, } from "node:crypto";
import * as fs from "node:fs";
import {
	type AppPermissionsSnapshot,
	buildAppPermissionsSnapshot,
	writeAppPermissionsSnapshot,
} from "../../src/cli/helpers/app-permissions.js";
import { projectIncarnationHash, } from "../../src/utils/project-incarnation.js";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	mkdirSync,
	readBody,
	readFileSync,
	rmSync,
	sendJson,
	statSync,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

const LIVE_PERMISSIONS = {
	owner: "alice",
	projectPermissions: [
		{ principal: "u1", type: "USER", mode: "WRITE", },
		{ principal: "g1", type: "GROUP", mode: "READ", },
	],
};

const CHANGED_PERMISSIONS = {
	owner: "alice",
	projectPermissions: [
		{ principal: "u1", type: "USER", mode: "READ_ONLY", },
		{ principal: "g3", type: "GROUP", mode: "READ", },
	],
	fresh: "y",
};

const PARTIALLY_APPLIED_PERMISSIONS = {
	owner: "alice",
	projectPermissions: [
		{ principal: "u1", type: "USER", mode: "PARTIAL", },
		{ principal: "g1", type: "GROUP", mode: "READ", },
	],
};

const PERMISSIONS_PATH = "/public/api/projects/TEST/permissions";
const PROJECT_DETAILS_PATH = "/public/api/projects/TEST/";
const PROJECT_CREATION_TAG = { versionNumber: 1, lastModifiedOn: 1_700_000_000_000, };
const REPLACEMENT_PROJECT_CREATION_TAG = {
	versionNumber: 1,
	lastModifiedOn: 1_800_000_000_000,
};

/** Independent key-sorted canonical JSON, mirroring the snapshot hash contract. */
function canonicalJson(value: unknown,): string {
	if (value === null || typeof value !== "object") return JSON.stringify(value,);
	if (Array.isArray(value,)) {
		return `[${value.map((item,) => canonicalJson(item,)).join(",",)}]`;
	}
	const entries = Object.entries(value as Record<string, unknown>,)
		.sort(([a,], [b,],) => a < b ? -1 : a > b ? 1 : 0);
	return `{${
		entries.map(([key, item,],) => `${JSON.stringify(key,)}:${canonicalJson(item,)}`).join(",",)
	}}`;
}

function sha256Hex(value: string,): string {
	return createHash("sha256",).update(value,).digest("hex",);
}

function permissionsHash(permissions: unknown,): string {
	return sha256Hex(canonicalJson(permissions,),);
}

const PROJECT_INCARNATION_HASH = sha256Hex(canonicalJson({
	projectKey: "TEST",
	creationTag: PROJECT_CREATION_TAG,
},),);

function answerProjectDetails(
	req: { method?: string; url?: string; },
	res: Parameters<typeof sendJson>[0],
	creationTag: Record<string, unknown> = PROJECT_CREATION_TAG,
): boolean {
	if (req.method !== "GET" || req.url !== PROJECT_DETAILS_PATH) return false;
	sendJson(res, {
		projectKey: "TEST",
		name: "Test",
		projectAppType: "APP_INSTANCE",
		creationTag,
	},);
	return true;
}

function snapshotHash(snapshot: {
	version: number;
	projectKey: string;
	dssUrl: string;
	projectIncarnationHash: string;
	capturedAt: string;
	permissionsHash: string;
	permissions: unknown;
},): string {
	return sha256Hex(canonicalJson(snapshot,),);
}

function makeDir(): string {
	return mkdirSync(join(tmpdir(), "dss-perm-", Math.random().toString(36,).slice(2,),), {
		recursive: true,
	},);
}

it("keeps project incarnation stable when only app type changes", () => {
	const base = {
		projectKey: "TEST",
		creationTag: PROJECT_CREATION_TAG,
	};
	expect(projectIncarnationHash("TEST", { ...base, projectAppType: "APP_INSTANCE", },),).toBe(
		projectIncarnationHash("TEST", { ...base, projectAppType: "APP_TEMPLATE", },),
	);
});

describe("app permissions snapshot", () => {
	it("writes an owner-only JSON file bound to the DSS server with version, key, and hashes", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				const { stdout, stderr, } = await dss(
					["app", "permissions-snapshot", "--output", out,],
					{ env: cliEnv(url,), },
				);
				expect(stderr,).toBe("",);
				const result = JSON.parse(stdout,) as {
					output: string;
					projectKey: string;
					projectIncarnationHash: string;
					dssUrl: string;
					hash: string;
					permissionsHash: string;
					capturedAt: string;
				};
				expect(result.output,).toBe(out,);
				expect(result.projectKey,).toBe("TEST",);
				expect(result.dssUrl,).toBe(url,);
				expect(result.hash,).toMatch(/^[0-9a-f]{64}$/,);

				const saved = JSON.parse(readFileSync(out, "utf-8",),) as {
					version: number;
					projectKey: string;
					projectIncarnationHash: string;
					dssUrl: string;
					capturedAt: string;
					hash: string;
					permissionsHash: string;
					permissions: unknown;
				};
				expect(saved.version,).toBe(3,);
				expect(saved.projectKey,).toBe("TEST",);
				expect(saved.projectIncarnationHash,).toBe(PROJECT_INCARNATION_HASH,);
				expect(saved.dssUrl,).toBe(url,);
				expect(saved.capturedAt,).toBe(result.capturedAt,);
				expect(saved.hash,).toBe(result.hash,);
				expect(saved.permissions,).toEqual(LIVE_PERMISSIONS,);
				expect(saved.permissionsHash,).toBe(permissionsHash(LIVE_PERMISSIONS,),);
				expect(result.permissionsHash,).toBe(saved.permissionsHash,);
				expect(result.hash,).toBe(snapshotHash({
					version: saved.version,
					projectKey: saved.projectKey,
					projectIncarnationHash: saved.projectIncarnationHash,
					dssUrl: saved.dssUrl,
					capturedAt: saved.capturedAt,
					permissionsHash: saved.permissionsHash,
					permissions: saved.permissions,
				},),);
				if (process.platform !== "win32") {
					expect(statSync(out,).mode & 0o777,).toBe(0o600,);
				}
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("rejects capture when the project key is replaced after reading permissions", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		let detailReads = 0;
		let permissionReads = 0;
		try {
			await withCliServer((req, res,) => {
				if (req.method === "GET" && req.url === PROJECT_DETAILS_PATH) {
					detailReads += 1;
					answerProjectDetails(
						req,
						res,
						detailReads === 1
							? PROJECT_CREATION_TAG
							: REPLACEMENT_PROJECT_CREATION_TAG,
					);
					return;
				}
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					permissionReads += 1;
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				const failure = await dssFailure(
					["app", "permissions-snapshot", "--output", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("not the same project incarnation",);
				expect(fs.existsSync(out,),).toBe(false,);
			},);
			expect(detailReads,).toBe(2,);
			expect(permissionReads,).toBe(1,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("canonicalizes the bound server URL: trailing slashes and URL userinfo never reach the snapshot", () => {
		const snapshot = buildAppPermissionsSnapshot(
			"TEST",
			"http://secret:token@127.0.0.1:8123/",
			PROJECT_INCARNATION_HASH,
			LIVE_PERMISSIONS,
			"2026-08-13T00:00:00.000Z",
		);
		expect(snapshot.dssUrl,).toBe("http://127.0.0.1:8123",);
		const text = JSON.stringify(snapshot,);
		expect(text,).not.toContain("secret",);
		expect(text,).not.toContain("token",);
	});
});

describe("app permissions snapshot writer safety", () => {
	const canonical = buildAppPermissionsSnapshot(
		"TEST",
		"http://127.0.0.1:8123/",
		PROJECT_INCARNATION_HASH,
		{
			owner: "alice",
			projectPermissions: [
				{ principal: "u1", type: "USER", mode: "WRITE", },
				{ principal: "g1", type: "GROUP", mode: "READ", },
			],
		},
		"2026-08-13T00:00:00.000Z",
	);
	const canonicalText = `${JSON.stringify(canonical, null, 2,)}\n`;

	it("clamps a pre-existing 0644 file to 0600 before writing the snapshot", () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			fs.writeFileSync(out, "stale content", { mode: 0o644, },);
			fs.chmodSync(out, 0o644,);
			writeAppPermissionsSnapshot(out, canonical,);
			if (process.platform !== "win32") {
				expect(fs.statSync(out,).mode & 0o777,).toBe(0o600,);
			}
			expect(fs.readFileSync(out, "utf-8",),).toBe(canonicalText,);
		} finally {
			fs.rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("overwrites a longer existing file without leaving trailing bytes", () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			fs.writeFileSync(out, "x".repeat(4096,), { mode: 0o600, },);
			writeAppPermissionsSnapshot(out, canonical,);
			expect(fs.readFileSync(out, "utf-8",),).toBe(canonicalText,);
		} finally {
			fs.rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("writes the existing pretty-printed JSON format byte-for-byte on a fresh file", () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			writeAppPermissionsSnapshot(out, canonical,);
			expect(fs.readFileSync(out, "utf-8",),).toBe(canonicalText,);
			if (process.platform !== "win32") {
				expect(fs.statSync(out,).mode & 0o777,).toBe(0o600,);
			}
		} finally {
			fs.rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("refuses to write through a symlink and leaves the target untouched where O_NOFOLLOW is supported", () => {
		if (typeof fs.constants.O_NOFOLLOW !== "number") return;
		const dir = makeDir();
		const target = join(dir, "target.json",);
		const link = join(dir, "link.json",);
		const marker = "SENTINEL-TARGET-CONTENT";
		try {
			fs.writeFileSync(target, marker, { mode: 0o644, },);
			fs.chmodSync(target, 0o644,);
			try {
				fs.symlinkSync(target, link,);
			} catch {
				return; // symlinks unavailable in this environment
			}
			let threw: unknown;
			try {
				writeAppPermissionsSnapshot(link, canonical,);
			} catch (error) {
				threw = error;
			}
			expect(threw instanceof Error,).toBe(true,);
			expect(String((threw as Error).message,),).not.toContain(marker,);
			// The symlinked victim must be untouched: content and mode alike.
			expect(fs.readFileSync(target, "utf-8",),).toBe(marker,);
			expect(fs.statSync(target,).mode & 0o777,).toBe(0o644,);
			expect(fs.lstatSync(link,).isSymbolicLink(),).toBe(true,);
		} finally {
			fs.rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("does not modify the destination or leak payload values when serialization fails", () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		const marker = "SENSITIVE-PAYLOAD-MARKER";
		try {
			fs.writeFileSync(out, "previous-content", { mode: 0o644, },);
			fs.chmodSync(out, 0o644,);
			const malicious = {
				...canonical,
				projectKey: marker,
				permissions: { marker: 10n, },
			} as unknown as AppPermissionsSnapshot;
			let threw: unknown;
			try {
				writeAppPermissionsSnapshot(out, malicious,);
			} catch (error) {
				threw = error;
			}
			expect(threw instanceof Error,).toBe(true,);
			expect(String((threw as Error).message,),).not.toContain(marker,);
			// Serialization fails before the file is touched at all.
			expect(fs.readFileSync(out, "utf-8",),).toBe("previous-content",);
			if (process.platform !== "win32") {
				expect(fs.statSync(out,).mode & 0o777,).toBe(0o644,);
			}
		} finally {
			fs.rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("reports the failing path without snapshot content on open errors", () => {
		const dir = makeDir();
		const missing = join(dir, "missing-dir", "permissions.json",);
		try {
			let threw: unknown;
			try {
				writeAppPermissionsSnapshot(missing, canonical,);
			} catch (error) {
				threw = error;
			}
			expect(threw instanceof Error,).toBe(true,);
			const message = String((threw as Error).message,);
			expect(message,).toContain(missing,);
			expect(message,).not.toContain("alice",);
			expect(message,).not.toContain("u1",);
		} finally {
			fs.rmSync(dir, { recursive: true, force: true, },);
		}
	});
});

describe("app permissions diff", () => {
	it("reports unchanged when live permissions match the snapshot on the bound server", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
				const { stdout, stderr, } = await dss(
					["app", "permissions-diff", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(stderr,).toBe("",);
				const result = JSON.parse(stdout,) as {
					projectKey: string;
					backupHash: string;
					currentHash: string;
					changed: boolean;
					differences: unknown[];
				};
				expect(result.projectKey,).toBe("TEST",);
				expect(result.changed,).toBe(false,);
				expect(result.differences,).toEqual([],);
				expect(result.backupHash,).toBe(result.currentHash,);
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("returns deterministic sorted added/removed/changed differences on the bound server", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			let live: unknown = LIVE_PERMISSIONS;
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					sendJson(res, live,);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
				live = CHANGED_PERMISSIONS;
				const { stdout, stderr, } = await dss(
					["app", "permissions-diff", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(stderr,).toBe("",);
				const result = JSON.parse(stdout,) as {
					backupHash: string;
					currentHash: string;
					changed: boolean;
					differences: Array<{
						path: string;
						status: string;
						backup?: unknown;
						current?: unknown;
					}>;
				};
				expect(result.changed,).toBe(true,);
				expect(result.backupHash,).toBe(permissionsHash(LIVE_PERMISSIONS,),);
				expect(result.currentHash,).toBe(permissionsHash(CHANGED_PERMISSIONS,),);
				expect(result.differences,).toEqual([
					{ path: "fresh", status: "added", current: "y", },
					{
						path: "projectPermissions[0].mode",
						status: "changed",
						backup: "WRITE",
						current: "READ_ONLY",
					},
					{
						path: "projectPermissions[1].principal",
						status: "changed",
						backup: "g1",
						current: "g3",
					},
				],);
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("rejects a corrupted snapshot before any server call", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			let gets = 0;
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					gets += 1;
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
				expect(gets,).toBe(1,); // the capture fetch
				gets = 0;
				const saved = JSON.parse(readFileSync(out, "utf-8",),) as {
					permissions: { projectPermissions: Array<{ mode: string; }>; };
				};
				saved.permissions.projectPermissions[0]!.mode = "EVIL";
				writeFileSync(out, `${JSON.stringify(saved, null, 2,)}\n`,);
				const failure = await dssFailure(
					["app", "permissions-diff", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("failed its integrity check",);
				expect(failure.stderr,).toContain("corrupted",);
			},);
			expect(gets,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("rejects snapshot project-key tampering before any server call", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			let gets = 0;
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					gets += 1;
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
				expect(gets,).toBe(1,); // the capture fetch
				gets = 0;
				const saved = JSON.parse(readFileSync(out, "utf-8",),) as { projectKey: string; };
				saved.projectKey = "OTHER";
				writeFileSync(out, `${JSON.stringify(saved, null, 2,)}\n`,);
				const failure = await dssFailure(
					["app", "permissions-diff", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("failed its integrity check",);
			},);
			expect(gets,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("rejects snapshot dssUrl tampering before any server call", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			let gets = 0;
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					gets += 1;
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
				expect(gets,).toBe(1,); // the capture fetch
				gets = 0;
				const saved = JSON.parse(readFileSync(out, "utf-8",),) as { dssUrl: string; };
				saved.dssUrl = "http://127.0.0.1:1";
				writeFileSync(out, `${JSON.stringify(saved, null, 2,)}\n`,);
				const failure = await dssFailure(
					["app", "permissions-diff", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("failed its integrity check",);
			},);
			expect(gets,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("rejects a snapshot from a different DSS server before any server call", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
			},);

			let requests = 0;
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				requests += 1;
				sendJson(res, LIVE_PERMISSIONS,);
			}, async (url,) => {
				const failure = await dssFailure(
					["app", "permissions-diff", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("across DSS servers",);
			},);
			expect(requests,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("refuses version-2 snapshots without project-incarnation binding", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			// Version 2 bound the server URL but not the concrete project incarnation.
			const legacy = {
				version: 2,
				projectKey: "TEST",
				dssUrl: "http://127.0.0.1:8123",
				capturedAt: "2026-08-13T00:00:00.000Z",
				permissionsHash: permissionsHash(LIVE_PERMISSIONS,),
				permissions: LIVE_PERMISSIONS,
			};
			writeFileSync(
				out,
				`${JSON.stringify({ ...legacy, hash: sha256Hex(canonicalJson(legacy,),), }, null, 2,)}\n`,
			);

			let requests = 0;
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				requests += 1;
				sendJson(res, LIVE_PERMISSIONS,);
			}, async (url,) => {
				const failure = await dssFailure(
					["app", "permissions-diff", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("unsupported snapshot version 2",);
				expect(failure.stderr,).toContain("not bound",);
			},);
			expect(requests,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("rejects diff against a replacement project with the same key", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		let permissionRequests = 0;
		try {
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res, REPLACEMENT_PROJECT_CREATION_TAG,)) return;
				permissionRequests += 1;
				sendJson(res, LIVE_PERMISSIONS,);
			}, async (url,) => {
				writeAppPermissionsSnapshot(
					out,
					buildAppPermissionsSnapshot(
						"TEST",
						url,
						PROJECT_INCARNATION_HASH,
						LIVE_PERMISSIONS,
					),
				);
				const failure = await dssFailure(
					["app", "permissions-diff", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("different incarnation of project TEST",);
			},);
			expect(permissionRequests,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("rejects diff when the project key is replaced after reading permissions", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		let detailReads = 0;
		let permissionReads = 0;
		try {
			await withCliServer((req, res,) => {
				if (req.method === "GET" && req.url === PROJECT_DETAILS_PATH) {
					detailReads += 1;
					answerProjectDetails(
						req,
						res,
						detailReads === 1
							? PROJECT_CREATION_TAG
							: REPLACEMENT_PROJECT_CREATION_TAG,
					);
					return;
				}
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					permissionReads += 1;
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				writeAppPermissionsSnapshot(
					out,
					buildAppPermissionsSnapshot(
						"TEST",
						url,
						PROJECT_INCARNATION_HASH,
						LIVE_PERMISSIONS,
					),
				);
				const failure = await dssFailure(
					["app", "permissions-diff", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("not the same project incarnation",);
			},);
			expect(detailReads,).toBe(2,);
			expect(permissionReads,).toBe(1,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});
});

describe("app permissions restore", () => {
	it("refuses to restore a snapshot captured from another project", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			let calls = 0;
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				calls += 1;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
				expect(calls,).toBe(1,); // the capture fetch
				calls = 0;
				const failure = await dssFailure(
					["app", "permissions-restore", "--file", out, "--project-key", "OTHER",],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("Refusing to restore permissions across projects",);
			},);
			expect(calls,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("refuses to restore a snapshot on a different DSS server before any server call", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
			},);

			let requests = 0;
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				requests += 1;
				sendJson(res, LIVE_PERMISSIONS,);
			}, async (url,) => {
				const failure = await dssFailure(
					["app", "permissions-restore", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("across DSS servers",);
			},);
			expect(requests,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("rejects restore into a replacement project before reading or writing permissions", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		let permissionRequests = 0;
		try {
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res, REPLACEMENT_PROJECT_CREATION_TAG,)) return;
				permissionRequests += 1;
				sendJson(res, LIVE_PERMISSIONS,);
			}, async (url,) => {
				writeAppPermissionsSnapshot(
					out,
					buildAppPermissionsSnapshot(
						"TEST",
						url,
						PROJECT_INCARNATION_HASH,
						LIVE_PERMISSIONS,
					),
				);
				const failure = await dssFailure(
					["app", "permissions-restore", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("different incarnation of project TEST",);
			},);
			expect(permissionRequests,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("does not PUT when the project key is replaced after the restore permission read", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		let detailReads = 0;
		let permissionReads = 0;
		let puts = 0;
		try {
			await withCliServer((req, res,) => {
				if (req.method === "GET" && req.url === PROJECT_DETAILS_PATH) {
					detailReads += 1;
					answerProjectDetails(
						req,
						res,
						detailReads === 1
							? PROJECT_CREATION_TAG
							: REPLACEMENT_PROJECT_CREATION_TAG,
					);
					return;
				}
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					permissionReads += 1;
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				if (req.method === "PUT" && req.url === PERMISSIONS_PATH) {
					puts += 1;
					sendJson(res, {},);
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				writeAppPermissionsSnapshot(
					out,
					buildAppPermissionsSnapshot(
						"TEST",
						url,
						PROJECT_INCARNATION_HASH,
						CHANGED_PERMISSIONS,
					),
				);
				const failure = await dssFailure(
					["app", "permissions-restore", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("not the same project incarnation",);
			},);
			expect(detailReads,).toBe(2,);
			expect(permissionReads,).toBe(1,);
			expect(puts,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("does not PUT on dry-run and reports hashes and differences", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			let live: unknown = LIVE_PERMISSIONS;
			let gets = 0;
			let puts = 0;
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					gets += 1;
					sendJson(res, live,);
					return;
				}
				puts += 1;
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
				live = CHANGED_PERMISSIONS;
				const { stdout, stderr, } = await dss(
					["app", "permissions-restore", "--file", out, "--dry-run",],
					{ env: cliEnv(url,), },
				);
				expect(stderr,).toBe("",);
				const result = JSON.parse(stdout,) as {
					dryRun: boolean;
					projectKey: string;
					beforeHash: string;
					desiredHash: string;
					verifiedHash: string | null;
					applied: boolean;
					differences: Array<{ path: string; }>;
				};
				expect(result.dryRun,).toBe(true,);
				expect(result.applied,).toBe(false,);
				expect(result.projectKey,).toBe("TEST",);
				expect(result.beforeHash,).toBe(permissionsHash(CHANGED_PERMISSIONS,),);
				expect(result.desiredHash,).toBe(permissionsHash(LIVE_PERMISSIONS,),);
				expect(result.verifiedHash,).toBe(null,);
				expect(result.beforeHash,).not.toBe(result.desiredHash,);
				expect(result.differences.length,).toBeGreaterThan(0,);
			},);
			expect(gets,).toBe(2,); // capture fetch + dry-run before-fetch
			expect(puts,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("does not PUT when live permissions already match the snapshot", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			let puts = 0;
			await withCliServer((req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					sendJson(res, LIVE_PERMISSIONS,);
					return;
				}
				puts += 1;
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
				const { stdout, stderr, } = await dss(
					["app", "permissions-restore", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(stderr,).toBe("",);
				const result = JSON.parse(stdout,) as {
					beforeHash: string;
					desiredHash: string;
					verifiedHash: string;
					applied: boolean;
					reason: string;
				};
				expect(result.applied,).toBe(false,);
				expect(result.reason,).toBe("unchanged",);
				expect(result.beforeHash,).toBe(result.desiredHash,);
				expect(result.verifiedHash,).toBe(result.desiredHash,);
			},);
			expect(puts,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("PUTs only when changed, refetches, and verifies the restored hash", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			let live: unknown = LIVE_PERMISSIONS;
			let gets = 0;
			let puts = 0;
			let putBody: unknown;
			await withCliServer(async (req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					gets += 1;
					sendJson(res, live,);
					return;
				}
				if (req.method === "PUT" && req.url === PERMISSIONS_PATH) {
					puts += 1;
					putBody = JSON.parse(await readBody(req,),);
					live = LIVE_PERMISSIONS;
					res.statusCode = 204;
					res.end();
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
				live = CHANGED_PERMISSIONS;
				const { stdout, stderr, } = await dss(
					["app", "permissions-restore", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(stderr,).toBe("",);
				const result = JSON.parse(stdout,) as {
					projectKey: string;
					beforeHash: string;
					desiredHash: string;
					verifiedHash: string;
					applied: boolean;
				};
				expect(result.projectKey,).toBe("TEST",);
				expect(result.applied,).toBe(true,);
				expect(result.beforeHash,).toBe(permissionsHash(CHANGED_PERMISSIONS,),);
				expect(result.desiredHash,).toBe(permissionsHash(LIVE_PERMISSIONS,),);
				expect(result.verifiedHash,).toBe(result.desiredHash,);
			},);
			expect(gets,).toBe(3,); // capture fetch + before-fetch + refetch after PUT
			expect(puts,).toBe(1,);
			expect(putBody,).toEqual(LIVE_PERMISSIONS,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("fails without exposing payload values when post-PUT verification mismatches", async () => {
		const dir = makeDir();
		const out = join(dir, "permissions.json",);
		try {
			let live: unknown = LIVE_PERMISSIONS;
			let gets = 0;
			await withCliServer(async (req, res,) => {
				if (answerProjectDetails(req, res,)) return;
				if (req.method === "GET" && req.url === PERMISSIONS_PATH) {
					gets += 1;
					sendJson(res, live,);
					return;
				}
				if (req.method === "PUT" && req.url === PERMISSIONS_PATH) {
					live = PARTIALLY_APPLIED_PERMISSIONS;
					res.statusCode = 204;
					res.end();
					return;
				}
				sendJson(res, { message: "unexpected request", }, 404,);
			}, async (url,) => {
				await dss(["app", "permissions-snapshot", "--output", out,], { env: cliEnv(url,), },);
				live = CHANGED_PERMISSIONS;
				const failure = await dssFailure(
					["app", "permissions-restore", "--file", out,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toContain("verification failed",);
				expect(failure.stderr,).toContain(permissionsHash(LIVE_PERMISSIONS,),);
				expect(failure.stderr,).toContain(permissionsHash(PARTIALLY_APPLIED_PERMISSIONS,),);
				// Never leak permission payload values or principals on stderr.
				expect(failure.stderr,).not.toContain("alice",);
				expect(failure.stderr,).not.toContain("PARTIAL",);
				expect(failure.stderr,).not.toContain("u1",);
			},);
			expect(gets,).toBe(3,); // capture fetch + before-fetch + refetch after PUT
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});
});
