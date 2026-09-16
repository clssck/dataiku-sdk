import { afterEach, expect, it, } from "bun:test";
import { createHash, } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { verifyCandidate, verifyPublishedCandidate, } from "../scripts/release-candidate.mjs";

const directories: string[] = [];
afterEach(() => {
	for (const directory of directories.splice(0,)) {
		fs.rmSync(directory, { recursive: true, force: true, },);
	}
},);
function candidate() {
	const directory = fs.mkdtempSync(path.join(os.tmpdir(), "dss-publication-test-",),);
	directories.push(directory,);
	const bytes = Buffer.from("immutable candidate content",);
	const manifest = {
		name: "dataiku-sdk",
		version: "3.3.1",
		baseRevision: "a".repeat(40,),
		commit: "b".repeat(40,),
		bytes: bytes.length,
		integrity: "sha512-" + createHash("sha512",).update(bytes,).digest("base64",),
	};
	fs.writeFileSync(path.join(directory, "package.tgz",), bytes,);
	fs.writeFileSync(path.join(directory, "manifest.json",), JSON.stringify(manifest,),);
	return { directory, bytes, manifest, };
}
function metadata(fixture: ReturnType<typeof candidate>, url: string,) {
	return {
		name: fixture.manifest.name,
		version: fixture.manifest.version,
		dist: {
			integrity: fixture.manifest.integrity,
			tarball: url + "dataiku-sdk/-/dataiku-sdk-3.3.1.tgz",
		},
	};
}

it("waits for both metadata and the canonical tarball before confirming publication", async () => {
	const fixture = candidate();
	let metadataReads = 0;
	let tarballReads = 0;
	const server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		fetch(request,) {
			if (request.url.endsWith(".tgz",)) {
				tarballReads++;
				return tarballReads === 1
					? new Response(null, { status: 404, },)
					: new Response(fixture.bytes,);
			}
			metadataReads++;
			return metadataReads === 1
				? new Response(null, { status: 404, },)
				: Response.json(metadata(fixture, new URL(request.url,).origin + "/",),);
		},
	},);
	try {
		const result = await verifyPublishedCandidate(fixture.directory, {
			registry: server.url.href,
			pollMs: 1,
			timeoutMs: 2_000,
		},);
		expect(result.integrity,).toBe(fixture.manifest.integrity,);
		expect(metadataReads,).toBe(3,);
		expect(tarballReads,).toBe(2,);
	} finally {
		await server.stop(true,);
	}
});

for (const mismatch of ["metadata", "download-url", "tarball",] as const) {
	it(`refuses a published ${mismatch} mismatch without polling it again`, async () => {
		const fixture = candidate();
		let requests = 0;
		const server = Bun.serve({
			hostname: "127.0.0.1",
			port: 0,
			fetch(request,) {
				requests++;
				if (request.url.endsWith(".tgz",)) return new Response(Buffer.alloc(fixture.bytes.length, 0,),);
				const value = metadata(fixture, new URL(request.url,).origin + "/",);
				if (mismatch === "metadata") value.dist.integrity = "sha512-" + "0".repeat(86,) + "==";
				if (mismatch === "download-url") value.dist.tarball = "https://untrusted.invalid/package.tgz";
				return Response.json(value,);
			},
		},);
		try {
			await expect(
				verifyPublishedCandidate(fixture.directory, {
					registry: server.url.href,
					timeoutMs: 2_000,
					pollMs: 1,
				},),
			).rejects.toThrow(/different content|integrity mismatch/,);
			expect(requests,).toBe(mismatch === "tarball" ? 2 : 1,);
		} finally {
			await server.stop(true,);
		}
	});
}

it("bounds a stalled registry body by the overall verification deadline", async () => {
	const fixture = candidate();
	const server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		fetch() {
			return new Response(
				new ReadableStream({
					start(controller,) {
						controller.enqueue(new TextEncoder().encode("{",),);
					},
				},),
			);
		},
	},);
	try {
		const start = Date.now();
		await expect(
			verifyPublishedCandidate(fixture.directory, {
				registry: server.url.href,
				timeoutMs: 100,
				pollMs: 1,
			},),
		).rejects.toThrow("Publication not confirmed",);
		expect(Date.now() - start,).toBeLessThan(2_000,);
	} finally {
		await server.stop(true,);
	}
});

it("refuses a changed local candidate before contacting the registry", () => {
	const fixture = candidate();
	fs.writeFileSync(
		path.join(fixture.directory, "package.tgz",),
		Buffer.alloc(fixture.bytes.length, 0,),
	);
	expect(() => verifyCandidate(fixture.directory,)).toThrow("integrity mismatch",);
});
