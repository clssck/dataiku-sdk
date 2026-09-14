import { expect, } from "bun:test";
import { readFile, } from "node:fs/promises";
import { join, } from "node:path";
import { LiveCapabilityError, type LiveContext, } from "./live-context.js";

/**
 * Bundle live-suite module. Design-node bundle CRUD (export, list, download,
 * delete) runs against a disposable owned child project. Publication to the
 * Project Deployer and every Automation-node action are reported as explicit
 * blockers so their prerequisites never hide the working Design-node path.
 */

const BUNDLE_ID_PREFIX = "live_bundle";
const RELEASE_NOTES = "Live suite bundle release notes";

interface ExportedBundle {
	bundleId?: string;
	exportManifest?: { releaseNotes?: string; };
	[key: string]: unknown;
}

function blocked(message: string,): never {
	throw new LiveCapabilityError(message, "blocked",);
}

async function withOwnedProject(
	ctx: LiveContext,
	label: string,
	body: (projectKey: string,) => Promise<void>,
): Promise<void> {
	const errors: unknown[] = [];
	let key: string | undefined;
	try {
		key = await ctx.createProject(label,);
		await body(key,);
	} catch (error) {
		errors.push(error,);
	} finally {
		if (key) {
			try {
				await ctx.deleteProject(key,);
			} catch (error) {
				errors.push(error,);
			}
		}
	}
	if (errors.length) {
		throw new AggregateError(
			errors,
			errors.map(e => e instanceof Error ? e.message : String(e,)).join("; ",),
		);
	}
}

async function listExported(ctx: LiveContext, projectKey: string,): Promise<ExportedBundle[]> {
	const bundles = await ctx.run<ExportedBundle[]>(["bundle", "list-exported",], { projectKey, },);
	expect(Array.isArray(bundles,),).toBe(true,);
	return bundles;
}

export async function exerciseBundles(ctx: LiveContext,): Promise<void> {
	if (ctx.phase !== "run") return;

	await ctx.check("core.bundle.export-lifecycle", [
		"project.create",
		"bundle.list-exported",
		"bundle.export",
		"bundle.download-exported",
		"bundle.delete-exported",
		"project.delete",
	], () =>
		withOwnedProject(ctx, "bundle", async (projectKey,) => {
			const bundleId = `${BUNDLE_ID_PREFIX}_i${String(ctx.iteration,)}`;
			expect((await listExported(ctx, projectKey,)).some(b => b.bundleId === bundleId),).toBe(
				false,
			);
			// The instance configuration forbids disabling Project Standards
			// Checks on bundle creation (observed raw 400: "Creating a bundle
			// without evaluating Project Standards Checks is not allowed by
			// configuration"), so this case uses the server default (evaluate
			// = true) and never passes --evaluate-standards-checks false.
			const exported = await ctx.run<{ exported: string; }>([
				"bundle",
				"export",
				bundleId,
				"--release-notes",
				RELEASE_NOTES,
			], { projectKey, },);
			expect(exported.exported,).toBe(bundleId,);
			// One export with release notes proves the full action contract
			// (new id, --release-notes, default standards evaluation): DSS 15
			// REFUSES overwriting an existing exported bundle id (observed raw
			// 400 "Bundle <id> already exists for project <key>"), so the
			// official-client "create or overwrite" wording is wrong for this
			// server; every export here targets a fresh id.
			const first = (await listExported(ctx, projectKey,)).find(b => b.bundleId === bundleId);
			expect(first,).toBeTruthy();

			const archive = join(ctx.dir, `bundle-${String(ctx.iteration,)}.zip`,);
			const download = await ctx.run<{ path: string; bytes: number; }>([
				"bundle",
				"download-exported",
				bundleId,
				"--output",
				archive,
			], { projectKey, },);
			expect(download.bytes,).toBeGreaterThan(0,);
			const bytes = await readFile(archive,);
			expect(bytes.byteLength,).toBe(download.bytes,);
			// Zip local-file-header signature; DSS bundles are plain zip archives.
			expect(bytes.subarray(0, 2,).toString("latin1",),).toBe("PK",);

			const deleted = await ctx.run<{ deleted: boolean; }>([
				"bundle",
				"delete-exported",
				bundleId,
			], { projectKey, },);
			expect(deleted.deleted,).toBe(true,);
			expect((await listExported(ctx, projectKey,)).some(b => b.bundleId === bundleId),).toBe(
				false,
			);
		},),);

	// Real ownership via the context's project-deployer-project kind: the
	// reservation name carries the fresh 32-hex nonce (exact server identity
	// is projectBasicInfo.id === reserved name), create-project receives that
	// nonce-bearing key, the context binds it on the identity-verified readback,
	// bundle.publish targets ONLY that bound global (context rechecks identity
	// before execution), and cleanup is the context's guarded private DELETE.
	await ctx.check("core.bundle.publish", [
		"project-deployer.create-project",
		"project-deployer.project-status",
		"bundle.export",
		"bundle.publish",
		"project.create",
		"project.delete",
	], async () => {
		const published = await ctx.createGlobal(
			"project-deployer-project",
			"publish",
			(name,) => [
				"project-deployer",
				"create-project",
				"--data",
				JSON.stringify({ publishedProjectKey: name, },),
			],
		);
		try {
			await withOwnedProject(ctx, "publish", async (projectKey,) => {
				const bundleId = `${BUNDLE_ID_PREFIX}_pub_i${String(ctx.iteration,)}`;
				await ctx.run([
					"bundle",
					"export",
					bundleId,
					"--release-notes",
					RELEASE_NOTES,
				], { projectKey, },);
				const publish = await ctx.run<Record<string, unknown>>([
					"bundle",
					"publish",
					bundleId,
					"--published-project-key",
					published.name,
				], { projectKey, },);
				// The documented publish receipt echoes the published project key.
				const echoed = publish["publishedProjectKey"];
				expect(echoed === undefined || echoed === published.name,).toBe(true,);
			},);
			// The published project must resolve by its exact reserved identity.
			const status = await ctx.run<Record<string, unknown>>([
				"project-deployer",
				"project-status",
				published.name,
			],);
			expect(typeof status,).toBe("object",);
		} finally {
			await ctx.deleteGlobal("project-deployer-project", published.id ?? published.name,);
		}
	}, { capability: "bundle.project-deployer-publish", required: false, },);

	await ctx.check("core.bundle.automation-node", [
		"bundle.list-imported",
		"bundle.import-from-archive",
		"bundle.import-from-stream",
		"bundle.activate",
		"bundle.preload",
		"bundle.delete-imported",
	], async () => {
		blocked(
			"Imported-bundle actions (list-imported, import-from-archive, import-from-stream, activate, preload, delete-imported) only apply to a project on an Automation node; the lab credentials target the Design node and live cases cannot substitute server credentials.",
		);
	}, { capability: "bundle.automation-node", required: false, },);
}
