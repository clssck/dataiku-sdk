import { describe, it, } from "bun:test";
import { loadEnvFile, } from "../src/cli/env.js";
import { resolveCredentials, } from "../src/cli/runtime.js";
import { exerciseApplications, } from "./live-applications.js";
import { exerciseBundles, } from "./live-bundles.js";
import { exerciseCapabilities, provisionRetainedMl, } from "./live-capabilities.js";
import { matchesLiveCase, } from "./live-cases.js";
import { exerciseCollaboration, provisionCollaboration, } from "./live-collaboration.js";
import { LiveRunContext, loadLiveManifest, stopLiveCommands, } from "./live-context.js";
import { exerciseDeployers, } from "./live-deployers.js";
import { exerciseCoreFixtures, provisionCoreFixtures, } from "./live-fixtures.js";
import { exerciseFlowJobs, } from "./live-flow-jobs.js";
import { exerciseFoldersNotebooks, } from "./live-folders-notebooks.js";
import { exerciseInfrastructureDisposable, } from "./live-infrastructure-disposable.js";
import { exerciseInfrastructureReads, } from "./live-infrastructure-reads.js";
import { exercisePlugins, } from "./live-plugins.js";
import { exerciseProjectGit, } from "./live-project-git.js";
import { exerciseProjectLifecycle, } from "./live-project-lifecycle.js";
import { exerciseProjectSurface, } from "./live-project-surface.js";
import { exerciseScenariosStats, } from "./live-scenarios-stats.js";

const enabled = process.env.RUN_DATAIKU_LIVE === "1" && Boolean(process.env.DATAIKU_LIVE_MANIFEST,);
(enabled ? describe : describe.skip)("self-provisioned live CLI lab", () => {
	it("validates the selected live phase", async () => {
		loadEnvFile();
		const file = process.env.DATAIKU_LIVE_MANIFEST!;
		const manifest = await loadLiveManifest(file,);
		const phase = process.env.DATAIKU_LIVE_PHASE === "setup" ? "setup" : "run";
		const selection = (process.env.DATAIKU_LIVE_CASES ?? "").split(",",).filter(Boolean,);
		const ctx = new LiveRunContext(file, manifest, resolveCredentials({},), phase, selection,);
		let stopping = false;
		const stop = async (code: number,) => {
			if (stopping) return;
			stopping = true;
			await stopLiveCommands();
			await ctx.save();
			process.exit(code,);
		};
		const interrupt = () => {
			void stop(130,);
		};
		const terminate = () => {
			void stop(143,);
		};
		process.once("SIGINT", interrupt,);
		process.once("SIGTERM", terminate,);
		try {
			if (phase === "setup") {
				await provisionCoreFixtures(ctx,);
				await provisionCollaboration(ctx,);
				if (ctx.profiles.includes("ml",)) {
					await ctx.check("ml.setup.retained", [], async () => {
						await provisionRetainedMl(ctx,);
					},);
				}
			} else {
				if (!manifest.setupComplete) {
					throw new Error("Lab setup is incomplete; complete setup before running cases",);
				}
				await exerciseCoreFixtures(ctx,);
				await exerciseCollaboration(ctx,);
				await exerciseProjectLifecycle(ctx,);
				await exerciseProjectSurface(ctx,);
				await exerciseBundles(ctx,);
				await exerciseProjectGit(ctx,);
				await exerciseFlowJobs(ctx,);
				await exerciseFoldersNotebooks(ctx,);
				await exerciseScenariosStats(ctx,);
				await exerciseCapabilities(ctx,);
				if (ctx.profiles.includes("applications",)) await exerciseApplications(ctx,);
				if (ctx.profiles.includes("infrastructure",)) {
					await exerciseInfrastructureDisposable(ctx,);
					await exerciseInfrastructureReads(ctx,);
					await exercisePlugins(ctx,);
					await exerciseDeployers(ctx,);
				}
			}
			await ctx.save();
			for (const selected of selection) {
				if (
					!manifest.cases.some(c => matchesLiveCase(c.id, selected,))
				) throw new Error(`No live case matched ${selected}`,);
			}
			const failures = manifest.cases.filter(c =>
				c.status === "failed" || c.required !== false && c.status !== "passed"
			);
			if (failures.length) {
				throw new Error(
					`Live cases did not pass: ${failures.map(c => `${c.id} (${c.status})`).join(", ",)}`,
				);
			}
		} finally {
			process.off("SIGINT", interrupt,);
			process.off("SIGTERM", terminate,);
			await ctx.save();
		}
	}, 55 * 60 * 1000,);
},);
