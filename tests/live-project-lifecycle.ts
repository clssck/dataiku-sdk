import { expect, } from "bun:test";
import { join, } from "node:path";
import type { LiveContext, } from "./live-context.js";

export async function exerciseProjectLifecycle(ctx: LiveContext,): Promise<void> {
	if (ctx.phase !== "run") return;
	await ctx.check("project.lifecycle", [
		"project.create",
		"project.export",
		"project.inspect-archive",
		"project.import",
		"project.duplicate",
		"project.delete",
		"variable.set",
		"variable.get",
	], async () => {
		const projects: string[] = [];
		const errors: unknown[] = [];
		try {
			const source = await ctx.createProject("lifecycle",);
			projects.push(source,);
			const marker = `live-${ctx.iteration}`;
			await ctx.run(["variable", "set", "--standard", JSON.stringify({ liveLifecycle: marker, },),], {
				projectKey: source,
			},);
			const archive = join(ctx.dir, `lifecycle-${ctx.iteration}.zip`,);
			await ctx.run(["project", "export", source, "--output", archive,],);
			const inspected = await ctx.run<Record<string, unknown>>([
				"project",
				"inspect-archive",
				archive,
			],);
			expect(inspected.sourceProjectKey,).toBe(source,);
			const imported = await ctx.importProject(archive, "imported",);
			projects.push(imported,);
			const duplicate = await ctx.duplicateProject(source, "duplicate",);
			projects.push(duplicate,);
			for (const key of [imported, duplicate,]) {
				const variables = await ctx.run<{ standard: Record<string, unknown>; }>(["variable", "get",], {
					projectKey: key,
				},);
				expect(variables.standard.liveLifecycle,).toBe(marker,);
			}
			expect((await ctx.client.projects.get(ctx.projectKey,)).projectKey,).toBe(ctx.projectKey,);
		} catch (error) {
			errors.push(error,);
		} finally {
			for (let index = projects.length - 1; index >= 0; index--) {
				const key = projects[index]!;
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
		const remaining = await ctx.client.projects.list();
		expect(remaining.some(project => projects.includes(project.projectKey,)),).toBe(false,);
		expect(remaining.some(project => project.projectKey === ctx.projectKey),).toBe(true,);
	},);
}
