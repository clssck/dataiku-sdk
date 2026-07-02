import { runDoctor, } from "../doctor.js";
import type { CommandMeta, } from "../types.js";

export const doctorCommands: Record<string, CommandMeta> = {
	run: {
		handler: async (_c, _a, f,) => (await runDoctor(f,)).result,
		usage: "dss doctor [--project-key KEY] [--capabilities] [--fast]",
		description: "Run JSON diagnostics for DSS credentials, connectivity, and project access.",
		examples: ["dss doctor", "dss doctor --project-key MYPROJ", "dss doctor --capabilities --fast",],
	},
};
