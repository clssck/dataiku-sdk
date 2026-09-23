import { withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";

export const doctorCommands: Record<string, CommandMeta> = withUsage("doctor", {
	run: {
		handler: async (_c, _a, f,) => {
			const { runDoctor, } = await import("../doctor.js");
			return (await runDoctor(f,)).result;
		},
		description: "Run JSON diagnostics for DSS credentials, connectivity, and project access.",
		examples: ["dss doctor", "dss doctor --project-key MYPROJ", "dss doctor --capabilities --fast",],
	},
},);
