import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const discussionCommands: Record<string, CommandMeta> = withUsage("discussion", {
	list: {
		handler: (c, a, f,) => {
			requireArgs(a, 2, commandUsage("discussion", "list",),);
			return c.discussions.list(a[0], a[1], f["project-key"] as string | undefined,);
		},
		description: "List discussions attached to a project object.",
		examples: ["dss discussion list DATASET customers",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				3,
				commandUsage("discussion", "get",),
			);
			return c.discussions.get(a[0], a[1], a[2], f["project-key"] as string | undefined,);
		},
		description: "Get one discussion with its replies.",
		examples: ["dss discussion get DATASET customers d123",],
	},
	create: {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				commandUsage("discussion", "create",),
			);
			const topic = f["topic"] as string | undefined;
			const reply = f["reply"] as string | undefined;
			if (!topic || !reply) {
				throw new UsageError("--topic and --reply are required.", "missing_required_flag",);
			}
			return c.discussions.create(a[0], a[1], topic, reply, f["project-key"] as string | undefined,);
		},
		description: "Create a discussion with its first reply.",
		examples: ["dss discussion create DATASET customers --topic Schema --reply Please-review",],
	},
	reply: {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				3,
				commandUsage("discussion", "reply",),
			);
			const text = f["text"] as string | undefined;
			if (!text) throw new UsageError("--text is required.", "missing_required_flag",);
			return c.discussions.reply(a[0], a[1], a[2], text, f["project-key"] as string | undefined,);
		},
		description: "Add a reply to an existing discussion.",
		examples: ["dss discussion reply DATASET customers d123 --text Done",],
	},
},);
