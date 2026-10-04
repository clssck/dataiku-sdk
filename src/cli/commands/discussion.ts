import { jsonInput, } from "../coerce.js";
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
	update: {
		handler: async (c, a, f,) => {
			const pk = f["project-key"] as string | undefined;
			const body = jsonInput(f,);
			const topic = f["topic"];
			if ((body === undefined) === (typeof topic !== "string")) {
				throw new UsageError(
					"Pass exactly one of --topic TEXT or --data/--data-file/--stdin.",
					"missing_required_flag",
				);
			}
			const next = body ?? { ...await c.discussions.get(a[0]!, a[1]!, a[2]!, pk,), topic, };
			return c.discussions.update(a[0]!, a[1]!, a[2]!, next, pk,);
		},
		description:
			"Update a discussion: --topic TEXT edits the topic of the current discussion, or --data sends a full discussion object from get (e.g. to move it to another object of the project).",
		examples: ['dss discussion update DATASET orders L8gkpoI6 --topic "Schema change (edited)"',],
	},
},);
