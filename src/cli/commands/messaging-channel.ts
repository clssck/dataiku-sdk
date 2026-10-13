import { access, } from "node:fs/promises";
import { basename, } from "node:path";
import { ClientValidationError, } from "../../errors.js";
import {
	type MailAttachment,
	mailMessagePayload,
	type MailMessageRequest,
	mailSendQuery,
	messagingChannelCreateBody,
	type MessagingChannelCreateRequest,
	type MessagingChannelPermission,
} from "../../resources/messaging-channels.js";
import { deepMerge, } from "../../utils/deep-merge.js";
import {
	messagingChannelSecretsFromBody,
	messagingChannelSettingsHash,
	sanitizeMessagingChannelError,
	sanitizeMessagingChannelSecrets,
} from "../../utils/messaging-channel-secrets.js";
import { isRecord, } from "../../utils/records.js";
import { SECRET_REDACTED, } from "../../utils/secret-sanitize.js";
import { SHA256_HEX_PATTERN, stableJson, } from "../../utils/stable-hash.js";
import {
	parseBooleanOption,
	parseJsonValue,
	readInputText,
	readStdinText,
	requiredJsonInput,
	requiredStringFlag,
	splitCsvFlag,
	unknownJsonInput,
} from "../coerce.js";
import { executionMode, } from "../flags.js";
import { planResult, readIfExists, skipResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

const RESOURCE = "messaging-channel";
const PLAN_FAILURE_CODES = { usage: 1, error: 2, transient: 3, };

/** The offline request shape of a messaging-channel write; mirrors the fields `commandPlanShape` returns. */
export interface MessagingChannelPlanShape {
	endpoint?: string;
	exact?: boolean;
	identifiers?: Record<string, unknown>;
	method?: string;
	payload?: unknown;
	reason?: string;
	requests?: unknown;
}

function channelEndpoint(channelId: string,): string {
	return `/public/api/messaging-channels/${encodeURIComponent(channelId,)}`;
}

function channelIdArg(args: string[], usage: string,): string {
	requireArgs(args, 1, usage,);
	const channelId = args[0]!.trim();
	if (channelId.length === 0) {
		throw new UsageError(
			`channel id must be a non-empty string.\nUsage: ${usage}`,
			"validation_failed",
		);
	}
	return channelId;
}

/**
 * Read commands print `[redacted]` for credentials. Writing that placeholder
 * back would overwrite the stored secret with the literal marker, so it is
 * refused before any request.
 */
function rejectRedactedPlaceholder(value: unknown, source: string,): void {
	if (value === SECRET_REDACTED) {
		throw new UsageError(
			`${source} contains the ${SECRET_REDACTED} placeholder that read commands print instead of a credential.`,
			"validation_failed",
			"Omit unchanged credential fields (settings-set merges into the stored settings) or supply the real value.",
		);
	}
	if (Array.isArray(value,)) {
		for (const item of value) rejectRedactedPlaceholder(item, source,);
	} else if (isRecord(value,)) {
		for (const item of Object.values(value,)) rejectRedactedPlaceholder(item, source,);
	}
}

function permissionsFromValue(value: unknown, source: string,): MessagingChannelPermission[] {
	if (!Array.isArray(value,) || !value.every(isRecord,)) {
		throw new UsageError(
			`${source} must be a JSON array of permission objects such as [{"group":"analysts","canUse":true},{"user":"jdoe","canUse":false}].`,
			"validation_failed",
		);
	}
	return value;
}

function createInputFromFlags(
	flags: Record<string, string | boolean>,
	usage: string,
): { request: MessagingChannelCreateRequest; body: Record<string, unknown>; secrets: string[]; } {
	const type = requiredStringFlag(flags, "type", usage,);
	const configuration = unknownJsonInput(flags,) ?? {};
	if (!isRecord(configuration,)) {
		throw new UsageError(
			"The channel configuration must be a JSON object.",
			"validation_failed",
			`Usage: ${usage}`,
		);
	}
	rejectRedactedPlaceholder(configuration, "The channel configuration",);
	const permissionsText = flags["permissions"];
	const permissions = typeof permissionsText === "string"
		? permissionsFromValue(parseJsonValue(permissionsText, "--permissions",), "--permissions",)
		: undefined;
	const id = typeof flags["id"] === "string" ? flags["id"].trim() : undefined;
	const request: MessagingChannelCreateRequest = {
		type,
		...(id ? { id, } : {}),
		configuration,
		permissions,
	};
	const body = messagingChannelCreateBody(request,);
	return { request, body, secrets: messagingChannelSecretsFromBody(body,), };
}

interface SendInput {
	channelId: string;
	message: MailMessageRequest;
	attachments: MailAttachment[];
	sendAsSingleMessage: boolean;
}

function mailBodyFromFlags(flags: Record<string, string | boolean>, usage: string,): string {
	const sources = (["body", "body-file", "stdin",] as const).filter((name,) =>
		name === "stdin"
			? parseBooleanOption(flags[name], "--stdin",) === true
			: flags[name] !== undefined
	);
	if (sources.length !== 1) {
		throw new UsageError(
			sources.length === 0
				? `--body, --body-file, or --stdin is required (message body). Usage: ${usage}`
				: `Conflicting input sources: ${
					sources.map((name,) => `--${name}`).join(" or ",)
				}. Provide exactly one.`,
			sources.length === 0 ? "missing_required_flag" : "conflicting_input_sources",
		);
	}
	if (sources[0] === "stdin") return readStdinText();
	if (sources[0] === "body-file") return readInputText(String(flags["body-file"],), "--body-file",);
	return String(flags["body"],);
}

function sendInputFromFlags(
	args: string[],
	flags: Record<string, string | boolean>,
	usage: string,
): SendInput {
	const channelId = channelIdArg(args, usage,);
	const to = splitCsvFlag(flags["to"],);
	if (to.length === 0) {
		throw new UsageError(
			`--to is required (comma-separated recipient addresses). Usage: ${usage}`,
			"missing_required_flag",
		);
	}
	const subject = flags["subject"];
	if (typeof subject !== "string") {
		throw new UsageError(`--subject is required. Usage: ${usage}`, "missing_required_flag",);
	}
	const cc = splitCsvFlag(flags["cc"],);
	const bcc = splitCsvFlag(flags["bcc"],);
	const from = flags["from"];
	return {
		channelId,
		message: {
			to,
			subject,
			body: mailBodyFromFlags(flags, usage,),
			...(cc.length > 0 ? { cc, } : {}),
			...(bcc.length > 0 ? { bcc, } : {}),
			...(typeof from === "string" && from.trim() ? { from: from.trim(), } : {}),
			plainText: flags["plain-text"] === true,
		},
		attachments: splitCsvFlag(flags["attachment"],).map((path,) => ({ path, })),
		sendAsSingleMessage: flags["single-message"] === true,
	};
}

/** Dot paths whose value differs between two settings objects; values stay out of the result. */
function changedPaths(before: unknown, after: unknown, prefix = "",): string[] {
	if (isRecord(before,) && isRecord(after,)) {
		return [...new Set([...Object.keys(before,), ...Object.keys(after,),],),].flatMap((key,) =>
			changedPaths(before[key], after[key], prefix ? `${prefix}.${key}` : key,)
		);
	}
	return stableJson(before,) === stableJson(after,) ? [] : [prefix,];
}

function validatedExpectHash(
	value: string | boolean | undefined,
	usage: string,
): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !SHA256_HEX_PATTERN.test(value,)) {
		throw new UsageError(
			`--expect-hash must be a 64-character SHA-256 hex digest (the settingsHash of settings-get). Usage: ${usage}`,
			"invalid_flag_value",
		);
	}
	return value.toLowerCase();
}

/**
 * Offline request shape of a messaging-channel write, shared by `--plan` and
 * `--dry-run`. Credentials are redacted: a plan never carries a secret.
 */
export function messagingChannelPlanShape(
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
	projectKey: string | undefined,
): MessagingChannelPlanShape {
	const usage = commandUsage(RESOURCE, action,);
	switch (action) {
		case "create": {
			const { body, } = createInputFromFlags(flags, usage,);
			return {
				method: "POST",
				endpoint: "/public/api/messaging-channels/",
				identifiers: typeof body["id"] === "string" && body["id"] !== "" ? { id: body["id"], } : {},
				payload: sanitizeMessagingChannelSecrets(body,),
			};
		}
		case "settings-set": {
			const channelId = channelIdArg(args, usage,);
			const patch = requiredJsonInput(
				flags,
				`--data, --data-file, or --stdin is required (settings to merge). Usage: ${usage}`,
			);
			rejectRedactedPlaceholder(patch, "The settings patch",);
			const expectHash = validatedExpectHash(flags["expect-hash"], usage,);
			const endpoint = `${channelEndpoint(channelId,)}/settings`;
			return {
				exact: false,
				reason:
					"A fresh GET supplies the current settings, the patch is merged into them, and the merged object is PUT back.",
				endpoint,
				identifiers: { id: channelId, ...(expectHash ? { expectHash, } : {}), },
				requests: [
					{ method: "GET", endpoint, purpose: "read current settings and check --expect-hash", },
					{
						method: "PUT",
						endpoint,
						payload: "<omitted; merged settings may include credentials>",
						redactedFields: ["payload",],
					},
				],
			};
		}
		case "permissions-set": {
			const channelId = channelIdArg(args, usage,);
			const permissions = permissionsFromValue(
				unknownJsonInput(flags,),
				"The permissions input",
			);
			return {
				method: "PUT",
				endpoint: `${channelEndpoint(channelId,)}/permissions`,
				identifiers: { id: channelId, },
				payload: permissions,
			};
		}
		case "delete": {
			const channelId = channelIdArg(args, usage,);
			return {
				method: "DELETE",
				endpoint: channelEndpoint(channelId,),
				identifiers: { id: channelId, },
			};
		}
		case "send": {
			const input = sendInputFromFlags(args, flags, usage,);
			if (!projectKey) {
				throw new UsageError(
					`--project-key is required for send. Usage: ${usage}`,
					"missing_required_flag",
				);
			}
			return {
				method: "POST",
				endpoint: `${channelEndpoint(input.channelId,)}/actions/send?${
					mailSendQuery(projectKey, input.sendAsSingleMessage,).toString()
				}`,
				identifiers: { id: input.channelId, },
				payload: {
					upload: "multipart",
					message: mailMessagePayload(input.message,),
					attachments: input.attachments.map((attachment,) => ({
						path: attachment.path,
						fileName: basename(attachment.path,),
					})),
				},
			};
		}
		default:
			return {
				exact: false,
				reason: `No offline request shape is defined for ${RESOURCE}.${action}.`,
			};
	}
}

/** `--dry-run` result: the redacted plan, flagged as also being a dry run. */
function dryRunPlan(
	action: string,
	shape: MessagingChannelPlanShape,
	idempotency: string,
): Record<string, unknown> {
	return planResult(RESOURCE, action, {
		...shape,
		asyncKind: "none",
		exitCodesOnFailure: PLAN_FAILURE_CODES,
		idempotency,
		plannedAndDryRun: true,
	},);
}

export const messagingChannelCommands: Record<string, CommandMeta> = withUsage(RESOURCE, {
	list: {
		handler: async (c, _a, f,) =>
			sanitizeMessagingChannelSecrets(
				await c.messagingChannels.list({
					channelType: f["type"] as string | undefined,
					channelFamily: f["family"] as string | undefined,
				},),
			),
		description:
			"List messaging channels visible to the caller (compact: id, type, family; --full returns the DSS objects). Filter with --type (e.g. smtp, aws-ses-mail, slack) or --family mail. Credentials are redacted in output.",
		examples: ["dss messaging-channel list --family mail",],
	},
	get: {
		handler: async (c, a,) =>
			sanitizeMessagingChannelSecrets(
				await c.messagingChannels.get(channelIdArg(a, commandUsage(RESOURCE, "get",),),),
			),
		description:
			"Get one messaging channel (type, family, sender of a mail channel). Credentials are redacted in output.",
		examples: ["dss messaging-channel get smtp-main",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const { request, secrets, } = createInputFromFlags(f, commandUsage(RESOURCE, "create",),);
			if (executionMode(f,).dryRun) {
				return dryRunPlan(
					"create",
					messagingChannelPlanShape("create", [], f, undefined,),
					"none",
				);
			}
			try {
				return sanitizeMessagingChannelSecrets(
					await c.messagingChannels.create(request,),
					secrets,
				);
			} catch (error) {
				throw sanitizeMessagingChannelError(error, secrets,);
			}
		},
		description:
			"Create a messaging channel (admin). --type: smtp, aws-ses-mail, microsoft-graph-mail, slack, msteams, google-chat, twilio, shell; --data is its configuration; --id defaults to a DSS-allocated id; --permissions is a JSON array of {group|user, canUse} (all users when omitted). Credentials are never echoed.",
		examples: [
			`dss messaging-channel create --type shell --id ops-shell --data '{"type":"COMMAND","command":"echo hi"}' --dry-run`,
		],
	},
	"settings-get": {
		handler: async (c, a,) => {
			const channelId = channelIdArg(a, commandUsage(RESOURCE, "settings-get",),);
			const settings = await c.messagingChannels.getSettings(channelId,);
			return {
				id: channelId,
				settings: sanitizeMessagingChannelSecrets(settings,),
				settingsHash: messagingChannelSettingsHash(settings,),
			};
		},
		description:
			"Get a messaging channel's settings, i.e. its configuration (admin). Credentials are redacted; settingsHash fingerprints the redacted settings for settings-set --expect-hash.",
		examples: ["dss messaging-channel settings-get smtp-main",],
	},
	"settings-set": {
		handler: async (c, a, f,) => {
			const usage = commandUsage(RESOURCE, "settings-set",);
			const channelId = channelIdArg(a, usage,);
			const patch = requiredJsonInput(
				f,
				`--data, --data-file, or --stdin is required (settings to merge). Usage: ${usage}`,
			);
			rejectRedactedPlaceholder(patch, "The settings patch",);
			const expectHash = validatedExpectHash(f["expect-hash"], usage,);
			const current = await c.messagingChannels.getSettings(channelId,);
			const currentHash = messagingChannelSettingsHash(current,);
			if (expectHash !== undefined && currentHash !== expectHash) {
				throw new ClientValidationError(
					`The messaging channel ${JSON.stringify(channelId,)} changed since it was read.`,
					"validation_failed",
					"Re-read the settings with `dss messaging-channel settings-get` and retry with the current settingsHash.",
					{ id: channelId, expectedHash: expectHash, actualHash: currentHash, },
				);
			}
			const next = deepMerge(current, patch,);
			const secrets = messagingChannelSecretsFromBody(next,);
			const changed = changedPaths(current, next,);
			const nextHash = messagingChannelSettingsHash(next,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "settings-set",
					resource: RESOURCE,
					id: channelId,
					method: "PUT",
					endpoint: `${channelEndpoint(channelId,)}/settings`,
					current: sanitizeMessagingChannelSecrets(current,),
					next: sanitizeMessagingChannelSecrets(next,),
					changed,
					currentHash,
					nextHash,
					...(expectHash !== undefined ? { expectHash, provenanceVerified: true, } : {}),
				};
			}
			try {
				await c.messagingChannels.saveSettings(channelId, next,);
			} catch (error) {
				throw sanitizeMessagingChannelError(error, secrets,);
			}
			return { updated: channelId, changed, previousHash: currentHash, settingsHash: nextHash, };
		},
		description:
			"Merge supplied fields into a messaging channel's settings (GET-merge-PUT, admin); other fields are kept. --expect-hash takes settingsHash from settings-get and refuses the write if the redacted settings changed since. --dry-run shows redacted current/next settings, changed paths, and hashes. Never paste a [redacted] placeholder back: omit unchanged credentials.",
		examples: [
			`dss messaging-channel settings-set smtp-main --data '{"sender":"noreply@example.com"}' --dry-run`,
		],
	},
	"permissions-get": {
		handler: (c, a,) =>
			c.messagingChannels.getPermissions(
				channelIdArg(a, commandUsage(RESOURCE, "permissions-get",),),
			),
		description:
			'Get the permission list of a messaging channel: [{"group":"analysts","canUse":true},{"user":"jdoe","canUse":true}].',
		examples: ["dss messaging-channel permissions-get smtp-main",],
	},
	"permissions-set": {
		handler: async (c, a, f,) => {
			const channelId = channelIdArg(a, commandUsage(RESOURCE, "permissions-set",),);
			const permissions = permissionsFromValue(unknownJsonInput(f,), "The permissions input",);
			if (executionMode(f,).dryRun) {
				return dryRunPlan(
					"permissions-set",
					messagingChannelPlanShape("permissions-set", a, f, undefined,),
					"convergent",
				);
			}
			await c.messagingChannels.setPermissions(channelId, permissions,);
			return { updated: channelId, permissions, };
		},
		description:
			"Replace a messaging channel's permission list with a JSON array of {group|user, canUse} entries; read the current list with permissions-get, edit it, and send it back.",
		examples: [
			`dss messaging-channel permissions-set smtp-main --data '[{"group":"analysts","canUse":true}]' --dry-run`,
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			const channelId = channelIdArg(a, commandUsage(RESOURCE, "delete",),);
			if (executionMode(f,).dryRun || f["if-exists"] === true) {
				const current = await readIfExists(() => c.messagingChannels.get(channelId,));
				if (!current) return skipResult(RESOURCE, channelId, "missing",);
				if (executionMode(f,).dryRun) {
					return dryRunPlan(
						"delete",
						messagingChannelPlanShape("delete", a, f, undefined,),
						"if-exists",
					);
				}
			}
			await c.messagingChannels.delete(channelId,);
			return { deleted: channelId, resource: RESOURCE, };
		},
		description:
			"Delete a messaging channel (admin). Scenario reporters that point at it stop delivering.",
		examples: ["dss messaging-channel delete ops-shell --if-exists",],
	},
	send: {
		handler: async (c, a, f,) => {
			const usage = commandUsage(RESOURCE, "send",);
			const input = sendInputFromFlags(a, f, usage,);
			const projectKey = c.resolveProjectKey(f["project-key"] as string | undefined,);
			if (executionMode(f,).dryRun) {
				// Attachments are the one input a dry run can still check without sending.
				for (const attachment of input.attachments) {
					await access(attachment.path,).catch(() => {
						throw new UsageError(
							`Attachment could not be read: ${attachment.path}`,
							"validation_failed",
							"Verify that the path names an existing, readable file.",
						);
					},);
				}
				return dryRunPlan(
					"send",
					messagingChannelPlanShape("send", a, f, projectKey,),
					"none",
				);
			}
			await c.messagingChannels.send(input.channelId, input.message, {
				projectKey,
				sendAsSingleMessage: input.sendAsSingleMessage,
				attachments: input.attachments,
			},);
			return {
				sent: input.channelId,
				projectKey,
				to: input.message.to,
				cc: input.message.cc ?? [],
				bcc: input.message.bcc ?? [],
				subject: input.message.subject,
				attachments: input.attachments.map((attachment,) => basename(attachment.path,)),
				sendAsSingleMessage: input.sendAsSingleMessage,
			};
		},
		description:
			"Send an email through a mail channel (`list --family mail`) for a project (needs Write content). Delivered mail cannot be recalled: preview with --dry-run or --plan. Body is HTML unless --plain-text; --attachment takes comma-separated or repeated paths; one message per --to address unless --single-message.",
		examples: [
			'dss messaging-channel send smtp-main --to a@example.com,b@example.com --subject "Nightly build" --body "<b>done</b>" --project-key MYPROJ --dry-run',
		],
	},
},);
