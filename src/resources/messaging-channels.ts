import { readFile, } from "node:fs/promises";
import { basename, } from "node:path";
import type { UploadFormPart, } from "../client.js";
import { ClientValidationError, } from "../errors.js";
import { BaseResource, requireArrayResponse, requireNonEmpty, requireObject, } from "./base.js";

/** One entry of a channel's permission list: a group or a user, and whether it may use the channel. */
export interface MessagingChannelPermission extends Record<string, unknown> {
	group?: string;
	user?: string;
	canUse?: boolean;
}

/** A channel as DSS lists or returns it. Configuration fields (credentials included) arrive unredacted. */
export interface MessagingChannel extends Record<string, unknown> {
	id?: string;
	type?: string;
	family?: string;
}

export interface MessagingChannelListOptions {
	/** Channel type to filter by, e.g. `smtp`, `aws-ses-mail`, `slack`. */
	channelType?: string;
	/** Channel family to filter by; `mail` selects every channel that sends email-like messages. */
	channelFamily?: string;
}

export interface MessagingChannelCreateRequest {
	type: string;
	/** Channel id; DSS allocates one when omitted. */
	id?: string;
	/** Type-specific configuration (host, sender, webhookUrl, ...). */
	configuration?: Record<string, unknown>;
	/** Usage permissions; `null`/omitted lets DSS grant every user. */
	permissions?: MessagingChannelPermission[] | null;
}

export interface MailAttachment {
	/** Local file to attach. */
	path: string;
	/** Name the recipient sees; defaults to the file's base name. */
	fileName?: string;
}

export interface MailMessageRequest {
	to: string[];
	subject: string;
	body: string;
	cc?: string[];
	bcc?: string[];
	/** Sender address; defaults to the sender configured on the channel. */
	from?: string;
	/** Send `body` as plain text instead of HTML. */
	plainText?: boolean;
}

export interface MailSendOptions {
	/** Project issuing the email; the caller needs "Write content" on it. Defaults to the client's project. */
	projectKey?: string;
	/** One message to every `to` address (true) or one message per address (false, DSS default). */
	sendAsSingleMessage?: boolean;
	attachments?: MailAttachment[];
}

/** The JSON `message` part of the send request, shaped like the official Python client's payload. */
export function mailMessagePayload(message: MailMessageRequest,): Record<string, unknown> {
	return {
		from: message.from ?? null,
		to: message.to,
		cc: message.cc ?? null,
		bcc: message.bcc ?? null,
		subject: message.subject,
		body: message.body,
		plainText: message.plainText ?? false,
	};
}

/** Query string of the send request. */
export function mailSendQuery(
	projectKey: string,
	sendAsSingleMessage: boolean | undefined,
): URLSearchParams {
	return new URLSearchParams({
		projectKey,
		sendAsSingleMessage: String(sendAsSingleMessage ?? false,),
	},);
}

/** The POST body of a channel creation, as the official Python client sends it. */
export function messagingChannelCreateBody(
	request: MessagingChannelCreateRequest,
): Record<string, unknown> {
	return {
		id: request.id ? requireNonEmpty(request.id, "channelId",) : "",
		type: requireNonEmpty(request.type, "type",),
		configuration: request.configuration === undefined
			? {}
			: requireObject(request.configuration, "configuration",),
		permissions: request.permissions ?? null,
	};
}

export class MessagingChannelsResource extends BaseResource {
	/** List messaging channels visible to the caller, optionally filtered by type or family. */
	async list(options: MessagingChannelListOptions = {},): Promise<MessagingChannel[]> {
		const query = new URLSearchParams();
		if (options.channelType !== undefined) query.set("channelType", options.channelType,);
		if (options.channelFamily !== undefined) query.set("channelFamily", options.channelFamily,);
		const suffix = query.size > 0 ? `?${query.toString()}` : "";
		const channels = await this.client.get<unknown>(`/public/api/messaging-channels/${suffix}`,);
		return requireArrayResponse<MessagingChannel>(channels, "messagingChannels.list",);
	}

	/** Get one channel (type, family, and for mail channels the sender). */
	async get(channelId: string,): Promise<MessagingChannel> {
		return this.client.get<MessagingChannel>(this.channelPath(channelId,),);
	}

	/** Create a channel (admin). DSS returns the created channel. */
	async create(request: MessagingChannelCreateRequest,): Promise<MessagingChannel> {
		return this.client.post<MessagingChannel>(
			"/public/api/messaging-channels/",
			messagingChannelCreateBody(request,),
		);
	}

	/** Get a channel's settings, i.e. its configuration (admin). */
	async getSettings(channelId: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(`${this.channelPath(channelId,)}/settings`,);
	}

	/** Replace a channel's settings (admin); pass the object obtained from {@link getSettings}. */
	async saveSettings(channelId: string, settings: Record<string, unknown>,): Promise<void> {
		await this.client.putVoid(
			`${this.channelPath(channelId,)}/settings`,
			requireObject(settings, "settings",),
		);
	}

	/** Get the users and groups allowed to use a channel. */
	async getPermissions(channelId: string,): Promise<MessagingChannelPermission[]> {
		const permissions = await this.client.get<unknown>(
			`${this.channelPath(channelId,)}/permissions`,
		);
		return requireArrayResponse<MessagingChannelPermission>(
			permissions,
			"messagingChannels.getPermissions",
		);
	}

	/** Replace a channel's permission list. */
	async setPermissions(
		channelId: string,
		permissions: MessagingChannelPermission[],
	): Promise<void> {
		await this.client.putVoid(`${this.channelPath(channelId,)}/permissions`, permissions,);
	}

	/** Delete a channel (admin). */
	async delete(channelId: string,): Promise<void> {
		await this.client.del(this.channelPath(channelId,),);
	}

	/**
	 * Send an email through a mail-family channel, as a multipart request: a JSON
	 * `message` part plus one `attachments` part per file. This sends real mail
	 * and is never retried.
	 */
	async send(
		channelId: string,
		message: MailMessageRequest,
		options: MailSendOptions = {},
	): Promise<void> {
		const parts: UploadFormPart[] = [{
			name: "message",
			blob: new Blob([JSON.stringify(mailMessagePayload(message,),),], {
				type: "application/json",
			},),
			fileName: "send-payload.json",
		},];
		for (const attachment of options.attachments ?? []) {
			let blob: Blob;
			try {
				blob = new Blob([await readFile(attachment.path,),],);
			} catch {
				throw new ClientValidationError(
					`Attachment could not be read: ${attachment.path}`,
					"validation_failed",
					"Verify that the path names an existing, readable file.",
					{ path: attachment.path, },
				);
			}
			parts.push({
				name: "attachments",
				blob,
				fileName: attachment.fileName ?? basename(attachment.path,),
			},);
		}
		await this.client.uploadFormVoid(
			`${this.channelPath(channelId,)}/actions/send`,
			parts,
			mailSendQuery(this.resolveProjectKey(options.projectKey,), options.sendAsSingleMessage,),
		);
	}

	private channelPath(channelId: string,): string {
		return `/public/api/messaging-channels/${
			encodeURIComponent(requireNonEmpty(channelId, "channelId",),)
		}`;
	}
}
