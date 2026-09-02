import type { DataikuClient, } from "../client.js";

export type CommandHandler = (
	client: DataikuClient,
	args: string[],
	flags: Record<string, string | boolean>,
) => Promise<unknown>;

export type LocalCommandHandler = (
	args: string[],
	flags: Record<string, string | boolean>,
) => Promise<unknown>;

export interface CommandPayloadSchema {
	stdin?: boolean;
	dataFlag?: boolean;
	dataFileFlag?: boolean;
	contentType?: "application/json" | "text/plain";
	jsonShape?: "object" | "array";
}

export interface CommandFlagChoice {
	oneOf: string[][];
}

export interface CommandRegistryOverride {
	requiredFlags?: string[];
	requiredOneOf?: CommandFlagChoice[];
	optionalFlags?: string[];
	payloadSchema?: CommandPayloadSchema;
	examplePayload?: unknown;
	cleanupCommand?: string;
}

export interface CommandMeta extends CommandRegistryOverride {
	handler: CommandHandler;
	localHandler?: LocalCommandHandler;
	usage: string;
	description?: string;
	examples?: string[];
}
