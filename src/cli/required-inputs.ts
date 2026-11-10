import type { CommandRegistryEntry, } from "./contract.js";
import { UsageError, } from "./usage.js";

function flagIsProvided(
	flags: Record<string, string | boolean>,
	name: string,
	allowEmpty = false,
): boolean {
	const value = flags[name];
	if (value === undefined || value === false) return false;
	if (typeof value !== "string") return true;
	return allowEmpty || value.length > 0;
}

/**
 * The registry's required flags and required-choice groups (the usage-level
 * contract of `command-syntax.json`) as a usage error. Shared by the batch
 * pre-validation and `--plan`, so a plan never previews a command that apply
 * would reject for a missing flag.
 */
export function requireEntryFlags(
	resource: string,
	action: string,
	flags: Record<string, string | boolean>,
	entry: CommandRegistryEntry,
): void {
	const allowEmptyFlags: Record<string, true> = {};
	for (const flag of entry.flags) {
		if (flag.allowEmptyValue === true) allowEmptyFlags[flag.name] = true;
	}
	for (const flagName of entry.requiredFlags) {
		if (!flagIsProvided(flags, flagName, allowEmptyFlags[flagName] === true,)) {
			throw new UsageError(
				`--${flagName} is required. Usage: ${entry.usage}`,
				"missing_required_flag",
			);
		}
	}
	for (const choice of entry.requiredOneOf ?? []) {
		const satisfied = choice.oneOf.some((alternative,) =>
			alternative.every((flagName,) =>
				flagIsProvided(flags, flagName, allowEmptyFlags[flagName] === true,),
			),
		);
		if (!satisfied) {
			const alternatives = choice.oneOf.map((alternative,) =>
				alternative.map((flagName,) => `--${flagName}`,).join(" and ",),
			).join(" or ",);
			throw new UsageError(
				`One of ${alternatives} is required for ${resource} ${action}. Usage: ${entry.usage}`,
				"missing_required_flag",
			);
		}
	}
}
