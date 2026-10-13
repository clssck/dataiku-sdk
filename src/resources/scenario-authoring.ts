import { ClientValidationError, } from "../errors.js";
import { isRecord, } from "../utils/records.js";

/**
 * Pure builders for the JSON DSS stores under `triggers` and `reporters` of a
 * scenario. Trigger shapes mirror dataikuapi's `add_periodic_trigger`,
 * `add_hourly_trigger`, `add_daily_trigger` (which becomes a Weekly trigger when
 * given days) and `add_monthly_trigger`; the mail reporter shape is the one DSS
 * 15 stores for a `mail-scenario` reporter.
 */

export const SCENARIO_TRIGGER_FREQUENCIES = [
	"minutely",
	"hourly",
	"daily",
	"weekly",
	"monthly",
] as const;
export type ScenarioTriggerFrequency = typeof SCENARIO_TRIGGER_FREQUENCIES[number];

export const SCENARIO_WEEKDAYS = [
	"Monday",
	"Tuesday",
	"Wednesday",
	"Thursday",
	"Friday",
	"Saturday",
	"Sunday",
] as const;

export const SCENARIO_MONTHLY_RUN_ON = [
	"ON_THE_DAY",
	"LAST_DAY_OF_THE_MONTH",
	"FIRST_WEEK",
	"SECOND_WEEK",
	"THIRD_WEEK",
	"FOURTH_WEEK",
	"LAST_WEEK",
] as const;

export const SCENARIO_REPORTER_PHASES = ["START", "END",] as const;

export interface TemporalTriggerSpec {
	frequency: ScenarioTriggerFrequency;
	/** Repeat interval: minutes (minutely), hours, days, weeks or months. */
	every?: number;
	/** Hour of the day (daily, weekly, monthly) or hour the schedule starts from (hourly). */
	hour?: number;
	minute?: number;
	/** Weekday names; required by `weekly`, rejected elsewhere. */
	days?: readonly string[];
	/** Day of the month of the start date (monthly, default 1). */
	dayOfMonth?: number;
	runOn?: string;
	/** `YYYY-MM-DD` day before which the trigger does not run; default today (monthly: January 1st of this year). */
	startDate?: string;
	timezone?: string;
	name?: string;
	active?: boolean;
}

function invalid(message: string,): ClientValidationError {
	return new ClientValidationError(message, "invalid_flag_value",);
}

/** Membership test that narrows a free-form string to one of the allowed literals. */
function isOneOf<T extends string,>(allowed: readonly T[], value: string,): value is T {
	return (allowed as readonly string[]).includes(value,);
}

function boundedInteger(value: number, name: string, min: number, max: number,): number {
	if (!Number.isInteger(value,) || value < min || value > max) {
		throw invalid(`${name} must be an integer from ${min} to ${max}, got ${value}.`,);
	}
	return value;
}

const pad = (value: number, width = 2,): string => String(value,).padStart(width, "0",);

/**
 * dataikuapi's `startingFrom`: local midnight of the date, with milliseconds and
 * a `+HHMM` offset (taken from the machine's time zone, as dataikuapi does).
 */
function startingFrom(year: number, month: number, day: number,): string {
	const local = new Date(year, month - 1, day, 0, 0, 0, 0,);
	if (local.getFullYear() !== year || local.getMonth() !== month - 1 || local.getDate() !== day) {
		throw invalid(`${year}-${pad(month,)}-${pad(day,)} is not a calendar date.`,);
	}
	const offsetMinutes = -local.getTimezoneOffset();
	const sign = offsetMinutes < 0 ? "-" : "+";
	const absolute = Math.abs(offsetMinutes,);
	return `${year}-${pad(month,)}-${pad(day,)}T00:00:00.000${sign}${
		pad(Math.floor(absolute / 60,),)
	}${pad(absolute % 60,)}`;
}

function startingFromDate(startDate: string | undefined, fallback: () => string,): string {
	if (startDate === undefined) return fallback();
	const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(startDate,);
	if (!match) throw invalid(`--start-date must be YYYY-MM-DD, got ${JSON.stringify(startDate,)}.`,);
	return startingFrom(Number(match[1],), Number(match[2],), Number(match[3],),);
}

/** The scenario trigger object dataikuapi would append for this schedule. */
export function temporalTrigger(
	spec: TemporalTriggerSpec,
	now = new Date(),
): Record<string, unknown> {
	const every = spec.every === undefined
		? undefined
		: boundedInteger(spec.every, "--every", 1, 100_000,);
	const timezone = spec.timezone ?? "SERVER";
	if (spec.frequency !== "weekly" && spec.days !== undefined) {
		throw invalid(`--days applies to weekly triggers only (got --frequency ${spec.frequency}).`,);
	}
	if (spec.frequency !== "monthly" && (spec.runOn !== undefined || spec.dayOfMonth !== undefined)) {
		throw invalid(
			`--run-on and --day-of-month apply to monthly triggers only (got --frequency ${spec.frequency}).`,
		);
	}
	if (spec.frequency === "minutely") {
		for (
			const [flag, value,] of [["--hour", spec.hour,], ["--minute", spec.minute,], [
				"--start-date",
				spec.startDate,
			], ["--timezone", spec.timezone,],] as const
		) {
			if (value !== undefined) throw invalid(`${flag} does not apply to minutely triggers.`,);
		}
	}
	const today = () => startingFrom(now.getFullYear(), now.getMonth() + 1, now.getDate(),);
	const trigger = (params: Record<string, unknown>,): Record<string, unknown> => ({
		...(spec.name === undefined ? {} : { name: spec.name, }),
		active: spec.active ?? true,
		type: "temporal",
		params,
	});
	if (spec.frequency === "minutely") {
		return trigger({ frequency: "Minutely", repeatFrequency: every ?? 5, },);
	}
	const minute = boundedInteger(spec.minute ?? 0, "--minute", 0, 59,);
	if (spec.frequency === "hourly") {
		return trigger({
			frequency: "Hourly",
			hour: boundedInteger(spec.hour ?? 0, "--hour", 0, 23,),
			minute,
			startingFrom: startingFromDate(spec.startDate, today,),
			repeatFrequency: every ?? 1,
			timezone,
		},);
	}
	const hour = boundedInteger(spec.hour ?? 2, "--hour", 0, 23,);
	if (spec.frequency === "daily") {
		return trigger({
			frequency: "Daily",
			hour,
			minute,
			startingFrom: startingFromDate(spec.startDate, today,),
			repeatFrequency: every ?? 1,
			timezone,
		},);
	}
	if (spec.frequency === "weekly") {
		if (spec.days === undefined || spec.days.length === 0) {
			throw invalid(
				`--days is required for weekly triggers (${SCENARIO_WEEKDAYS.join(", ",)}, comma-separated).`,
			);
		}
		const unknown = spec.days.filter((day,) => !isOneOf(SCENARIO_WEEKDAYS, day,));
		if (unknown.length > 0) {
			throw invalid(
				`Unknown weekday ${unknown.map((day,) => JSON.stringify(day,)).join(", ",)}; use ${
					SCENARIO_WEEKDAYS.join(", ",)
				}.`,
			);
		}
		return trigger({
			frequency: "Weekly",
			hour,
			minute,
			startingFrom: startingFromDate(spec.startDate, today,),
			daysOfWeek: [...spec.days,],
			repeatFrequency: every ?? 1,
			timezone,
		},);
	}
	const runOn = spec.runOn ?? "ON_THE_DAY";
	if (!isOneOf(SCENARIO_MONTHLY_RUN_ON, runOn,)) {
		throw invalid(`--run-on must be one of ${SCENARIO_MONTHLY_RUN_ON.join(", ",)}, got ${runOn}.`,);
	}
	if (spec.startDate !== undefined && spec.dayOfMonth !== undefined) {
		throw invalid("--start-date and --day-of-month are mutually exclusive for monthly triggers.",);
	}
	const dayOfMonth = boundedInteger(spec.dayOfMonth ?? 1, "--day-of-month", 1, 31,);
	return trigger({
		frequency: "Monthly",
		startingFrom: startingFromDate(
			spec.startDate,
			// dataikuapi starts monthly schedules on January 1st (not today) so that day 31 never lands on a short month.
			() => startingFrom(now.getFullYear(), 1, dayOfMonth,),
		),
		hour,
		minute,
		monthlyRunOn: runOn,
		repeatFrequency: every ?? 1,
		timezone,
	},);
}

export interface MailReporterSpec {
	channel: string;
	recipient: string;
	subject: string;
	message: string;
	/** DSS expression gating the report, e.g. `outcome == 'FAILED'`. */
	condition?: string;
	phase?: string;
	name?: string;
	active?: boolean;
}

function requiredText(value: string, name: string,): string {
	if (typeof value !== "string" || value.trim().length === 0) {
		throw invalid(`${name} must be a non-empty string.`,);
	}
	return value;
}

/** A `mail-scenario` reporter as DSS 15 stores it (inline message, Freemarker, no attachments). */
export function mailReporter(spec: MailReporterSpec,): Record<string, unknown> {
	const phase = spec.phase ?? "END";
	if (!isOneOf(SCENARIO_REPORTER_PHASES, phase,)) {
		throw invalid(`--phase must be ${SCENARIO_REPORTER_PHASES.join(" or ",)}, got ${phase}.`,);
	}
	if (spec.condition !== undefined) requiredText(spec.condition, "--condition",);
	return {
		...(spec.name === undefined ? {} : { name: spec.name, }),
		active: spec.active ?? true,
		phase,
		runConditionEnabled: spec.condition !== undefined,
		runCondition: spec.condition ?? "true",
		messaging: {
			type: "mail-scenario",
			configuration: {
				channelId: requiredText(spec.channel, "--channel",),
				recipient: requiredText(spec.recipient, "--recipient",),
				subject: spec.subject,
				message: spec.message,
				messageSource: "INLINE",
				templateFormat: "FREEMARKER",
				sendAsHTML: false,
				attachments: [],
			},
		},
	};
}

/** Which element of a trigger/reporter list to remove. */
export interface ScenarioListSelector {
	index?: number;
	name?: string;
}

/** Position of the selected element; ambiguous or missing selections throw. */
export function selectScenarioListIndex(
	items: readonly unknown[],
	selector: ScenarioListSelector,
	kind: "trigger" | "reporter",
): number {
	if ((selector.index === undefined) === (selector.name === undefined)) {
		throw invalid(`Pass exactly one of --index or --name to select the ${kind}.`,);
	}
	if (selector.index !== undefined) {
		if (!Number.isInteger(selector.index,) || selector.index < 0 || selector.index >= items.length) {
			throw invalid(
				`--index ${selector.index} is outside the ${items.length} ${kind}(s) of this scenario (0-based).`,
			);
		}
		return selector.index;
	}
	const matches = items.flatMap((item, index,) =>
		isRecord(item,) && item["name"] === selector.name ? [index,] : []
	);
	if (matches.length !== 1) {
		throw invalid(
			matches.length === 0
				? `No ${kind} is named ${JSON.stringify(selector.name,)}.`
				: `${matches.length} ${kind}s are named ${JSON.stringify(selector.name,)} (indexes ${
					matches.join(", ",)
				}); use --index.`,
		);
	}
	return matches[0];
}
