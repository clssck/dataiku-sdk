import { describe, expect, it, } from "bun:test";
import {
	dssRaw,
	hasCredentials,
	integrationEnabled,
	parseJsonOutput,
	uniqueTestName,
} from "./integration-harness.js";

const appId = process.env.DATAIKU_MASKED_403_APP_ID?.trim();
const predecessorProjectKey = process.env.DATAIKU_MASKED_403_FROM_PROJECT?.trim();
const describeMasked403Integration = integrationEnabled
		&& hasCredentials
		&& process.env.RUN_DATAIKU_MASKED_403_INTEGRATION === "1"
		&& appId
		&& predecessorProjectKey
	? describe
	: describe.skip;

describeMasked403Integration("App successor masked-403 live integration", () => {
	it("refuses an invisible unknown target without issuing creation", async () => {
		const targetProjectKey = uniqueTestName("ZZ_MASKED_403_TARGET",).toUpperCase();
		const result = await dssRaw([
			"app",
			"successor-preflight",
			appId!,
			"--from",
			predecessorProjectKey!,
			"--to",
			targetProjectKey,
		],);

		expect(result.code, result.stdout || result.stderr,).toBe(1,);
		const report = parseJsonOutput<{
			code: string;
			category: string;
			details: Record<string, unknown>;
		}>(result.stdout,);
		expect(report,).toMatchObject({
			code: "target_absence_unverifiable",
			category: "permission_or_environment",
			details: {
				targetProjectKey,
				directTargetProbe: 403,
				targetVisibleInProjectList: false,
				targetVisibleInAppInstances: false,
				preflightExecuted: true,
				creationPostAttempted: false,
			},
		},);
	});
},);
