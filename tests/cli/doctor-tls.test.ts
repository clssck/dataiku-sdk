import { describe, expect, it, } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, } from "node:fs";
import { createServer, } from "node:https";
import type { AddressInfo, } from "node:net";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { cliEnv, dssFailure, exec, } from "./_harness.js";

interface DoctorCheck {
	name: string;
	ok: boolean;
	details?: Record<string, unknown>;
}

// The certificate is generated per run; hosts without openssl (e.g. bare Windows) skip.
describe("doctor TLS trust failures", () => {
	it.skipIf(Bun.which("openssl",) === null,)(
		"reports the TLS cause and trust hint on the connectivity check",
		async () => {
			const dir = mkdtempSync(join(tmpdir(), "dss-doctor-tls-",),);
			try {
				await exec("openssl", [
					"req",
					"-x509",
					"-newkey",
					"rsa:2048",
					"-nodes",
					"-keyout",
					join(dir, "key.pem",),
					"-out",
					join(dir, "cert.pem",),
					"-days",
					"1",
					"-subj",
					"/CN=localhost",
					"-addext",
					"subjectAltName=DNS:localhost",
				],);
				const https = createServer({
					key: readFileSync(join(dir, "key.pem",),),
					cert: readFileSync(join(dir, "cert.pem",),),
				}, (_req, res,) => {
					res.setHeader("Content-Type", "application/json",);
					res.end("[]",);
				},);
				await new Promise<void>((resolve,) => https.listen(0, "127.0.0.1", resolve,));
				try {
					const port = (https.address() as AddressInfo).port;
					const env = cliEnv(`https://localhost:${port}`,);
					delete env.DATAIKU_PROJECT_KEY;
					delete env.NODE_EXTRA_CA_CERTS;
					delete env.NODE_TLS_REJECT_UNAUTHORIZED;
					const failure = await dssFailure(["doctor",], { env, },);
					const report = JSON.parse(failure.stdout,) as { ok: boolean; checks: DoctorCheck[]; };
					expect(report.ok,).toBe(false,);
					const connectivity = report.checks.find((check,) => check.name === "connectivity");
					expect(connectivity?.ok,).toBe(false,);
					expect(String(connectivity?.details?.message,),).toMatch(
						/self.signed|SELF_SIGNED_CERT|unable to verify/i,
					);
					expect(String(connectivity?.details?.retryHint,),).toContain("--ca-cert",);
					expect(String(connectivity?.details?.retryHint,),).toContain("NODE_EXTRA_CA_CERTS",);
				} finally {
					https.close();
				}
			} finally {
				rmSync(dir, { recursive: true, force: true, },);
			}
		},
	);
});
