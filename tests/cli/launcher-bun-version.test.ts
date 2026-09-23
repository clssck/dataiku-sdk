import { expect, it, } from "bun:test";
import { chmodSync, mkdtempSync, rmSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { delimiter, join, } from "node:path";
import { SDK_ROOT, } from "./_harness.js";

const node = Bun.which("node",);

/** Run bin/dss.js under Node with a fake `bun` on PATH that reports `version`. */
function launchWithBun(version: string,): { exitCode: number | null; stdout: string; } {
	const dir = mkdtempSync(join(tmpdir(), "dss-fake-bun-",),);
	try {
		const fake = join(dir, "bun",);
		// Answers --version; any real launch exits 0 with a marker.
		writeFileSync(
			fake,
			`#!/bin/sh\nif [ "$1" = "--version" ]; then echo ${version}; else echo LAUNCHED; fi\n`,
		);
		chmodSync(fake, 0o755,);
		const child = Bun.spawnSync([node!, join(SDK_ROOT, "bin", "dss.js",), "version",], {
			env: { ...process.env, PATH: `${dir}${delimiter}${process.env.PATH ?? ""}`, },
			stdout: "pipe",
		},);
		return { exitCode: child.exitCode, stdout: child.stdout.toString(), };
	} finally {
		rmSync(dir, { recursive: true, force: true, },);
	}
}

it.skipIf(!node || process.platform === "win32",)(
	"the Node-launched bin checks the Bun version before spawning it",
	() => {
		const old = launchWithBun("1.3.9",);
		expect(old.exitCode,).toBe(2,);
		expect(JSON.parse(old.stdout,),).toMatchObject({ code: "internal_error", exitCode: 2, },);
		expect(old.stdout,).toContain("1.3.9 is older than",);

		// Pre-release and build suffixes compare by their release part.
		expect(launchWithBun("1.4.3-canary.12+abc",).stdout.trim(),).toBe("LAUNCHED",);
		expect(launchWithBun("1.4.0",).stdout.trim(),).toBe("LAUNCHED",);
	},
);
