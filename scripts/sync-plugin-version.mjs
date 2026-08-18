import * as fs from "node:fs";

const packageUrl = new URL("../package.json", import.meta.url,);
const pluginUrl = new URL("../plugin.json", import.meta.url,);
const packageJson = JSON.parse(fs.readFileSync(packageUrl, "utf-8",),);
const plugin = JSON.parse(fs.readFileSync(pluginUrl, "utf-8",),);
const expectedVersion = packageJson.version;

if (typeof expectedVersion !== "string" || expectedVersion.length === 0) {
	throw new Error("package.json must contain a non-empty version string.",);
}

if (process.argv.includes("--check",)) {
	if (plugin.version !== expectedVersion) {
		console.error(
			`plugin.json version ${
				JSON.stringify(plugin.version,)
			} does not match package.json version ${expectedVersion}.`,
		);
		process.exitCode = 1;
	}
} else {
	plugin.version = expectedVersion;
	fs.writeFileSync(pluginUrl, `${JSON.stringify(plugin, null, "\t",)}\n`, "utf-8",);
}
