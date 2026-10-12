const packageUrl = new URL("../package.json", import.meta.url,);
const pluginUrl = new URL("../plugin.json", import.meta.url,);
const packageJson = await Bun.file(packageUrl,).json();
const plugin = await Bun.file(pluginUrl,).json();
const expectedVersion = packageJson.version;

if (typeof expectedVersion !== "string" || expectedVersion.length === 0) {
	throw new Error("package.json must contain a non-empty version string.",);
}

if (process.argv.includes("--check",)) {
	if (plugin.version !== expectedVersion) {
		process.stderr.write(
			`plugin.json version ${
				JSON.stringify(plugin.version,)
			} does not match package.json version ${expectedVersion}.\n`,
		);
		process.exitCode = 1;
	}
} else {
	plugin.version = expectedVersion;
	await Bun.write(Bun.file(pluginUrl,), `${JSON.stringify(plugin, null, "\t",)}\n`,);
}
