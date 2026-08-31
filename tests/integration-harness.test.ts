import { expect, it, } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { dssRaw, } from "./integration-harness.js";

it("does not let Bun preload cwd .env into scrubbed integration subprocesses", async () => {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "dss-integration-env-",),);
	const home = path.join(root, "home",);
	const injectedConfig = path.join(root, "injected-config",);
	await fs.mkdir(injectedConfig, { recursive: true, },);
	await fs.writeFile(path.join(root, ".env",), `DSS_CONFIG_DIR=${injectedConfig}\n`,);
	await fs.writeFile(
		path.join(injectedConfig, "credentials.json",),
		JSON.stringify({ url: "http://127.0.0.1:1", apiKey: "fake-key", },),
	);

	const env: NodeJS.ProcessEnv = {
		...process.env,
		HOME: home,
		USERPROFILE: home,
		XDG_CONFIG_HOME: path.join(home, "xdg",),
		APPDATA: path.join(home, "appdata",),
		DATAIKU_URL: "",
		DATAIKU_API_KEY: "",
		DATAIKU_PROJECT_KEY: "",
		DATAIKU_DISABLE_ENV: "1",
	};
	delete env.DSS_CONFIG_DIR;

	try {
		const result = await dssRaw(["project", "list",], { cwd: root, env, },);
		expect(result.code,).toBe(1,);
		expect(result.stderr,).toBe("",);
		expect(JSON.parse(result.stdout,),).toMatchObject({
			code: "missing_required_flag",
			error: "Missing Dataiku URL.",
		},);
	} finally {
		await fs.rm(root, { recursive: true, force: true, },);
	}
});
