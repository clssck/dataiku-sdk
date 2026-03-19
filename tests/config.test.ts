import { afterEach, beforeEach, describe, expect, it, } from "bun:test";
import { existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import {
	deleteCredentials,
	type DssCredentials,
	getConfigDir,
	getCredentialsPath,
	loadCredentials,
	maskApiKey,
	saveCredentials,
} from "../src/config.js";

describe("getConfigDir", () => {
	const orig = { ...process.env, };

	afterEach(() => {
		process.env.DSS_CONFIG_DIR = orig.DSS_CONFIG_DIR;
		process.env.XDG_CONFIG_HOME = orig.XDG_CONFIG_HOME;
	},);

	it("uses DSS_CONFIG_DIR when set", () => {
		process.env.DSS_CONFIG_DIR = "/tmp/custom-dss";
		expect(getConfigDir(),).toBe("/tmp/custom-dss",);
	});

	it("uses XDG_CONFIG_HOME when set", () => {
		delete process.env.DSS_CONFIG_DIR;
		process.env.XDG_CONFIG_HOME = "/tmp/xdg";
		expect(getConfigDir(),).toBe(join("/tmp/xdg", "dataiku",),);
	});

	it("defaults to ~/.config/dataiku", () => {
		delete process.env.DSS_CONFIG_DIR;
		delete process.env.XDG_CONFIG_HOME;
		const dir = getConfigDir();
		expect(dir,).toContain(".config",);
		expect(dir,).toEndWith("dataiku",);
	});
});

describe("credentials CRUD", () => {
	let tmpDir: string;
	const origConfigDir = process.env.DSS_CONFIG_DIR;

	beforeEach(() => {
		tmpDir = join(
			tmpdir(),
			`dss-config-test-${Date.now()}-${Math.random().toString(36,).slice(2,)}`,
		);
		mkdirSync(tmpDir, { recursive: true, },);
		process.env.DSS_CONFIG_DIR = tmpDir;
	},);

	afterEach(() => {
		process.env.DSS_CONFIG_DIR = origConfigDir;
		rmSync(tmpDir, { recursive: true, force: true, },);
	},);

	it("loadCredentials returns null when no file exists", () => {
		expect(loadCredentials(),).toBeNull();
	});

	it("saveCredentials writes and loadCredentials reads back", () => {
		const creds: DssCredentials = { url: "https://dss.example.com", apiKey: "dkuaps-test123", };
		saveCredentials(creds,);
		const loaded = loadCredentials();
		expect(loaded,).not.toBeNull();
		expect(loaded!.url,).toBe("https://dss.example.com",);
		expect(loaded!.apiKey,).toBe("dkuaps-test123",);
		expect(loaded!.projectKey,).toBeUndefined();
	});

	it("saveCredentials includes projectKey when provided", () => {
		const creds: DssCredentials = {
			url: "https://dss.example.com",
			apiKey: "dkuaps-test123",
			projectKey: "PROJ",
		};
		saveCredentials(creds,);
		const loaded = loadCredentials();
		expect(loaded,).not.toBeNull();
		expect(loaded!.projectKey,).toBe("PROJ",);
	});

	it("saveCredentials sets file permissions to 0o600 on Unix", () => {
		if (process.platform === "win32") return;
		saveCredentials({ url: "https://dss.example.com", apiKey: "key", },);
		const stat = statSync(getCredentialsPath(),);
		expect(stat.mode & 0o777,).toBe(0o600,);
	});

	it("saveCredentials writes valid JSON with 2-space indent and trailing newline", () => {
		saveCredentials({ url: "https://dss.example.com", apiKey: "key", },);
		const raw = readFileSync(getCredentialsPath(), "utf-8",);
		expect(raw,).toEndWith("\n",);
		const parsed = JSON.parse(raw,);
		expect(parsed.url,).toBe("https://dss.example.com",);
		// Verify 2-space indent by checking the raw text
		expect(raw,).toContain('  "url"',);
	});

	it("deleteCredentials removes the file", () => {
		saveCredentials({ url: "https://dss.example.com", apiKey: "key", },);
		expect(existsSync(getCredentialsPath(),),).toBe(true,);
		deleteCredentials();
		expect(loadCredentials(),).toBeNull();
	});

	it("deleteCredentials is idempotent (no error if missing)", () => {
		expect(() => deleteCredentials()).not.toThrow();
	});

	it("saveCredentials creates intermediate directories", () => {
		process.env.DSS_CONFIG_DIR = join(tmpDir, "a", "b", "c",);
		saveCredentials({ url: "https://dss.example.com", apiKey: "key", },);
		expect(existsSync(getCredentialsPath(),),).toBe(true,);
	});

	it("loadCredentials returns null for malformed JSON (missing url)", () => {
		writeFileSync(getCredentialsPath(), JSON.stringify({ apiKey: "key", },), "utf-8",);
		expect(loadCredentials(),).toBeNull();
	});

	it("loadCredentials returns null for non-object JSON", () => {
		writeFileSync(getCredentialsPath(), "[]", "utf-8",);
		expect(loadCredentials(),).toBeNull();
	});
});

describe("maskApiKey", () => {
	it("masks long keys showing first 6 and last 6 chars", () => {
		const masked = maskApiKey("dkuaps-abc123xyz789",);
		expect(masked,).toBe("dkuaps...xyz789",);
	});

	it("returns *** for short keys", () => {
		expect(maskApiKey("short",),).toBe("***",);
	});

	it("returns *** for empty string", () => {
		expect(maskApiKey("",),).toBe("***",);
	});

	it("returns *** for keys with exactly 12 chars", () => {
		expect(maskApiKey("123456789012",),).toBe("***",);
	});

	it("masks 13-char key correctly", () => {
		const masked = maskApiKey("1234567890123",);
		expect(masked,).toBe("123456...890123",);
	});
});
