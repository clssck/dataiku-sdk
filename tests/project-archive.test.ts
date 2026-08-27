import { afterEach, beforeEach, describe, expect, it, } from "bun:test";
import { mkdtemp, rm, writeFile, } from "node:fs/promises";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { crc32, } from "node:zlib";
import { ClientValidationError, } from "../src/errors.js";
import {
	assertImportableProjectArchive,
	inspectProjectArchive,
} from "../src/utils/project-archive.js";

// ---------------------------------------------------------------------------
// In-memory ZIP builder: store-method entries with data-descriptor records
// (general-purpose bit 3), so the utility's manual CRC-32 path is exercised.
// ---------------------------------------------------------------------------

interface TestMember {
	name: string;
	data: Uint8Array;
	crc?: number;
	size?: number;
}

function u16le(value: number,): Uint8Array {
	const out = new Uint8Array(2,);
	new DataView(out.buffer,).setUint16(0, value, true,);
	return out;
}

function u32le(value: number,): Uint8Array {
	const out = new Uint8Array(4,);
	new DataView(out.buffer,).setUint32(0, value >>> 0, true,);
	return out;
}

function concatBytes(...parts: Uint8Array[]): Uint8Array {
	const total = parts.reduce((sum, part,) => sum + part.length, 0,);
	const out = new Uint8Array(total,);
	let offset = 0;
	for (const part of parts) {
		out.set(part, offset,);
		offset += part.length;
	}
	return out;
}

function encodeUtf8(text: string,): Uint8Array {
	return new TextEncoder().encode(text,);
}

const SIG_LOCAL = 0x04034b50;
const SIG_CENTRAL = 0x02014b50;
const SIG_DATA_DESCRIPTOR = 0x08074b50;
const SIG_EOCD = 0x06054b50;
const FLAG_DATA_DESCRIPTOR = 0x0008;

function buildZip(members: TestMember[],): Uint8Array {
	const locals: Uint8Array[] = [];
	const centrals: Uint8Array[] = [];
	let offset = 0;
	for (const member of members) {
		const name = encodeUtf8(member.name,);
		const declaredCrc = member.crc ?? crc32(member.data,);
		const declaredSize = member.size ?? member.data.length;
		locals.push(
			concatBytes(
				u32le(SIG_LOCAL,),
				u16le(20,), // version needed
				u16le(FLAG_DATA_DESCRIPTOR,),
				u16le(0,), // method: store
				u16le(0,), // mod time
				u16le(0,), // mod date
				u32le(0,), // crc unknown until descriptor
				u32le(0,), // compressed size unknown
				u32le(0,), // uncompressed size unknown
				u16le(name.length,),
				u16le(0,), // extra length
				name,
				member.data,
				u32le(SIG_DATA_DESCRIPTOR,),
				u32le(declaredCrc >>> 0,),
				u32le(member.data.length,),
				u32le(declaredSize >>> 0,),
			),
		);
		centrals.push(
			concatBytes(
				u32le(SIG_CENTRAL,),
				u16le(20,), // version made by
				u16le(20,), // version needed
				u16le(FLAG_DATA_DESCRIPTOR,),
				u16le(0,), // method: store
				u16le(0,), // mod time
				u16le(0,), // mod date
				u32le(declaredCrc >>> 0,),
				u32le(member.data.length,),
				u32le(declaredSize >>> 0,),
				u16le(name.length,),
				u16le(0,), // extra length
				u16le(0,), // comment length
				u16le(0,), // disk start
				u16le(0,), // internal attributes
				u32le(0,), // external attributes
				u32le(offset,),
				name,
			),
		);
		offset += locals[locals.length - 1].length;
	}
	const central = concatBytes(...centrals,);
	return concatBytes(
		...locals,
		central,
		concatBytes(
			u32le(SIG_EOCD,),
			u16le(0,),
			u16le(0,),
			u16le(members.length,),
			u16le(members.length,),
			u32le(central.length,),
			u32le(offset,),
			u16le(0,),
		),
	);
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

function manifestJson(overrides: Record<string, unknown> = {},): Uint8Array {
	return encodeUtf8(JSON.stringify({
		manifestVersion: 3,
		originalProjectKey: "TEST_PROJECT",
		actualContent: { includedDatasets: [], },
		exportedWithOptions: {},
		...overrides,
	},),);
}

const VALID_MEMBERS: TestMember[] = [
	{
		name: "export-manifest.json",
		data: manifestJson({
			actualContent: {
				includedDatasets: [
					{ name: "ds_one", size: 4, type: "S3", usedTransferMethod: "DATA_BUNDLE", },
					{ name: "ds_two", size: 3, type: "UploadedFiles", usedTransferMethod: "UPLOAD", },
				],
			},
		},),
	},
	{ name: "project_config/datasets/ds_one.json", data: encodeUtf8("{}",), },
	{ name: "project_config/datasets/ds_two.json", data: encodeUtf8("{}",), },
	{
		name: "project_config/recipes/r1.json",
		data: encodeUtf8(
			JSON.stringify({
				inputs: { main: { items: [{ deps: [], ref: "ds_one", },], }, },
				outputs: { main: { items: [{ ref: "ds_two", },], }, },
			},),
		),
	},
	{ name: "project_config/recipes/r1.grouping", data: encodeUtf8("# payload",), },
	{ name: "any_datasets_data/ds_one/out-s0.csv", data: encodeUtf8("a,b\n1,2\n",), },
	{ name: "uploads/ds_two/input.csv", data: encodeUtf8("x\n",), },
];

let tempDir = "";

beforeEach(async () => {
	tempDir = await mkdtemp(join(tmpdir(), "project-archive-test-",),);
},);

afterEach(async () => {
	await rm(tempDir, { recursive: true, force: true, },);
},);

async function writeZip(members: TestMember[], name = "project.zip",): Promise<string> {
	const filePath = join(tempDir, name,);
	await writeFile(filePath, buildZip(members,),);
	return filePath;
}

function issueCodes(report: Awaited<ReturnType<typeof inspectProjectArchive>>,): string[] {
	return report.issues.map((issue,) => issue.code);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("inspectProjectArchive", () => {
	it("reports a complete valid archive with projects and members", async () => {
		const filePath = await writeZip(VALID_MEMBERS,);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(true,);
		expect(report.sizeBytes,).toBeGreaterThan(0,);
		expect(report.memberCount,).toBe(VALID_MEMBERS.length,);
		expect(report.sourceProjectKey,).toBe("TEST_PROJECT",);
		expect(report.issues,).toEqual([],);
	});

	it("assertImportableProjectArchive returns the report for a valid archive", async () => {
		const filePath = await writeZip(VALID_MEMBERS,);
		const report = await assertImportableProjectArchive(filePath,);
		expect(report.valid,).toBe(true,);
	});

	it("flags a member whose declared CRC does not match its data", async () => {
		const filePath = await writeZip([
			{ name: "export-manifest.json", data: manifestJson(), },
			{
				name: "project_config/datasets/ds_one.json",
				data: encodeUtf8('{"name":"ds_one"}',),
				crc: 0xdeadbeef,
			},
		],);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(false,);
		expect(issueCodes(report,),).toContain("member_crc_mismatch",);
		const crcIssue = report.issues.find((issue,) => issue.code === "member_crc_mismatch");
		expect(crcIssue?.member,).toBe("project_config/datasets/ds_one.json",);
		expect(crcIssue?.message,).toContain("CRC-32",);
		await expect(assertImportableProjectArchive(filePath,),).rejects.toThrow(
			ClientValidationError,
		);
		await expect(assertImportableProjectArchive(filePath,),).rejects.toMatchObject({
			code: "validation_failed",
		},);
	});

	it("flags members whose declared size does not match their data", async () => {
		const filePath = await writeZip([
			{ name: "export-manifest.json", data: manifestJson(), },
			{
				name: "project_config/datasets/wide.json",
				data: encodeUtf8('{"wide":true}',),
				size: 500,
			},
		],);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(false,);
		expect(issueCodes(report,),).toContain("member_size_mismatch",);
	});

	it("flags duplicate central-directory member names", async () => {
		const filePath = await writeZip([
			{ name: "export-manifest.json", data: manifestJson(), },
			{ name: "project_config/datasets/dup.json", data: encodeUtf8("{a:1}",), },
			{ name: "project_config/datasets/dup.json", data: encodeUtf8("{b:2}",), },
		],);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(false,);
		expect(report.memberCount,).toBe(3,);
		const dupIssue = report.issues.find((issue,) => issue.code === "duplicate_member_name");
		expect(dupIssue?.member,).toBe("project_config/datasets/dup.json",);
		expect(dupIssue?.message,).toContain("2 times",);
	});

	it("flags unsafe member paths without extracting them", async () => {
		const filePath = await writeZip([
			{ name: "export-manifest.json", data: manifestJson(), },
			{ name: "../escape.txt", data: encodeUtf8("evil",), },
			{ name: "/abs/escape.txt", data: encodeUtf8("evil",), },
			{ name: "sub/../../escape.txt", data: encodeUtf8("evil",), },
			{ name: "win\\..\\escape.txt", data: encodeUtf8("evil",), },
		],);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(false,);
		const paths = report.issues
			.filter((issue,) => issue.code === "unsafe_member_path")
			.map((issue,) => issue.member);
		expect(paths,).toEqual([
			"../escape.txt",
			"/abs/escape.txt",
			"sub/../../escape.txt",
			"win\\..\\escape.txt",
		],);
	});

	it("flags an archive without export-manifest.json", async () => {
		const filePath = await writeZip([
			{ name: "project_config/tags.json", data: encodeUtf8("{}",), },
		],);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(false,);
		expect(issueCodes(report,),).toContain("manifest_missing",);
	});

	it("flags a manifest that is not valid JSON", async () => {
		const filePath = await writeZip([
			{ name: "export-manifest.json", data: encodeUtf8("not json{",), },
		],);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(false,);
		expect(issueCodes(report,),).toContain("manifest_invalid_json",);
	});

	it("flags a manifest without a source project key", async () => {
		const filePath = await writeZip([
			{ name: "export-manifest.json", data: manifestJson({ originalProjectKey: undefined, },), },
		],);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(false,);
		expect(issueCodes(report,),).toContain("manifest_project_key_missing",);
		expect(report.sourceProjectKey,).toBeUndefined();
	});

	it("rejects empty and non-regular archive paths", async () => {
		const emptyPath = join(tempDir, "empty.zip",);
		await writeFile(emptyPath, new Uint8Array(0,),);
		const emptyReport = await inspectProjectArchive(emptyPath,);
		expect(emptyReport.valid,).toBe(false,);
		expect(issueCodes(emptyReport,),).toContain("archive_empty",);

		const dirReport = await inspectProjectArchive(tempDir,);
		expect(dirReport.valid,).toBe(false,);
		expect(issueCodes(dirReport,),).toContain("archive_not_regular_file",);

		const missingReport = await inspectProjectArchive(join(tempDir, "missing.zip",),);
		expect(missingReport.valid,).toBe(false,);
		expect(issueCodes(missingReport,),).toContain("archive_not_regular_file",);
	});

	it("flags dangling recipe dataset references", async () => {
		const filePath = await writeZip([
			{ name: "export-manifest.json", data: manifestJson(), },
			{ name: "project_config/datasets/existing.json", data: encodeUtf8("{}",), },
			{
				name: "project_config/recipes/produce.json",
				data: encodeUtf8(
					JSON.stringify({
						inputs: { left: { items: [{ ref: "existing", },], }, },
						outputs: { main: { items: [{ ref: "vanished", },], }, },
					},),
				),
			},
		],);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(false,);
		expect(issueCodes(report,),).toContain("dataset_reference_unresolved",);
		const refIssue = report.issues.find(
			(issue,) => issue.code === "dataset_reference_unresolved",
		);
		expect(refIssue?.member,).toBe("project_config/recipes/produce.json",);
		expect(refIssue?.message,).toContain("vanished",);
	});

	it("flags orphaned bundled-data roots and recipe payloads", async () => {
		const filePath = await writeZip([
			{ name: "export-manifest.json", data: manifestJson(), },
			{ name: "any_datasets_data/ghost/out-s0.csv", data: encodeUtf8("a,b\n1,2\n",), },
			{ name: "project_config/recipes/orphaned.shaker", data: encodeUtf8("# payload",), },
		],);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(false,);
		const orphans = report.issues
			.filter((issue,) => issue.code === "orphan_member")
			.map((issue,) => issue.member);
		expect(orphans,).toEqual([
			"any_datasets_data/ghost",
			"project_config/recipes/orphaned.shaker",
		],);
	});

	it("accepts bundled data anchored by the manifest even without definitions", async () => {
		const filePath = await writeZip([
			{
				name: "export-manifest.json",
				data: manifestJson({
					actualContent: {
						includedDatasets: [
							{ name: "bundled_only", size: 4, type: "S3", usedTransferMethod: "DATA_BUNDLE", },
						],
					},
				},),
			},
			{ name: "any_datasets_data/bundled_only/out-s0.csv", data: encodeUtf8("a,b\n1,2\n",), },
		],);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(true,);
		expect(report.issues,).toEqual([],);
	});

	it("reports identity, history and machine-specific residue only as redacted warnings", async () => {
		const filePath = await writeZip([
			{ name: "export-manifest.json", data: manifestJson(), },
			{
				name: "project_config/datasets/contacted.json",
				data: encodeUtf8(JSON.stringify({
					apiKey: "sk-live-1234567890abcdef1234",
					owner: "diku@example.info",
					email: "diku@example.info",
					createdBy: "sam@example.info",
					localDir: "/Users/diku/secret/lab",
				},),),
			},
		],);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(true,);
		const residueIssues = report.issues.filter((issue,) => issue.code === "identity_residue");
		expect(residueIssues,).toHaveLength(1,);
		expect(residueIssues[0].member,).toBe("project_config/datasets/contacted.json",);
		const serialized = JSON.stringify(report.issues,);
		expect(serialized,).not.toContain("sk-live-1234567890abcdef1234",);
		expect(serialized,).not.toContain("diku@example.info",);
		expect(serialized,).not.toContain("secret/lab",);
		expect(serialized,).not.toContain("sam@example.info",);
		expect(residueIssues[0].message,).toMatch(/residue pattern/,);
		expect(residueIssues[0].message,).toMatch(/credential-like/,);
		expect(residueIssues[0].message,).toMatch(/email-like/,);
		expect(residueIssues[0].message,).toMatch(/local-path-like/,);
		expect(residueIssues[0].message,).toMatch(/identity metadata field-like/,);
	});

	it("produces deterministic sorted issues across repeated inspections", async () => {
		const filePath = await writeZip([
			{ name: "export-manifest.json", data: manifestJson(), },
			{ name: "project_config/datasets/dup.json", data: encodeUtf8("{}",), },
			{ name: "project_config/datasets/dup.json", data: encodeUtf8("{}",), },
			{ name: "../escape.txt", data: encodeUtf8("evil",), },
			{
				name: "project_config/datasets/corrupt.json",
				data: encodeUtf8("{}",),
				crc: 0x12345678,
			},
		],);
		const first = await inspectProjectArchive(filePath,);
		const second = await inspectProjectArchive(filePath,);

		expect(first.issues,).toEqual(second.issues,);
		expect(first.issues,).toHaveLength(3,);
		expect(first.issues.map((issue,) => issue.code),).toEqual([
			"duplicate_member_name",
			"member_crc_mismatch",
			"unsafe_member_path",
		],);
	});

	it("streams oversized manifest members through integrity checks without buffering them", async () => {
		const members: TestMember[] = [
			{
				name: "export-manifest.json",
				data: concatBytes(manifestJson(), new Uint8Array(17 * 1024 * 1024,),),
				size: 17 * 1024 * 1024 + 30,
			},
		];
		const filePath = await writeZip(members,);
		const report = await inspectProjectArchive(filePath,);

		expect(report.valid,).toBe(false,);
		expect(issueCodes(report,),).toContain("manifest_too_large",);
	});
});
