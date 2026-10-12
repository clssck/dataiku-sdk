import * as fs from "node:fs";

/**
 * Minimal deterministic ZIP builder for archive tests. Emits STORED entries
 * (no compression) with correct CRCs, so any real zip reader — and the
 * project-archive utility's fatal checks — must accept the output.
 */
const CRC_TABLE: Uint32Array = (() => {
	const table = new Uint32Array(256,);
	for (let n = 0; n < 256; n++) {
		let c = n;
		for (let k = 0; k < 8; k++) {
			c = (c & 1) !== 0 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
		}
		table[n] = c >>> 0;
	}
	return table;
})();

function crc32(data: Uint8Array,): number {
	let c = 0xffffffff;
	for (let i = 0; i < data.length; i++) {
		c = CRC_TABLE[(c ^ data[i]!) & 0xff]! ^ (c >>> 8);
	}
	return (c ^ 0xffffffff) >>> 0;
}

export function buildStoredZip(files: Record<string, string | Uint8Array>,): Uint8Array {
	const encoder = new TextEncoder();
	const localParts: Uint8Array[] = [];
	const centralParts: Uint8Array[] = [];
	const entries: Array<{ name: Uint8Array; crc: number; size: number; offset: number; }> = [];
	let offset = 0;
	let cdSize = 0;

	for (const [name, content,] of Object.entries(files,)) {
		const nameBytes = encoder.encode(name,);
		const data = typeof content === "string" ? encoder.encode(content,) : content;
		const crc = crc32(data,);

		const local = new Uint8Array(30,);
		const localView = new DataView(local.buffer,);
		localView.setUint32(0, 0x04034b50, true,);
		localView.setUint16(4, 20, true,);
		localView.setUint16(6, 0, true,);
		localView.setUint16(8, 0, true,);
		localView.setUint16(10, 0, true,);
		localView.setUint16(12, 0x5821, true,);
		localView.setUint32(14, crc, true,);
		localView.setUint32(18, data.length, true,);
		localView.setUint32(22, data.length, true,);
		localView.setUint16(26, nameBytes.length, true,);
		localView.setUint16(28, 0, true,);

		localParts.push(local, nameBytes, data,);
		entries.push({ name: nameBytes, crc, size: data.length, offset, },);
		offset += local.length + nameBytes.length + data.length;
	}

	for (const entry of entries) {
		const central = new Uint8Array(46,);
		const centralView = new DataView(central.buffer,);
		centralView.setUint32(0, 0x02014b50, true,);
		centralView.setUint16(4, 20, true,);
		centralView.setUint16(6, 20, true,);
		centralView.setUint16(8, 0, true,);
		centralView.setUint16(10, 0, true,);
		centralView.setUint16(12, 0, true,);
		centralView.setUint16(14, 0x5821, true,);
		centralView.setUint32(16, entry.crc, true,);
		centralView.setUint32(20, entry.size, true,);
		centralView.setUint32(24, entry.size, true,);
		centralView.setUint16(28, entry.name.length, true,);
		centralView.setUint16(30, 0, true,);
		centralView.setUint16(32, 0, true,);
		centralView.setUint16(34, 0, true,);
		centralView.setUint16(36, 0, true,);
		// Offset 38 is the external-attributes field; the local-header offset is at 42.
		centralView.setUint32(42, entry.offset, true,);
		centralParts.push(central, entry.name,);
		cdSize += central.length + entry.name.length;
	}

	const eocd = new Uint8Array(22,);
	const eocdView = new DataView(eocd.buffer,);
	eocdView.setUint32(0, 0x06054b50, true,);
	eocdView.setUint16(4, 0, true,);
	eocdView.setUint16(6, 0, true,);
	eocdView.setUint16(8, entries.length, true,);
	eocdView.setUint16(10, entries.length, true,);
	eocdView.setUint32(12, cdSize, true,);
	eocdView.setUint32(16, offset, true,);
	eocdView.setUint16(20, 0, true,);

	const total = offset + cdSize + eocd.length;
	const out = new Uint8Array(total,);
	let pos = 0;
	for (const part of [...localParts, ...centralParts, eocd,]) {
		out.set(part, pos,);
		pos += part.length;
	}
	return out;
}

/**
 * Write a project export archive that passes every fatal check of
 * `assertImportableProjectArchive`: exactly one member, a valid
 * `export-manifest.json` with a non-empty `originalProjectKey`, no recipes,
 * no datasets, and no `any_datasets_data/`/`uploads/` roots.
 */
export function writeProjectArchive(filePath: string, originalProjectKey: string,): void {
	const zip = buildStoredZip({
		"export-manifest.json": JSON.stringify({ originalProjectKey, },),
	},);
	fs.writeFileSync(filePath, Buffer.from(zip,),);
}

/** Secret values embedded by {@link credentialBearingArchive}; tests assert none reaches CLI output. */
export const ARCHIVE_FIXTURE_SECRETS = [
	"sk-live-FIXTURE-APIKEYS-0001",
	"fixture-dhl-variable-secret-2",
	"fixture-webapp-config-secret-3",
] as const;

/**
 * A project export archive that passes every fatal check but carries the
 * credential structures DSS exports verbatim: `project_config/apikeys.json`,
 * a secret-named project variable and a nested web-app `apiKey`.
 */
export function credentialBearingArchive(originalProjectKey: string,): Uint8Array {
	const [apiKey, variable, webAppKey,] = ARCHIVE_FIXTURE_SECRETS;
	return buildStoredZip({
		"export-manifest.json": JSON.stringify({ originalProjectKey, },),
		"project_config/apikeys.json": JSON.stringify([{ name: "DHL", key: apiKey, },],),
		"project_config/variables.json": JSON.stringify({
			standard: { DHL_API_KEY: variable, PAGE_SIZE: "50", },
			local: {},
		},),
		"project_config/web_apps/app1.json": JSON.stringify({
			name: "app1",
			config: { service: { apiKey: webAppKey, }, keyColumn: "customer_id", },
		},),
	},);
}
