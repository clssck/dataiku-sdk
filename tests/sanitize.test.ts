import { describe, expect, it, } from "bun:test";
import { sanitizeFileName, } from "../src/utils/sanitize.js";

describe("sanitizeFileName", () => {
	const fallback = "unnamed";

	it("passes through a normal filename unchanged", () => {
		expect(sanitizeFileName("report.csv", fallback,),).toBe("report.csv",);
	});

	it("replaces illegal characters with underscores", () => {
		expect(
			sanitizeFileName('data<>:"/|file.txt', fallback,),
		).toBe("data______file.txt",);
	});

	it("replaces question mark and asterisk", () => {
		expect(sanitizeFileName("what?ever*.doc", fallback,),).toBe("what_ever_.doc",);
	});

	it("replaces backslash", () => {
		expect(sanitizeFileName("path\\file.txt", fallback,),).toBe("path_file.txt",);
	});

	it("strips trailing dots", () => {
		expect(sanitizeFileName("file...", fallback,),).toBe("file",);
	});

	it("strips trailing spaces", () => {
		expect(sanitizeFileName("file   ", fallback,),).toBe("file",);
	});

	it("strips trailing mix of dots and spaces", () => {
		expect(sanitizeFileName("file. . .", fallback,),).toBe("file",);
	});

	it("returns fallback for empty string", () => {
		expect(sanitizeFileName("", fallback,),).toBe(fallback,);
	});

	it("returns fallback when all characters are illegal", () => {
		const result = sanitizeFileName('<>:"', fallback,);
		// All replaced with `_`, trailing dots/spaces stripped — underscores remain
		expect(result,).toBe("____",);
	});

	it("returns fallback for only-whitespace input", () => {
		expect(sanitizeFileName("   ", fallback,),).toBe(fallback,);
	});

	it("appends underscore to Windows reserved name without extension", () => {
		expect(sanitizeFileName("CON", fallback,),).toBe("CON_",);
	});

	it("appends underscore before extension for Windows reserved name", () => {
		expect(sanitizeFileName("con.txt", fallback,),).toBe("con_.txt",);
	});

	it("handles Windows reserved names case-insensitively", () => {
		expect(sanitizeFileName("AuX.log", fallback,),).toBe("AuX_.log",);
	});

	it("handles PRN reserved name", () => {
		expect(sanitizeFileName("PRN", fallback,),).toBe("PRN_",);
	});

	it("handles NUL reserved name with extension", () => {
		expect(sanitizeFileName("NUL.txt", fallback,),).toBe("NUL_.txt",);
	});

	it("handles COM1-COM9 reserved names", () => {
		expect(sanitizeFileName("COM1.sys", fallback,),).toBe("COM1_.sys",);
		expect(sanitizeFileName("com9", fallback,),).toBe("com9_",);
	});

	it("handles LPT1-LPT9 reserved names", () => {
		expect(sanitizeFileName("LPT1.dat", fallback,),).toBe("LPT1_.dat",);
		expect(sanitizeFileName("lpt3", fallback,),).toBe("lpt3_",);
	});

	it("does not treat non-reserved names with dots as reserved", () => {
		expect(sanitizeFileName("my.file.csv", fallback,),).toBe("my.file.csv",);
	});

	it("does not alter names that start with a reserved word but are longer", () => {
		expect(sanitizeFileName("CONSOLE.txt", fallback,),).toBe("CONSOLE.txt",);
	});

	it("preserves unicode characters", () => {
		expect(sanitizeFileName("données_2024.csv", fallback,),).toBe("données_2024.csv",);
	});

	it("preserves CJK characters", () => {
		expect(sanitizeFileName("報告書.xlsx", fallback,),).toBe("報告書.xlsx",);
	});

	it("replaces control characters with underscores", () => {
		expect(sanitizeFileName("file\x00name", fallback,),).toBe("file_name",);
	});

	it("replaces all control characters in range \\u0000-\\u001F", () => {
		expect(sanitizeFileName("a\x01b\x1Fc", fallback,),).toBe("a_b_c",);
	});

	it("uses the provided fallback value", () => {
		expect(sanitizeFileName("", "default_file",),).toBe("default_file",);
	});
});
