import { describe, expect, it, } from "bun:test";
import { readFileSync, } from "node:fs";

// Textual scan of the workflow source (this repo has no YAML dependency).
// The file is small and structured: every release step starts at 6 spaces.
const workflow = readFileSync(
	new URL("../.github/workflows/release.yml", import.meta.url,),
	"utf8",
);
const lines = workflow.split("\n",);

/** Lines of the step block starting at `start` (up to the next step marker). */
function stepBlock(start: number,): string {
	const block: string[] = [lines[start],];
	for (let i = start + 1; i < lines.length; i++) {
		if (/^ {6}- /.test(lines[i],)) break;
		block.push(lines[i],);
	}
	return block.join("\n",);
}

function findStep(predicate: (line: string,) => boolean,): string {
	const idx = lines.findIndex(predicate,);
	expect(idx, "expected step not found in release.yml",).toBeGreaterThanOrEqual(0,);
	return stepBlock(idx,);
}

const thirdPartyUses = /^ {6}- uses: ([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+)@(\S+)/;
const fullSha = /^[0-9a-f]{40}$/;

describe(".github/workflows/release.yml", () => {
	it("pins every third-party action to a full commit SHA", () => {
		const refs = lines
			.map((line,) => thirdPartyUses.exec(line,))
			.filter((match,): match is RegExpExecArray => match !== null)
			.map((match,) => match[2]);
		expect(refs.length, "expected at least one third-party action",).toBeGreaterThan(0,);
		for (const ref of refs) {
			// Mutable tags (v4, v2) are not immutable code identities.
			expect(ref, `unpinned mutable ref in release.yml: ${ref}`,).toMatch(fullSha,);
		}
	});

	it("does not persist checkout credentials", () => {
		const checkout = findStep((line,) => line.includes("uses: actions/checkout@",));
		expect(checkout, "checkout step must not persist credentials",).toContain(
			"persist-credentials: false",
		);
		expect(checkout, "checkout step must not re-enable persisted credentials",).not.toContain(
			"persist-credentials: true",
		);
	});

	it("keeps the tag push able to authenticate without persisted credentials", () => {
		const push = findStep((line,) => line.includes("name: Push version tag",));
		expect(push, "push step must re-supply the token after persist-credentials: false",).toContain(
			"GITHUB_TOKEN: ${{ github.token }}",
		);
		expect(push, "push step must keep pushing tags",).toContain("git push --follow-tags",);
		expect(push, "push step must authenticate as x-access-token with the job token",).toContain(
			"x-access-token:${GITHUB_TOKEN}",
		);
	});
});
