/**
 * Whether a Bun version string is at least `minimum`, comparing only the
 * release part (major.minor.patch). Canary builds print e.g.
 * `1.4.3-canary.12+abc`; npm-style semver ranges (and Bun.semver) reject every
 * prerelease, which would lock out anyone on a canary channel.
 */
export function versionAtLeast(actual, minimum,) {
	const a = actual.split(/[-+]/,)[0].split(".",).map(Number,);
	const m = minimum.split(".",).map(Number,);
	for (let i = 0; i < m.length; i++) {
		if ((a[i] ?? 0) !== m[i]) return (a[i] ?? 0) > m[i];
	}
	return true;
}
