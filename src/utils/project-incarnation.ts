import { asRecord, } from "./records.js";
import { stableHash, } from "./stable-hash.js";

/**
 * Fingerprint one concrete DSS project incarnation using only the immutable
 * creationTag. Mutable project metadata, including projectAppType and
 * versionTag, is intentionally excluded.
 */
export function projectIncarnationHash(
	projectKey: string,
	projectDetails: unknown,
): string | undefined {
	const details = asRecord(projectDetails,);
	if (!details) return undefined;
	const returnedKey = details["projectKey"];
	if (typeof returnedKey !== "string" || returnedKey.trim() !== projectKey) return undefined;
	const creationTag = asRecord(details["creationTag"],);
	if (!creationTag) return undefined;
	const lastModifiedOn = creationTag["lastModifiedOn"];
	if (
		!(typeof lastModifiedOn === "number" && Number.isFinite(lastModifiedOn,))
		&& !(typeof lastModifiedOn === "string" && lastModifiedOn.trim().length > 0)
	) return undefined;
	return stableHash({ projectKey, creationTag, },);
}
