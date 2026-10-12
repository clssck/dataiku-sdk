/**
 * Did-you-mean suggestions for `commands run --fields RESOURCE[.ACTION[.FIELD...]]`
 * selectors that name no resource or action. Pure data in, strings out.
 */

const MAX_SUGGESTIONS = 4;

/** Levenshtein distance with a single rolling row. */
function editDistance(a: string, b: string,): number {
	let previous = Array.from({ length: b.length + 1, }, (_, index,) => index,);
	for (let i = 1; i <= a.length; i++) {
		const current = [i,];
		for (let j = 1; j <= b.length; j++) {
			const cost = a[i - 1] === b[j - 1] ? 0 : 1;
			current.push(Math.min(previous[j]! + 1, current[j - 1]! + 1, previous[j - 1]! + cost,),);
		}
		previous = current;
	}
	return previous[b.length]!;
}

/** Names close to `name`: a typo (distance <= 2, <= 1 for short names) or one containing it. */
function closeNames(name: string, candidates: readonly string[],): string[] {
	const maxDistance = name.length >= 5 ? 2 : 1;
	return candidates.filter((candidate,) =>
		candidate !== name
		&& (editDistance(name, candidate,) <= maxDistance
			|| (name.length >= 3 && candidate.includes(name,)))
	);
}

/**
 * Selectors worth trying instead of an unknown `resource` / `resource.action`
 * selector, keeping any trailing `.field` path. For a known resource: the same
 * action name under other resources (`flow.map.usage` -> `project.map.usage`),
 * then close action names of that resource. For an unknown resource: close
 * resource names (keeping the action), else the action under other resources.
 *
 * @param actionsByResource resource -> action names (`commandActionSummary()`).
 */
export function suggestSelectors(
	selector: string,
	actionsByResource: Record<string, string[]>,
): string[] {
	const [resource = "", action, ...rest] = selector.split(".",);
	const suffix = rest.length > 0 ? `.${rest.join(".",)}` : "";
	const resources = Object.keys(actionsByResource,);
	const sameActionElsewhere = action === undefined
		? []
		: resources.filter((other,) => other !== resource && actionsByResource[other]!.includes(action,))
			.map((
				other,
			) => `${other}.${action}${suffix}`);
	let suggestions: string[];
	if (Object.hasOwn(actionsByResource, resource,)) {
		const closeActions = action === undefined
			? []
			: closeNames(action, actionsByResource[resource]!,).map((close,) =>
				`${resource}.${close}${suffix}`
			);
		suggestions = [...sameActionElsewhere, ...closeActions,];
	} else {
		const tail = action === undefined ? "" : `.${[action, ...rest,].join(".",)}`;
		const closeResources = closeNames(resource, resources,).map((close,) => `${close}${tail}`);
		suggestions = closeResources.length > 0 ? closeResources : sameActionElsewhere;
	}
	return [...new Set(suggestions,),].slice(0, MAX_SUGGESTIONS,);
}
