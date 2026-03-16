export function normalizeQuery(query: string | undefined,): string | undefined {
	const normalized = query?.trim().toLowerCase();
	return normalized && normalized.length > 0 ? normalized : undefined;
}

export function filterByQuery<T,>(
	items: T[],
	query: string | undefined,
	pickValues: (item: T,) => Array<string | undefined>,
): T[] {
	const normalized = normalizeQuery(query,);
	if (!normalized) return items;

	return items.filter((item,) =>
		pickValues(item,).some((value,) => (value ?? "").toLowerCase().includes(normalized,))
	);
}

const DEFAULT_PAGE_LIMIT = 100;

export function paginateItems<T,>(
	items: T[],
	limit: number | undefined,
	offset: number | undefined,
	defaultLimit = DEFAULT_PAGE_LIMIT,
): { items: T[]; offset: number; limit: number; hasMore: boolean; } {
	const pageOffset = Math.max(0, offset ?? 0,);
	const effectiveLimit = Math.max(1, limit ?? defaultLimit,);
	const pagedItems = items.slice(pageOffset, pageOffset + effectiveLimit,);
	const hasMore = pageOffset + pagedItems.length < items.length;
	return {
		items: pagedItems,
		offset: pageOffset,
		limit: effectiveLimit,
		hasMore,
	};
}
