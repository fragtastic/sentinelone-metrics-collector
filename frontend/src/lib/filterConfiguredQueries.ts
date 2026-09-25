export function filterRowsByConfiguredQueries<T extends { query: string }>(
  rows: T[],
  configuredQueries: string[] | undefined,
): T[] {
  if (configuredQueries === undefined) {
    return []
  }
  const allowed = new Set(configuredQueries)
  return rows.filter((row) => allowed.has(row.query))
}

export function isConfiguredQuery(query: string, configuredQueries: string[] | undefined): boolean {
  if (configuredQueries === undefined) {
    return false
  }
  return configuredQueries.includes(query)
}
