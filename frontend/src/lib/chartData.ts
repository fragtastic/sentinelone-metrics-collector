export type TimeSeriesPoint = Record<string, string | number | null> & { time: string }

export function pivotDailyMax(
  rows: { day: string; query: string; max_result: number | null }[],
): { queries: string[]; points: TimeSeriesPoint[] } {
  const queries = [...new Set(rows.map((r) => r.query))].sort()
  const byDay = new Map<string, TimeSeriesPoint>()
  for (const row of rows) {
    let point = byDay.get(row.day)
    if (!point) {
      point = { time: row.day }
      byDay.set(row.day, point)
    }
    point[row.query] = row.max_result
  }
  const points = [...byDay.values()].sort((a, b) => String(a.time).localeCompare(String(b.time)))
  return { queries, points }
}

export function pivotHourlyMax(
  rows: { hour: string; query: string; max_result: number | null }[],
  queryFilter?: string,
): { queries: string[]; points: TimeSeriesPoint[] } {
  const filtered = queryFilter ? rows.filter((r) => r.query === queryFilter) : rows
  const queries = [...new Set(filtered.map((r) => r.query))].sort()
  const byHour = new Map<string, TimeSeriesPoint>()
  for (const row of filtered) {
    let point = byHour.get(row.hour)
    if (!point) {
      point = { time: row.hour }
      byHour.set(row.hour, point)
    }
    point[row.query] = row.max_result
  }
  const points = [...byHour.values()].sort((a, b) => String(a.time).localeCompare(String(b.time)))
  return { queries, points }
}

export function latestSnapshotPerQuery(
  rows: { timestamp: string; query: string; result: number | null }[],
): { timestamp: string; query: string; result: number | null }[] {
  const byQuery = new Map<string, { timestamp: string; query: string; result: number | null }>()
  for (const row of rows) {
    const existing = byQuery.get(row.query)
    if (!existing || row.timestamp > existing.timestamp) {
      byQuery.set(row.query, row)
    }
  }
  return [...byQuery.values()].sort((a, b) => a.query.localeCompare(b.query))
}
