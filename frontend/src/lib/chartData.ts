export type TimeSeriesPoint = Record<string, string | number | null> & { time: string }

/** UTC minute label for charts (browser-only; does not change API data). */
export function truncateIsoToMinuteUtc(iso: string): string {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) {
    const match = iso.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})/)
    return match ? match[1] : iso
  }
  const y = d.getUTCFullYear()
  const m = String(d.getUTCMonth() + 1).padStart(2, '0')
  const day = String(d.getUTCDate()).padStart(2, '0')
  const h = String(d.getUTCHours()).padStart(2, '0')
  const min = String(d.getUTCMinutes()).padStart(2, '0')
  return `${y}-${m}-${day}T${h}:${min}`
}

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

export type PivotRawSamplesOptions = {
  /** Bucket chart x-axis to UTC minutes (last sample wins within the minute). */
  displayMinuteBuckets?: boolean
}

export function pivotRawSamples(
  rows: { timestamp: string; query: string; result: number | null }[],
  queryFilter?: string,
  options?: PivotRawSamplesOptions,
): { queries: string[]; points: TimeSeriesPoint[] } {
  const filtered = queryFilter ? rows.filter((r) => r.query === queryFilter) : rows
  const queries = [...new Set(filtered.map((r) => r.query))].sort()
  const byTime = new Map<string, TimeSeriesPoint>()
  for (const row of filtered) {
    const timeKey = options?.displayMinuteBuckets
      ? truncateIsoToMinuteUtc(row.timestamp)
      : row.timestamp
    let point = byTime.get(timeKey)
    if (!point) {
      point = { time: timeKey }
      byTime.set(timeKey, point)
    }
    point[row.query] = row.result
  }
  const points = [...byTime.values()].sort((a, b) => String(a.time).localeCompare(String(b.time)))
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
