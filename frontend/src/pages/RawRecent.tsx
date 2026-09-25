import { useQuery } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import { fetchRaw } from '../api/client'
import { useConfiguredQueries } from '../hooks/useConfiguredQueries'
import { MultiSeriesLineChart } from '../components/MultiSeriesLineChart'
import { pivotRawSamples } from '../lib/chartData'
import { filterRowsByConfiguredQueries } from '../lib/filterConfiguredQueries'
import { RECENT_HOUR_OPTIONS } from '../lib/recentHoursOptions'

export function RawRecent() {
  const [hours, setHours] = useState<number>(24)
  const configuredQueries = useConfiguredQueries()

  const rawQuery = useQuery({
    queryKey: ['metrics', 'raw', hours],
    queryFn: () => fetchRaw(hours),
  })

  const chart = useMemo(() => {
    if (!rawQuery.data) {
      return { queries: [], points: [] }
    }
    const filtered = filterRowsByConfiguredQueries(rawQuery.data, configuredQueries.data)
    return pivotRawSamples(filtered, undefined, { displayMinuteBuckets: true })
  }, [rawQuery.data, configuredQueries.data])

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-white">Raw counts (recent)</h1>
          <p className="mt-1 text-sm text-slate-400">
            Each collection sample on the timeline (x-axis shown to the UTC minute). Values are
            the agent counts returned for each query at collect time.
          </p>
        </div>
        <label className="flex items-center gap-2 text-sm text-slate-300">
          Last
          <select
            value={hours}
            onChange={(e) => setHours(Number(e.target.value))}
            className="rounded border border-slate-600 bg-slate-800 px-2 py-1 text-white"
          >
            {RECENT_HOUR_OPTIONS.map((h) => (
              <option key={h} value={h}>
                {h} hours
              </option>
            ))}
          </select>
        </label>
      </div>

      {rawQuery.isLoading && <p className="text-slate-400">Loading raw samples…</p>}
      {rawQuery.error && (
        <p className="text-red-300">
          {rawQuery.error instanceof Error ? rawQuery.error.message : 'Failed to load'}
        </p>
      )}
      {chart.points.length > 0 && (
        <MultiSeriesLineChart
          title={`Collection samples — last ${hours} hours`}
          queries={chart.queries}
          points={chart.points}
        />
      )}
      {rawQuery.isSuccess && chart.points.length === 0 && (
        <p className="text-slate-400">No samples in this window.</p>
      )}
    </div>
  )
}
