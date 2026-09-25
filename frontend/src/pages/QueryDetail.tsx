import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from 'react-router-dom'
import { useMemo, useState } from 'react'
import { fetchHourlyMax } from '../api/client'
import { pivotHourlyMax } from '../lib/chartData'
import { formatQueryLabel } from '../lib/formatQueryLabel'
import { MultiSeriesLineChart } from '../components/MultiSeriesLineChart'

export function QueryDetail() {
  const { encodedQuery } = useParams()
  const query = encodedQuery ? decodeURIComponent(encodedQuery) : ''
  const [days, setDays] = useState(7)

  const hourlyQuery = useQuery({
    queryKey: ['metrics', 'hourly-max', days, query],
    queryFn: () => fetchHourlyMax({ days, query }),
    enabled: Boolean(query),
  })

  const chart = useMemo(
    () =>
      hourlyQuery.data
        ? pivotHourlyMax(hourlyQuery.data, query)
        : { queries: [], points: [] },
    [hourlyQuery.data, query],
  )

  if (!query) {
    return <p className="text-red-300">Missing query parameter.</p>
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-white">{formatQueryLabel(query)}</h1>
          <p className="mt-1 font-mono text-xs text-slate-500">{query}</p>
        </div>
        <label className="flex items-center gap-2 text-sm text-slate-300">
          Days
          <select
            value={days}
            onChange={(e) => setDays(Number(e.target.value))}
            className="rounded border border-slate-600 bg-slate-800 px-2 py-1 text-white"
          >
            {[7, 14, 30].map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </label>
      </div>

      {hourlyQuery.isLoading && <p className="text-slate-400">Loading hourly max…</p>}
      {hourlyQuery.error && (
        <p className="text-red-300">
          {hourlyQuery.error instanceof Error ? hourlyQuery.error.message : 'Failed to load'}
        </p>
      )}
      {chart.points.length > 0 && (
        <MultiSeriesLineChart
          title={`Hourly max (last ${days} days)`}
          queries={chart.queries}
          points={chart.points}
        />
      )}

      <Link
        to={`/explore?query=${encodeURIComponent(query)}`}
        className="inline-block text-sky-400 hover:text-sky-300"
      >
        Open in range explorer →
      </Link>
    </div>
  )
}
