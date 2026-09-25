import { useQuery } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import { fetchHourlyMax } from '../api/client'
import { MultiSeriesLineChart } from '../components/MultiSeriesLineChart'
import { pivotHourlyMax } from '../lib/chartData'
import { RECENT_HOUR_OPTIONS } from '../lib/recentHoursOptions'

export function HourlyMaxRecent() {
  const [hours, setHours] = useState<number>(24)

  const hourlyQuery = useQuery({
    queryKey: ['metrics', 'hourly-max', 'hours', hours],
    queryFn: () => fetchHourlyMax({ hours }),
  })

  const chart = useMemo(
    () =>
      hourlyQuery.data ? pivotHourlyMax(hourlyQuery.data) : { queries: [], points: [] },
    [hourlyQuery.data],
  )

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-white">Hourly max (recent)</h1>
          <p className="mt-1 text-sm text-slate-400">
            Maximum agent count per clock hour over the last N hours (UTC buckets).
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

      {hourlyQuery.isLoading && <p className="text-slate-400">Loading hourly max…</p>}
      {hourlyQuery.error && (
        <p className="text-red-300">
          {hourlyQuery.error instanceof Error ? hourlyQuery.error.message : 'Failed to load'}
        </p>
      )}
      {chart.points.length > 0 && (
        <MultiSeriesLineChart
          title={`Hourly max — last ${hours} hours`}
          queries={chart.queries}
          points={chart.points}
        />
      )}
      {hourlyQuery.isSuccess && chart.points.length === 0 && (
        <p className="text-slate-400">No hourly data in this window.</p>
      )}
    </div>
  )
}
