import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { useMemo, useState } from 'react'
import { fetchDailyMax, fetchLatest } from '../api/client'
import { useConfiguredQueries } from '../hooks/useConfiguredQueries'
import { latestSnapshotPerQuery, pivotDailyMax } from '../lib/chartData'
import { filterRowsByConfiguredQueries } from '../lib/filterConfiguredQueries'
import { formatQueryLabel } from '../lib/formatQueryLabel'
import { MultiSeriesLineChart } from '../components/MultiSeriesLineChart'

const DAY_OPTIONS = [7, 14, 30] as const

export function Dashboard() {
  const [days, setDays] = useState<number>(30)
  const configuredQueries = useConfiguredQueries()

  const latestQuery = useQuery({
    queryKey: ['metrics', 'latest'],
    queryFn: () => fetchLatest(200),
  })

  const dailyQuery = useQuery({
    queryKey: ['metrics', 'daily-max', days],
    queryFn: () => fetchDailyMax(days),
  })

  const snapshot = useMemo(() => {
    if (!latestQuery.data) {
      return []
    }
    const filtered = filterRowsByConfiguredQueries(latestQuery.data, configuredQueries.data)
    return latestSnapshotPerQuery(filtered)
  }, [latestQuery.data, configuredQueries.data])

  const chart = useMemo(() => {
    if (!dailyQuery.data) {
      return { queries: [], points: [] }
    }
    return pivotDailyMax(filterRowsByConfiguredQueries(dailyQuery.data, configuredQueries.data))
  }, [dailyQuery.data, configuredQueries.data])

  const metricsLoading =
    configuredQueries.isLoading || latestQuery.isLoading || dailyQuery.isLoading

  return (
    <div className="space-y-8">
      <section>
        <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold text-white">Dashboard</h1>
            <p className="mt-1 text-sm text-slate-400">
              Daily max agent counts per query (aligned with SentinelOne usage semantics). Only
              queries listed in <code className="text-slate-300">queries.json</code> are shown.
            </p>
          </div>
          <label className="flex items-center gap-2 text-sm text-slate-300">
            Days
            <select
              value={days}
              onChange={(e) => setDays(Number(e.target.value))}
              className="rounded border border-slate-600 bg-slate-800 px-2 py-1 text-white"
            >
              {DAY_OPTIONS.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          </label>
        </div>

        {configuredQueries.error && (
          <p className="text-red-300">
            Could not load queries.json:{' '}
            {configuredQueries.error instanceof Error
              ? configuredQueries.error.message
              : 'unknown error'}
          </p>
        )}
        {metricsLoading && <p className="text-slate-400">Loading…</p>}
        {dailyQuery.error && (
          <p className="text-red-300">
            {dailyQuery.error instanceof Error ? dailyQuery.error.message : 'Failed to load chart'}
          </p>
        )}
        {chart.points.length > 0 && (
          <MultiSeriesLineChart
            title={`Daily max (last ${days} days)`}
            queries={chart.queries}
            points={chart.points}
          />
        )}
        {dailyQuery.isSuccess && chart.points.length === 0 && (
          <p className="text-slate-400">No daily max data yet.</p>
        )}
      </section>

      <section>
        <h2 className="mb-3 text-lg font-medium text-white">Latest snapshot</h2>
        {latestQuery.isLoading && <p className="text-slate-400">Loading latest metrics…</p>}
        {latestQuery.error && (
          <p className="text-red-300">
            {latestQuery.error instanceof Error ? latestQuery.error.message : 'Failed to load latest'}
          </p>
        )}
        {snapshot.length > 0 && (
          <div className="overflow-x-auto rounded-lg border border-slate-700">
            <table className="min-w-full text-left text-sm">
              <thead className="bg-slate-800/80 text-slate-300">
                <tr>
                  <th className="px-3 py-2 font-medium">Query</th>
                  <th className="px-3 py-2 font-medium">Count</th>
                  <th className="px-3 py-2 font-medium">Timestamp (UTC)</th>
                  <th className="px-3 py-2 font-medium" />
                </tr>
              </thead>
              <tbody>
                {snapshot.map((row) => (
                  <tr key={row.query} className="border-t border-slate-700/80">
                    <td className="px-3 py-2 text-slate-200" title={row.query}>
                      {formatQueryLabel(row.query)}
                    </td>
                    <td className="px-3 py-2 font-mono text-sky-200">
                      {row.result === null ? '—' : row.result}
                    </td>
                    <td className="px-3 py-2 font-mono text-slate-400">{row.timestamp}</td>
                    <td className="px-3 py-2">
                      <Link
                        to={`/query/${encodeURIComponent(row.query)}`}
                        className="text-sky-400 hover:text-sky-300"
                      >
                        Detail
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  )
}
