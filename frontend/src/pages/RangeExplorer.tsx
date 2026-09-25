import { useQuery } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import { fetchRange } from '../api/client'
import { useConfiguredQueries } from '../hooks/useConfiguredQueries'
import { usePersistedLegendSelection } from '../hooks/usePersistedLegendSelection'
import { CHART_GRID_WITH_RIGHT_LEGEND, verticalScrollLegend } from '../lib/chartLegendOptions'
import { filterRowsByConfiguredQueries } from '../lib/filterConfiguredQueries'
import { formatQueryLabel } from '../lib/formatQueryLabel'
import { colorForQuery } from '../lib/queryColors'

const MAX_RANGE_DAYS = 31

function utcDateString(d: Date): string {
  return d.toISOString().slice(0, 10)
}

function daysBetween(from: string, to: string): number {
  const start = new Date(`${from}T00:00:00Z`).getTime()
  const end = new Date(`${to}T00:00:00Z`).getTime()
  return (end - start) / 86_400_000
}

export function RangeExplorer() {
  const [searchParams] = useSearchParams()
  const initialQuery = searchParams.get('query') ?? ''

  const [from, setFrom] = useState(() =>
    utcDateString(new Date(Date.now() - 7 * 86_400_000)),
  )
  const [to, setTo] = useState(() => utcDateString(new Date()))
  const [queryFilter, setQueryFilter] = useState(initialQuery)
  const [showBand, setShowBand] = useState(false)
  const [clientError, setClientError] = useState<string | null>(null)

  const configuredQueries = useConfiguredQueries()
  const queries = configuredQueries.data ?? []

  const rangeQuery = useQuery({
    queryKey: ['metrics', 'range', from, to, queryFilter, showBand],
    queryFn: () => fetchRange(from, to, { query: queryFilter || undefined }),
    enabled: false,
  })

  function runQuery() {
    setClientError(null)
    if (!from || !to) {
      setClientError('From and to dates are required.')
      return
    }
    if (to <= from) {
      setClientError('To date must be after from date.')
      return
    }
    if (daysBetween(from, to) > MAX_RANGE_DAYS) {
      setClientError(`Range cannot exceed ${MAX_RANGE_DAYS} days.`)
      return
    }
    void rangeQuery.refetch()
  }

  const rangeChartHeight = 400

  const rangeChartData = useMemo((): {
    series: EChartsOption['series']
    times: string[]
  } | null => {
    if (!rangeQuery.data?.length) {
      return null
    }
    const configuredRows = filterRowsByConfiguredQueries(
      rangeQuery.data,
      configuredQueries.data,
    )
    const rows = queryFilter
      ? configuredRows.filter((r) => r.query === queryFilter)
      : configuredRows
    const times = [...new Set(rows.map((r) => r.hour))].sort()
    const seriesQueries = [...new Set(rows.map((r) => r.query))].sort()

    const series: EChartsOption['series'] = []
    for (const q of seriesQueries) {
      const qRows = rows.filter((r) => r.query === q)
      const byHour = new Map(qRows.map((r) => [r.hour, r]))
      const color = colorForQuery(q)
      const maxData = times.map((t) => byHour.get(t)?.max_result ?? null)

      const maxLabel = `${formatQueryLabel(q)} (max)`
      series.push({
        name: maxLabel,
        type: 'line',
        connectNulls: false,
        showSymbol: false,
        smooth: true,
        itemStyle: { color },
        data: maxData,
      })
      if (showBand) {
        const minLabel = `${formatQueryLabel(q)} (min)`
        series.push({
          name: minLabel,
          type: 'line',
          connectNulls: false,
          showSymbol: false,
          lineStyle: { type: 'dashed', opacity: 0.5 },
          itemStyle: { color },
          data: times.map((t) => byHour.get(t)?.min_result ?? null),
        })
        const avgLabel = `${formatQueryLabel(q)} (avg)`
        series.push({
          name: avgLabel,
          type: 'line',
          connectNulls: false,
          showSymbol: false,
          lineStyle: { type: 'dotted', opacity: 0.6 },
          itemStyle: { color },
          data: times.map((t) => byHour.get(t)?.avg_result ?? null),
        })
      }
    }

    return { series, times }
  }, [rangeQuery.data, queryFilter, showBand, configuredQueries.data])

  const rangeSeriesNames = useMemo(() => {
    if (!rangeChartData?.series || !Array.isArray(rangeChartData.series)) {
      return []
    }
    return rangeChartData.series
      .map((item) =>
        typeof item === 'object' && item !== null && 'name' in item
          ? String(item.name)
          : '',
      )
      .filter(Boolean)
  }, [rangeChartData])

  const { legendSelected, legendChartEvents } = usePersistedLegendSelection(rangeSeriesNames)

  const rangeChartOption = useMemo((): EChartsOption | null => {
    if (!rangeChartData) {
      return null
    }
    return {
      backgroundColor: 'transparent',
      title: {
        text: 'Hourly aggregates',
        left: 0,
        textStyle: { color: '#e7ecf1', fontSize: 16, fontWeight: 600 },
      },
      tooltip: { trigger: 'axis' },
      legend: verticalScrollLegend(200, legendSelected),
      grid: { ...CHART_GRID_WITH_RIGHT_LEGEND },
      xAxis: {
        type: 'category',
        data: rangeChartData.times,
        axisLabel: {
          color: '#94a3b8',
          hideOverlap: true,
          formatter: (value: string) => (value.length > 16 ? `${value.slice(0, 16)}…` : value),
        },
        axisLine: { lineStyle: { color: '#475569' } },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: '#94a3b8' },
        splitLine: { lineStyle: { color: '#334155' } },
      },
      series: rangeChartData.series,
    }
  }, [rangeChartData, legendSelected])

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold text-white">Range explorer</h1>
        <p className="mt-1 text-sm text-slate-400">
          Hourly min / avg / max from <code className="text-slate-300">/metrics/range</code> (UTC
          dates).
        </p>
      </div>

      <div className="flex flex-wrap items-end gap-4 rounded-lg border border-slate-700 bg-slate-900/40 p-4">
        <label className="flex flex-col gap-1 text-sm text-slate-300">
          From
          <input
            type="date"
            value={from}
            onChange={(e) => setFrom(e.target.value)}
            className="rounded border border-slate-600 bg-slate-800 px-2 py-1 text-white"
          />
        </label>
        <label className="flex flex-col gap-1 text-sm text-slate-300">
          To
          <input
            type="date"
            value={to}
            onChange={(e) => setTo(e.target.value)}
            className="rounded border border-slate-600 bg-slate-800 px-2 py-1 text-white"
          />
        </label>
        <label className="flex flex-col gap-1 text-sm text-slate-300">
          Query filter
          <select
            value={queryFilter}
            onChange={(e) => setQueryFilter(e.target.value)}
            className="max-w-xs rounded border border-slate-600 bg-slate-800 px-2 py-1 text-white"
          >
            <option value="">All queries</option>
            {queries.map((q) => (
              <option key={q} value={q}>
                {formatQueryLabel(q)}
              </option>
            ))}
          </select>
        </label>
        <label className="flex items-center gap-2 pb-1 text-sm text-slate-300">
          <input
            type="checkbox"
            checked={showBand}
            onChange={(e) => setShowBand(e.target.checked)}
            className="rounded border-slate-600"
          />
          Show min / avg
        </label>
        <button
          type="button"
          onClick={runQuery}
          className="rounded bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-500"
        >
          Load range
        </button>
      </div>

      {(clientError || rangeQuery.error) && (
        <p className="text-red-300">
          {clientError ??
            (rangeQuery.error instanceof Error ? rangeQuery.error.message : 'Request failed')}
        </p>
      )}

      {rangeQuery.isFetching && <p className="text-slate-400">Loading range…</p>}

      {rangeChartOption && (
        <ReactECharts
          option={rangeChartOption}
          style={{ height: rangeChartHeight }}
          notMerge
          lazyUpdate
          onEvents={legendChartEvents}
        />
      )}
    </div>
  )
}
