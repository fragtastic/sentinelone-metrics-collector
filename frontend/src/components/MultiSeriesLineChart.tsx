import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import { useMemo } from 'react'
import { usePersistedLegendSelection } from '../hooks/usePersistedLegendSelection'
import { colorForQuery } from '../lib/queryColors'
import { formatQueryLabel } from '../lib/formatQueryLabel'
import type { TimeSeriesPoint } from '../lib/chartData'
import { CHART_GRID_WITH_RIGHT_LEGEND, verticalScrollLegend } from '../lib/chartLegendOptions'

type Props = {
  title: string
  queries: string[]
  points: TimeSeriesPoint[]
  timeKey?: string
  height?: number
}

export function MultiSeriesLineChart({
  title,
  queries,
  points,
  timeKey = 'time',
  height = 360,
}: Props) {
  const seriesNames = useMemo(() => queries.map((query) => formatQueryLabel(query)), [queries])
  const { legendSelected, legendChartEvents } = usePersistedLegendSelection(seriesNames)

  const times = points.map((p) => String(p[timeKey]))
  const series = queries.map((query) => ({
    name: formatQueryLabel(query),
    type: 'line' as const,
    connectNulls: false,
    showSymbol: false,
    smooth: true,
    itemStyle: { color: colorForQuery(query) },
    data: points.map((p) => (p[query] === undefined ? null : (p[query] as number | null))),
  }))

  const option: EChartsOption = {
    backgroundColor: 'transparent',
    title: {
      text: title,
      left: 0,
      textStyle: { color: '#e7ecf1', fontSize: 16, fontWeight: 600 },
    },
    tooltip: { trigger: 'axis' },
    legend: verticalScrollLegend(200, legendSelected),
    grid: { ...CHART_GRID_WITH_RIGHT_LEGEND },
    xAxis: {
      type: 'category',
      data: times,
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
    series,
  }

  return (
    <ReactECharts
      option={option}
      style={{ height }}
      notMerge
      lazyUpdate
      onEvents={legendChartEvents}
    />
  )
}
