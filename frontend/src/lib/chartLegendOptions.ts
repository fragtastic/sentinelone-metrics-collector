import type { EChartsOption } from 'echarts'

/** Built-in ECharts legend: vertical, scrollable, on the right (toggle series on click). */
export function verticalScrollLegend(
  width = 200,
  selected?: Record<string, boolean>,
): EChartsOption['legend'] {
  return {
    type: 'scroll',
    orient: 'vertical',
    right: 0,
    top: 48,
    bottom: 24,
    width,
    textStyle: {
      color: '#cbd5e1',
      width: width - 28,
      overflow: 'break',
    },
    pageIconColor: '#94a3b8',
    pageTextStyle: { color: '#94a3b8' },
    tooltip: { show: true },
    ...(selected ? { selected } : {}),
  }
}

export const CHART_GRID_WITH_RIGHT_LEGEND = {
  left: 48,
  right: 216,
  top: 48,
  bottom: 32,
} as const
