import type { ECharts } from 'echarts'

import { CHART_GRID_WITH_RIGHT_LEGEND } from './chartLegendOptions'

type LegendPageInfo = {
  pageNextDataIndex: number | null
  pagePrevDataIndex: number | null
}

/** Same indices ECharts uses for legend page up/down buttons. */
export function scrollTargetFromWheel(
  pageInfo: LegendPageInfo,
  deltaY: number,
): number | null {
  return deltaY > 0 ? pageInfo.pageNextDataIndex : pageInfo.pagePrevDataIndex
}

/** ECharts runtime API not exposed on the public `ECharts` type. */
type EChartsLegendInternals = {
  getModel(): { getComponent(type: string, index: number): unknown }
  getViewOfComponentModel(model: unknown): {
    _getPageInfo?: (legendModel: unknown) => LegendPageInfo
  }
}

function getLegendPageInfo(chart: ECharts): LegendPageInfo | null {
  const internal = chart as unknown as EChartsLegendInternals
  const legendModel = internal.getModel().getComponent('legend', 0)
  if (!legendModel) {
    return null
  }
  const view = internal.getViewOfComponentModel(legendModel)
  if (!view?._getPageInfo) {
    return null
  }
  return view._getPageInfo(legendModel)
}

/** Wheel over the right legend band dispatches ECharts legendScroll (scroll-type legend). */
export function bindLegendWheelScroll(chart: ECharts): () => void {
  const dom = chart.getDom()
  const legendBandPx = CHART_GRID_WITH_RIGHT_LEGEND.right

  const onWheel = (event: WheelEvent) => {
    const rect = dom.getBoundingClientRect()
    if (event.clientX < rect.right - legendBandPx) {
      return
    }

    const pageInfo = getLegendPageInfo(chart)
    if (!pageInfo) {
      return
    }

    const nextIndex = scrollTargetFromWheel(pageInfo, event.deltaY)
    if (nextIndex == null) {
      return
    }

    event.preventDefault()
    chart.dispatchAction({ type: 'legendScroll', scrollDataIndex: nextIndex })
  }

  dom.addEventListener('wheel', onWheel, { passive: false })
  return () => dom.removeEventListener('wheel', onWheel)
}
