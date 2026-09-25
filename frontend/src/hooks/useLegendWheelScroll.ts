import type { ECharts } from 'echarts'
import { useCallback, useEffect, useRef } from 'react'
import { bindLegendWheelScroll } from '../lib/legendWheelScroll'

export function useLegendWheelScroll() {
  const cleanupRef = useRef<(() => void) | null>(null)

  const onChartReady = useCallback((chart: ECharts) => {
    cleanupRef.current?.()
    cleanupRef.current = bindLegendWheelScroll(chart)
  }, [])

  useEffect(
    () => () => {
      cleanupRef.current?.()
      cleanupRef.current = null
    },
    [],
  )

  return { onChartReady }
}
