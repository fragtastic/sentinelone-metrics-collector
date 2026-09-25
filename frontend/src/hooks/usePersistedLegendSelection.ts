import { useEffect, useMemo, useState } from 'react'

/**
 * Keeps ECharts legend show/hide state across option updates (e.g. time range changes).
 * Sync via legendselectchanged; merge into legend.selected on each render.
 */
export function usePersistedLegendSelection(seriesNames: string[]) {
  const [selected, setSelected] = useState<Record<string, boolean>>({})

  useEffect(() => {
    setSelected((prev) => {
      const next = { ...prev }
      for (const name of seriesNames) {
        if (!(name in next)) {
          next[name] = true
        }
      }
      return next
    })
  }, [seriesNames.join('\0')])

  const legendSelected = useMemo(
    () => Object.fromEntries(seriesNames.map((name) => [name, selected[name] !== false])),
    [seriesNames, selected],
  )

  const legendChartEvents = useMemo(
    () => ({
      legendselectchanged: (params: { selected?: Record<string, boolean> }) => {
        if (params.selected) {
          setSelected(params.selected)
        }
      },
    }),
    [],
  )

  return { legendSelected, legendChartEvents }
}
