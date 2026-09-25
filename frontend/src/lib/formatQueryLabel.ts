import overrides from '../queryLabels.json'

const BOOL_LABELS: Record<string, Record<string, string>> = {
  isDecommissioned: {
    true: 'decommissioned',
    false: 'not decommissioned',
  },
  isActive: {
    true: 'active',
    false: 'inactive',
  },
}

function formatParam(key: string, value: string): string {
  const normalized = value.toLowerCase()
  const map = BOOL_LABELS[key]
  if (map && normalized in map) {
    return map[normalized]
  }
  return `${key}=${value}`
}

export function formatQueryLabel(query: string): string {
  const override = (overrides as Record<string, string>)[query]
  if (override) {
    return override
  }
  if (!query.includes('=')) {
    return query
  }
  const parts = query.split('&').map((segment) => {
    const eq = segment.indexOf('=')
    if (eq === -1) {
      return segment
    }
    const key = segment.slice(0, eq)
    const value = segment.slice(eq + 1)
    return formatParam(key, value)
  })
  const label = parts.join(', ')
  return label.charAt(0).toUpperCase() + label.slice(1)
}
