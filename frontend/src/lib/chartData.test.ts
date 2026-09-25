import { describe, expect, it } from 'vitest'
import { pivotRawSamples } from './chartData'

describe('pivotRawSamples', () => {
  it('groups queries on shared timestamps', () => {
    const { queries, points } = pivotRawSamples([
      { timestamp: '2026-01-01T12:00:00Z', query: 'a', result: 1 },
      { timestamp: '2026-01-01T12:00:00Z', query: 'b', result: 2 },
      { timestamp: '2026-01-01T12:01:00Z', query: 'a', result: 3 },
    ])
    expect(queries).toEqual(['a', 'b'])
    expect(points).toHaveLength(2)
    expect(points[0].a).toBe(1)
    expect(points[0].b).toBe(2)
    expect(points[1].a).toBe(3)
  })
})
