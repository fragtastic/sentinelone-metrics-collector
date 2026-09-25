import { describe, expect, it } from 'vitest'
import { filterRowsByConfiguredQueries, isConfiguredQuery } from './filterConfiguredQueries'

describe('filterConfiguredQueries', () => {
  it('filters rows to configured query strings only', () => {
    const rows = [
      { query: 'a', result: 1 },
      { query: 'retired', result: 2 },
    ]
    expect(filterRowsByConfiguredQueries(rows, ['a'])).toEqual([{ query: 'a', result: 1 }])
  })

  it('isConfiguredQuery reflects membership', () => {
    expect(isConfiguredQuery('a', ['a', 'b'])).toBe(true)
    expect(isConfiguredQuery('z', ['a', 'b'])).toBe(false)
  })
})
