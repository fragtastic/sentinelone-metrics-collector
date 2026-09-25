const PALETTE = [
  '#4e9af1',
  '#6bcB77',
  '#f4a261',
  '#e76f51',
  '#a78bfa',
  '#2dd4bf',
  '#f472b6',
  '#facc15',
]

function hashString(value: string): number {
  let hash = 0
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(i)
    hash |= 0
  }
  return Math.abs(hash)
}

export function colorForQuery(query: string): string {
  return PALETTE[hashString(query) % PALETTE.length]
}
