import { useQuery } from '@tanstack/react-query'
import { fetchQueries } from '../api/client'

export function useConfiguredQueries() {
  return useQuery({
    queryKey: ['metrics', 'queries'],
    queryFn: fetchQueries,
    staleTime: 5 * 60_000,
  })
}
