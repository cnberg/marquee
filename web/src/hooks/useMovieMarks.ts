import { useEffect, useState } from 'react'
import { api } from '../api/client'
import { useAuth } from '../auth/AuthContext'

export type MarkState = { want: boolean; watched: boolean; favorite: boolean }
export type MarkType = 'want' | 'watched' | 'favorite'

const EMPTY: MarkState = { want: false, watched: false, favorite: false }

/**
 * Manages mark state for a list of movies. Caller is responsible for passing
 * a stable list (memoize movieIds with useMemo). Returns the current mark
 * map and a `toggle` handler that performs optimistic updates and rolls
 * back on failure. The toggle resolves to the latest server-confirmed
 * MarkState (or null on error) so callers can react to side effects such
 * as removing the movie from a tab-filtered list.
 */
export function useMovieMarks(movieIds: number[]) {
  const { user, showAuthModal } = useAuth()
  const [marks, setMarks] = useState<Record<number, MarkState>>({})

  const idsKey = movieIds.join(',')

  useEffect(() => {
    if (!user) {
      setMarks({})
      return
    }
    if (movieIds.length === 0) return
    api
      .batchMarks(movieIds)
      .then((res) => setMarks(res))
      .catch(() => { /* ignore */ })
    // idsKey is the stable signature of movieIds; intentionally not depending on movieIds itself
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user?.id, idsKey])

  const toggle = async (movieId: number, markType: MarkType): Promise<MarkState | null> => {
    if (!user) {
      showAuthModal()
      return null
    }
    const prev = marks[movieId] ?? EMPTY
    const isActive = prev[markType]
    const optimistic = { ...prev, [markType]: !isActive }
    if (!isActive && markType === 'want') optimistic.watched = false
    if (!isActive && markType === 'watched') optimistic.want = false
    setMarks((m) => ({ ...m, [movieId]: optimistic }))
    try {
      const res = isActive
        ? await api.removeMark(movieId, markType)
        : await api.setMark(movieId, markType)
      setMarks((m) => ({ ...m, [movieId]: res }))
      return res
    } catch {
      setMarks((m) => ({ ...m, [movieId]: prev }))
      return null
    }
  }

  return { marks, toggle }
}
