import { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../api/client'
import MovieCard from '../components/MovieCard'
import { useAuth } from '../auth/AuthContext'
import { useLocale } from '../i18n/LocaleContext'
import type { Movie } from '../types'
import { useMovieMarks, type MarkType as ToggleMarkType } from '../hooks/useMovieMarks'
import { Tabs, TabsList, TabsTrigger } from '../components/ui/tabs'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../components/ui/select'
import { Alert, AlertDescription } from '../components/ui/alert'
import { Badge } from '../components/ui/badge'

type MarkType = 'watched' | 'want' | 'favorite'
type SortKey = 'marked' | 'year' | 'rating'

export default function MyMarks() {
  const { t } = useLocale()
  const { user, showAuthModal } = useAuth()
  const navigate = useNavigate()
  const [activeTab, setActiveTab] = useState<MarkType>('watched')
  const [sortKey, setSortKey] = useState<SortKey>('marked')
  const [cache, setCache] = useState<Record<MarkType, Movie[] | undefined>>({
    watched: undefined,
    want: undefined,
    favorite: undefined,
  })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => { document.title = t('marks_title') }, [t])

  useEffect(() => {
    if (!user) { showAuthModal(); navigate('/'); return }
  }, [user?.id])

  useEffect(() => {
    if (!user) return
    if (cache[activeTab] !== undefined) return
    setLoading(true)
    api.listMarkedMovies(activeTab)
      .then((movies) => setCache((c) => ({ ...c, [activeTab]: movies })))
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [user?.id, activeTab])

  const currentMovies = cache[activeTab] ?? []

  const sorted = useMemo(() => {
    const arr = [...currentMovies]
    if (sortKey === 'marked') {
      return arr
    }
    if (sortKey === 'year') {
      return arr.sort((a, b) => (b.year ?? 0) - (a.year ?? 0))
    }
    return arr.sort((a, b) => (b.tmdb_rating ?? 0) - (a.tmdb_rating ?? 0))
  }, [currentMovies, sortKey])

  const sortedIds = useMemo(() => sorted.map((m) => m.id), [sorted])
  const { marks, toggle } = useMovieMarks(sortedIds)

  const handleToggle = async (movieId: number, markType: ToggleMarkType) => {
    const result = await toggle(movieId, markType)
    if (!result) return
    if (!result[activeTab]) {
      setCache((c) => ({
        ...c,
        [activeTab]: c[activeTab]?.filter((m) => m.id !== movieId),
      }))
    }
  }

  if (!user) return null

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <h1 className="text-2xl font-semibold">{t('marks_title')}</h1>
        <div className="flex items-center gap-3">
          <span className="text-sm text-muted-foreground">{t('marks_sort_label')}</span>
          <Select value={sortKey} onValueChange={(v) => setSortKey(v as SortKey)}>
            <SelectTrigger className="w-44">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="marked">{t('marks_sort_marked')}</SelectItem>
              <SelectItem value="year">{t('marks_sort_year')}</SelectItem>
              <SelectItem value="rating">{t('marks_sort_rating')}</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as MarkType)}>
        <TabsList className="grid w-full grid-cols-3 sm:w-auto">
          {(['watched', 'want', 'favorite'] as MarkType[]).map((type) => (
            <TabsTrigger key={type} value={type} className="flex items-center gap-2">
              <span>{t(`marks_tab_${type}`)}</span>
              {cache[type] !== undefined && (
                <Badge variant="secondary" className="text-xs">
                  {cache[type]!.length}
                </Badge>
              )}
            </TabsTrigger>
          ))}
        </TabsList>
      </Tabs>

      {loading && (
        <div className="rounded-lg border bg-card px-4 py-6 text-sm text-muted-foreground">
          {t('common_loading')}
        </div>
      )}

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {!loading && sorted.length === 0 && (
        <div className="rounded-lg border border-dashed bg-muted/20 px-4 py-10 text-center text-sm text-muted-foreground">
          {t('marks_empty')}
        </div>
      )}

      {sorted.length > 0 && (
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5">
          {sorted.map((movie) => (
            <MovieCard
              key={movie.id}
              movie={movie}
              marks={marks[movie.id]}
              onToggleMark={(mt) => { handleToggle(movie.id, mt) }}
            />
          ))}
        </div>
      )}
    </div>
  )
}
