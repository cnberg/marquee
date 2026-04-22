import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import MovieCard from '../components/MovieCard'
import { api } from '../api/client'
import type { Movie, Person } from '../types'
import { useMovieMarks } from '../hooks/useMovieMarks'
import { useLocale } from '../i18n/LocaleContext'
import { Badge } from '../components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card'
import { Alert, AlertDescription } from '../components/ui/alert'
import { Button } from '../components/ui/button'
import { Skeleton } from '../components/ui/skeleton'

const TYPE_LABEL_KEYS: Record<string, string> = {
  director: 'browse_director',
  cast: 'browse_cast',
  genre: 'browse_genre',
  keyword: 'browse_keyword',
  decade: 'browse_decade',
  country: 'browse_country',
  language: 'browse_language',
}

export default function Browse() {
  const { t } = useLocale()
  const [searchParams] = useSearchParams()
  const type = searchParams.get('type') || ''
  const value = searchParams.get('value') || ''
  const name = searchParams.get('name') || value
  const personId = searchParams.get('person_id')

  const [movies, setMovies] = useState<Movie[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [person, setPerson] = useState<Person | null>(null)
  const [personLoading, setPersonLoading] = useState(false)

  const [page, setPage] = useState(1)
  const perPage = 40

  const typeLabel = TYPE_LABEL_KEYS[type] ? t(TYPE_LABEL_KEYS[type]) : type

  useEffect(() => {
    document.title = `${typeLabel}：${name} - Marquee`
  }, [typeLabel, name])

  useEffect(() => {
    if ((type === 'director' || type === 'cast') && personId) {
        setPersonLoading(true)
        api.getPerson(Number(personId))
          .then(setPerson)
          .catch(() => setPerson(null))
          .finally(() => setPersonLoading(false))
    } else {
        setPerson(null)
    }
  }, [type, personId])

  useEffect(() => {
    setLoading(true)
    setError(null)

    let request: Promise<any>

    if ((type === 'director' || type === 'cast') && personId) {
      request = api.getPersonMovies(Number(personId), {
        role: type,
        page: String(page),
        per_page: String(perPage),
      })
    } else {
      const params: Record<string, string> = {
        page: String(page),
        per_page: String(perPage),
      }
      if (type === 'decade') params.decade = value
      else if (type === 'genre') params.genre = value
      else if (type === 'keyword') params.keyword = value
      else if (type === 'country') params.country = value
      else if (type === 'language') params.language = value
      else if (type === 'director') params.director = value
      else if (type === 'cast') params.cast = value

      request = api.listMovies(params)
    }

    request
      .then((res: any) => {
        setMovies(Array.isArray(res.data) ? res.data : [])
        setTotal(res.total ?? 0)
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [type, value, personId, page])

  const totalPages = useMemo(() => Math.ceil(total / perPage), [total])

  const movieIds = useMemo(() => movies.map((m) => m.id), [movies])
  const { marks, toggle } = useMovieMarks(movieIds)

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-2">
        <div className="flex items-center gap-2">
          <Badge variant="secondary">{typeLabel}</Badge>
          <h1 className="text-2xl font-semibold leading-tight">{name}</h1>
        </div>
        {total > 0 && (
          <p className="text-sm text-muted-foreground">
            {t('browse_total', { count: total })}
          </p>
        )}
      </div>

      {(type === 'director' || type === 'cast') && personId && (
        personLoading ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Skeleton className="h-12 w-12 rounded-full" />
            <div className="space-y-2">
              <Skeleton className="h-4 w-32" />
              <Skeleton className="h-3 w-64" />
            </div>
          </div>
        ) : person ? (
          <Card>
            <CardHeader className="flex flex-row items-start gap-4 space-y-0">
              {person.profile_url ? (
                <img
                  src={person.profile_url}
                  alt={person.name}
                  className="h-20 w-20 rounded-full object-cover"
                />
              ) : (
                <div className="h-20 w-20 rounded-full bg-muted" />
              )}
              <div className="space-y-2">
                <CardTitle>{person.name}</CardTitle>
                <div className="flex flex-wrap gap-2 text-sm text-muted-foreground">
                  {person.birthday && <span>{person.birthday}</span>}
                  {person.deathday && <span> — {person.deathday}</span>}
                  {person.place_of_birth && <span> · {person.place_of_birth}</span>}
                </div>
                {person.also_known_as.length > 0 && (
                  <div className="text-sm text-muted-foreground">
                    {person.also_known_as.join('、')}
                  </div>
                )}
              </div>
            </CardHeader>
            {person.biography && (
              <CardContent>
                <p className="text-sm leading-relaxed text-muted-foreground whitespace-pre-line">
                  {person.biography}
                </p>
              </CardContent>
            )}
          </Card>
        ) : null
      )}

      {loading && (
        <div className="flex items-center justify-center rounded-lg border bg-card px-4 py-8 text-sm text-muted-foreground">
          {t('browse_loading')}
        </div>
      )}

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {!loading && !error && movies.length === 0 && (
        <div className="rounded-lg border border-dashed bg-muted/30 px-4 py-10 text-center text-sm text-muted-foreground">
          {t('browse_empty')}
        </div>
      )}

      {!loading && movies.length > 0 && (
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5">
          {movies.map((movie) => (
            <MovieCard
              key={movie.id}
              movie={movie}
              marks={marks[movie.id]}
              onToggleMark={(mt) => { toggle(movie.id, mt) }}
            />
          ))}
        </div>
      )}

      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-3">
          <Button
            variant="outline"
            size="sm"
            disabled={page <= 1}
            onClick={() => setPage(page - 1)}
          >
            {t('browse_prev')}
          </Button>
          <span className="text-sm text-muted-foreground">
            {page} / {totalPages}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= totalPages}
            onClick={() => setPage(page + 1)}
          >
            {t('browse_next')}
          </Button>
        </div>
      )}
    </div>
  )
}
