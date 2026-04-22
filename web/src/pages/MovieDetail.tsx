import type { ReactNode } from 'react'
import { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api } from '../api/client'
import type { MovieDetail as MovieDetailType, MovieWatchProvider, MovieReleaseDate } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { useAuth } from '../auth/AuthContext'
import { MovieMarkButtons } from '../components/MovieMarkButtons'
import { Card } from '../components/ui/card'
import { Badge } from '../components/ui/badge'
import { Alert, AlertDescription } from '../components/ui/alert'
import { Button } from '../components/ui/button'
import { Separator } from '../components/ui/separator'
import { ChevronDown, ChevronUp, Copy } from 'lucide-react'

interface ParsedCastMember {
  name: string
  tmdbPersonId?: number
  character?: string
}

interface ParsedDirector {
  name: string
  tmdbPersonId?: number
}

function parseStringList(json?: string): string[] {
  if (!json) return []
  try {
    const parsed = JSON.parse(json)
    if (!Array.isArray(parsed)) return []

    return parsed
      .map((item) => {
        if (typeof item === 'string') return item
        if (item && typeof item === 'object') {
          if ('name' in item && typeof item.name === 'string') return item.name
          if ('title' in item && typeof item.title === 'string') return item.title
        }
        return null
      })
      .filter((value): value is string => Boolean(value))
  } catch {
    return []
  }
}

function parseCastList(json?: string): ParsedCastMember[] {
  if (!json) return []
  try {
    const parsed = JSON.parse(json)
    if (!Array.isArray(parsed)) return []
    return parsed
      .map((item: any) => {
        if (typeof item === 'string') return { name: item }
        if (item && typeof item === 'object' && 'name' in item) {
          return {
            name: item.name,
            tmdbPersonId: item.tmdb_person_id,
            character: item.character,
          }
        }
        return null
      })
      .filter((v): v is ParsedCastMember => v !== null)
  } catch {
    return []
  }
}

function parseDirectorInfo(json?: string): ParsedDirector[] {
  if (!json) return []
  try {
    const parsed = JSON.parse(json)
    if (!Array.isArray(parsed)) return []
    return parsed
      .map((item: any) => ({
        name: item.name,
        tmdbPersonId: item.tmdb_person_id,
      }))
      .filter((d) => d.name)
  } catch {
    return []
  }
}

function parseJsonArray(json?: string): any[] {
  if (!json) return []
  try {
    const parsed = JSON.parse(json)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

function parseJsonObject(json?: string): any | null {
  if (!json) return null
  try {
    const parsed = JSON.parse(json)
    return typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null
  } catch {
    return null
  }
}

function yearToDecade(year: number): string {
  return `${Math.floor(year / 10) * 10}s`
}

function formatMoney(amount?: number): string | null {
  if (!amount || amount === 0) return null
  if (amount >= 1_000_000) return `$${(amount / 1_000_000).toFixed(1)}M`
  if (amount >= 1_000) return `$${(amount / 1_000).toFixed(0)}K`
  return `$${amount}`
}

const TMDB_IMG = 'https://image.tmdb.org/t/p'

function CollapsibleSection({ title, defaultOpen = false, children }: { title: string; defaultOpen?: boolean; children: ReactNode }) {
  const [open, setOpen] = useState(defaultOpen)
  const { t } = useLocale()
  return (
    <Card className="overflow-hidden border bg-card/70 backdrop-blur">
      <button
        className="flex w-full items-center justify-between px-4 py-3 text-left transition hover:bg-accent/40"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
      >
        <span className="text-base font-semibold">{title}</span>
        <span className="flex items-center gap-1 text-sm text-muted-foreground">
          {open ? t('detail_show_less') : t('detail_show_more')}
          {open ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
        </span>
      </button>
      {open && <div className="px-4 pb-4">{children}</div>}
    </Card>
  )
}

export default function MovieDetail() {
  const { id } = useParams<{ id: string }>()
  const { t } = useLocale()
  const { user, showAuthModal } = useAuth()
  const [movie, setMovie] = useState<MovieDetailType | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [marks, setMarks] = useState({ want: false, watched: false, favorite: false })

  useEffect(() => {
    let cancelled = false

    const movieId = Number(id)
    if (!id || Number.isNaN(movieId)) {
      setError(t('detail_not_found'))
      setLoading(false)
      return () => { /* noop */ }
    }

    const loadMovie = async () => {
      setLoading(true)
      setError(null)
      try {
        const res = await api.getMovie(movieId)
        if (!cancelled) setMovie(res as MovieDetailType)
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : t('detail_load_error'))
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    loadMovie()
    return () => { cancelled = true }
  }, [id])

  useEffect(() => {
    const movieId = Number(id)
    if (!id || Number.isNaN(movieId)) {
      setMarks({ want: false, watched: false, favorite: false })
      return
    }
    api
      .getMarks(movieId)
      .then((res) => setMarks(res))
      .catch(() => setMarks({ want: false, watched: false, favorite: false }))
  }, [id, user?.id])

  useEffect(() => {
    if (movie) {
      const year = movie.year ? ` - ${movie.year}` : ''
      document.title = `${movie.title}${year} - ${t('detail_title_suffix')}`
    } else {
      document.title = t('detail_title_suffix')
    }
  }, [movie, t])

  const genres = useMemo(() => parseStringList(movie?.genres), [movie?.genres])
  const castMembers = useMemo(() => parseCastList(movie?.cast), [movie?.cast])
  const directorList = useMemo(() => {
    const fromInfo = parseDirectorInfo(movie?.director_info)
    if (fromInfo.length > 0) return fromInfo
    if (movie?.director) return [{ name: movie.director }]
    return []
  }, [movie?.director_info, movie?.director])
  const keywords = useMemo(() => parseStringList(movie?.keywords), [movie?.keywords])
  const llmTags = useMemo(() => parseStringList(movie?.llm_tags), [movie?.llm_tags])
  const showOriginalTitle = Boolean(movie?.original_title && movie.original_title !== movie.title)
  const tagline = movie?.tagline_en || movie?.tagline_zh
  const collection = useMemo(() => parseJsonObject(movie?.collection), [movie?.collection])
  const productionCompanies = useMemo(() => parseJsonArray(movie?.production_companies), [movie?.production_companies])
  const spokenLanguages = useMemo(() => parseJsonArray(movie?.spoken_languages), [movie?.spoken_languages])
  const originCountries = useMemo(() => parseJsonArray(movie?.origin_country), [movie?.origin_country])
  const dirPaths = movie?.dir_paths ?? []

  const budgetStr = formatMoney(movie?.budget)
  const revenueStr = formatMoney(movie?.revenue)

  // Sub-resources
  const credits = movie?.credits ?? []
  const castCredits = credits.filter(c => c.credit_type === 'cast').sort((a, b) => (a.order ?? 999) - (b.order ?? 999))
  const crewCredits = credits.filter(c => c.credit_type === 'crew')
  const images = movie?.images ?? []
  const backdrops = images.filter(i => i.image_type === 'backdrop')
  const posters = images.filter(i => i.image_type === 'poster')
  const videos = (movie?.videos ?? []).filter(v => v.site === 'YouTube')
  const reviews = movie?.reviews ?? []
  const similar = movie?.similar ?? []
  const recommendations = movie?.recommendations ?? []
  const watchProviders = movie?.watch_providers ?? []
  const releaseDates = movie?.release_dates ?? []
  const externalIds = movie?.external_ids
  const altTitles = movie?.alternative_titles ?? []
  const translations = movie?.translations ?? []
  const lists = movie?.lists ?? []

  // Group watch providers by country, then by type
  const wpByCountry = useMemo(() => {
    const map = new Map<string, { stream: MovieWatchProvider[]; rent: MovieWatchProvider[]; buy: MovieWatchProvider[] }>()
    for (const wp of watchProviders) {
      if (!map.has(wp.iso_3166_1)) map.set(wp.iso_3166_1, { stream: [], rent: [], buy: [] })
      const entry = map.get(wp.iso_3166_1)!
      if (wp.provider_type === 'flatrate') entry.stream.push(wp)
      else if (wp.provider_type === 'rent') entry.rent.push(wp)
      else if (wp.provider_type === 'buy') entry.buy.push(wp)
    }
    return map
  }, [watchProviders])

  // Group release dates by country
  const rdByCountry = useMemo(() => {
    const map = new Map<string, MovieReleaseDate[]>()
    for (const rd of releaseDates) {
      if (!map.has(rd.iso_3166_1)) map.set(rd.iso_3166_1, [])
      map.get(rd.iso_3166_1)!.push(rd)
    }
    return map
  }, [releaseDates])

  // Get certification from US or CN
  const certification = useMemo(() => {
    for (const country of ['US', 'CN', 'GB']) {
      const rds = rdByCountry.get(country)
      if (rds) {
        const cert = rds.find(r => r.certification)
        if (cert?.certification) return cert.certification
      }
    }
    return null
  }, [rdByCountry])

  const handleToggleMark = async (markType: 'want' | 'watched' | 'favorite') => {
    const movieId = Number(id)
    if (!movieId || Number.isNaN(movieId)) return
    if (!user) {
      showAuthModal()
      return
    }
    const prev = marks
    const isActive = prev[markType]
    const optimistic = { ...prev, [markType]: !isActive }
    if (!isActive && markType === 'want') optimistic.watched = false
    if (!isActive && markType === 'watched') optimistic.want = false
    setMarks(optimistic)
    try {
      const res = isActive ? await api.removeMark(movieId, markType) : await api.setMark(movieId, markType)
      setMarks(res)
    } catch {
      setMarks(prev)
    }
  }

  const backdropUrl = movie?.backdrop_path ? `${TMDB_IMG}/w1280${movie.backdrop_path}` : null

  return (
    <div className="space-y-6">
      <Link to="/" className="inline-flex items-center gap-2 text-sm text-muted-foreground transition hover:text-foreground">
        {t('detail_back')}
      </Link>

      {loading && (
        <div className="flex justify-center rounded-lg border bg-card px-4 py-8 text-sm text-muted-foreground">
          {t('detail_loading')}
        </div>
      )}

      {error && !loading && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {!loading && !error && movie && (
        <>
          {backdropUrl && (
            <div className="relative h-48 overflow-hidden rounded-xl border bg-gradient-to-r from-background via-background/70 to-background">
              <img src={backdropUrl} alt="" className="absolute inset-0 h-full w-full object-cover opacity-40" />
              <div className="absolute inset-0 bg-gradient-to-b from-background/30 via-background/70 to-background" />
            </div>
          )}

          <Card className="overflow-hidden border bg-card/80 backdrop-blur">
            <div className="grid gap-6 p-6 md:grid-cols-[220px,1fr]">
              <div className="flex justify-center">
                {movie.poster_url ? (
                  <img
                    src={movie.poster_url}
                    alt={movie.title}
                    width={220}
                    height={330}
                    className="h-full max-h-[420px] w-full max-w-[240px] rounded-lg border bg-muted/40 object-cover"
                  />
                ) : (
                  <div className="flex h-full max-h-[420px] w-full max-w-[240px] items-center justify-center rounded-lg border bg-muted/30 text-sm text-muted-foreground">
                    {t('card_no_poster')}
                  </div>
                )}
              </div>

              <div className="space-y-4">
                <div className="space-y-3">
                  <div className="space-y-2">
                    <h1 className="text-3xl font-semibold leading-tight">{movie.title}</h1>
                    {showOriginalTitle && <div className="text-sm text-muted-foreground">{movie.original_title}</div>}
                    {tagline && <div className="text-lg text-muted-foreground">{tagline}</div>}
                  </div>
                  <MovieMarkButtons movieId={movie.id} marks={marks} onToggle={handleToggleMark} size="lg" />
                  <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
                    {movie.year && (
                      <Link to={`/browse?type=decade&value=${yearToDecade(movie.year)}&name=${yearToDecade(movie.year)}`} className="hover:text-foreground">
                        {movie.year}
                      </Link>
                    )}
                    {movie.runtime && <span>{movie.runtime} {t('detail_minutes')}</span>}
                    {certification && <Badge variant="secondary">{certification}</Badge>}
                    {movie.country && (
                      <Link
                        to={`/browse?type=country&value=${encodeURIComponent(movie.country)}&name=${encodeURIComponent(movie.country)}`}
                        className="hover:text-foreground"
                      >
                        {movie.country}
                      </Link>
                    )}
                    {movie.language && (
                      <Link
                        to={`/browse?type=language&value=${encodeURIComponent(movie.language)}&name=${encodeURIComponent(movie.language)}`}
                        className="hover:text-foreground"
                      >
                        {movie.language}
                      </Link>
                    )}
                    {movie.status && movie.status !== 'Released' && (
                      <Badge variant="outline">{movie.status}</Badge>
                    )}
                  </div>
                </div>

                <div className="flex flex-wrap gap-3 text-sm">
                  {typeof movie.tmdb_rating === 'number' && (
                    <div className="rounded-lg border bg-background/60 px-3 py-2">
                      <div className="text-2xl font-semibold">{movie.tmdb_rating.toFixed(1)}</div>
                      <div className="text-muted-foreground">{t('detail_tmdb_rating')}</div>
                    </div>
                  )}
                  {typeof movie.tmdb_votes === 'number' && movie.tmdb_votes > 0 && (
                    <div className="rounded-lg border bg-background/60 px-3 py-2 text-muted-foreground">
                      {movie.tmdb_votes.toLocaleString()} {t('detail_votes_suffix')}
                    </div>
                  )}
                  {typeof movie.popularity === 'number' && movie.popularity > 0 && (
                    <div className="rounded-lg border bg-background/60 px-3 py-2 text-muted-foreground">
                      {t('detail_popularity')} {movie.popularity.toFixed(1)}
                    </div>
                  )}
                </div>

                {genres.length > 0 && (
                  <div className="flex flex-wrap gap-2">
                    {genres.map((genre) => (
                      <Link
                        key={genre}
                        to={`/browse?type=genre&value=${encodeURIComponent(genre)}&name=${encodeURIComponent(genre)}`}
                        className="hover:no-underline"
                      >
                        <Badge variant="outline">{genre}</Badge>
                      </Link>
                    ))}
                  </div>
                )}

                <div className="space-y-2 text-sm">
                  {collection && collection.name && (
                    <div className="flex flex-wrap gap-2">
                      <span className="text-muted-foreground">{t('detail_collection')}:</span>
                      <span>{collection.name}</span>
                    </div>
                  )}

                  <div className="flex flex-wrap gap-2">
                    <span className="text-muted-foreground">{t('detail_director')}:</span>
                    <span className="flex flex-wrap gap-2">
                      {directorList.length > 0
                        ? directorList.map((d, i) => (
                            <span key={d.name} className="inline-flex items-center gap-1">
                              {i > 0 && <span>,</span>}
                              <Link
                                to={`/browse?type=director&value=${encodeURIComponent(d.name)}&name=${encodeURIComponent(d.name)}${d.tmdbPersonId ? `&person_id=${d.tmdbPersonId}` : ''}`}
                                className="hover:text-foreground"
                              >
                                {d.name}
                              </Link>
                            </span>
                          ))
                        : t('detail_director_unknown')}
                    </span>
                  </div>

                  <div className="flex flex-wrap gap-2">
                    <span className="text-muted-foreground">{t('detail_cast')}:</span>
                    <span className="flex flex-wrap gap-2">
                      {castMembers.length > 0
                        ? castMembers.map((a, i) => (
                            <span key={a.name + i} className="inline-flex items-center gap-1">
                              {i > 0 && <span>,</span>}
                              <Link
                                to={`/browse?type=cast&value=${encodeURIComponent(a.name)}&name=${encodeURIComponent(a.name)}${a.tmdbPersonId ? `&person_id=${a.tmdbPersonId}` : ''}`}
                                className="hover:text-foreground"
                              >
                                {a.name}
                              </Link>
                              {a.character && <span className="text-muted-foreground">({a.character})</span>}
                            </span>
                          ))
                        : t('detail_cast_empty')}
                    </span>
                  </div>

                  {(budgetStr || revenueStr) && (
                    <div className="flex flex-wrap gap-6">
                      {budgetStr && (
                        <div className="flex items-center gap-2">
                          <span className="text-muted-foreground">{t('detail_budget')}:</span>
                          <span>{budgetStr}</span>
                        </div>
                      )}
                      {revenueStr && (
                        <div className="flex items-center gap-2">
                          <span className="text-muted-foreground">{t('detail_revenue')}:</span>
                          <span>{revenueStr}</span>
                        </div>
                      )}
                    </div>
                  )}

                  {productionCompanies.length > 0 && (
                    <div className="flex flex-wrap gap-2">
                      <span className="text-muted-foreground">{t('detail_production')}:</span>
                      <span>{productionCompanies.map(c => c.name).filter(Boolean).join(', ')}</span>
                    </div>
                  )}

                  {spokenLanguages.length > 0 && (
                    <div className="flex flex-wrap gap-2">
                      <span className="text-muted-foreground">{t('detail_spoken_lang')}:</span>
                      <span>{spokenLanguages.map(l => l.english_name || l.name || l.iso_639_1).filter(Boolean).join(', ')}</span>
                    </div>
                  )}

                  {originCountries.length > 0 && (
                    <div className="flex flex-wrap gap-2">
                      <span className="text-muted-foreground">{t('detail_origin_country')}:</span>
                      <span>{originCountries.join(', ')}</span>
                    </div>
                  )}
                </div>

                {keywords.length > 0 && (
                  <div className="space-y-2">
                    <div className="text-sm font-semibold">{t('detail_keywords')}</div>
                    <div className="flex flex-wrap gap-2">
                      {keywords.map((kw) => (
                        <Link
                          key={kw}
                          to={`/browse?type=keyword&value=${encodeURIComponent(kw)}&name=${encodeURIComponent(kw)}`}
                          className="hover:no-underline"
                        >
                          <Badge variant="secondary">{kw}</Badge>
                        </Link>
                      ))}
                    </div>
                  </div>
                )}

                {llmTags.length > 0 && (
                  <div className="space-y-2">
                    <div className="text-sm font-semibold">{t('detail_llm_tags')}</div>
                    <div className="flex flex-wrap gap-2">
                      {llmTags.map((tag) => (
                        <Badge key={tag} variant="outline">{tag}</Badge>
                      ))}
                    </div>
                  </div>
                )}

                <div className="space-y-2">
                  <div className="text-sm font-semibold text-muted-foreground">{t('detail_external_links')}</div>
                  <div className="flex flex-wrap gap-2">
                    {movie.tmdb_id && (
                      <Button asChild variant="outline" size="sm">
                        <a href={`https://www.themoviedb.org/movie/${movie.tmdb_id}`} target="_blank" rel="noopener noreferrer">TMDB</a>
                      </Button>
                    )}
                    {(movie.imdb_id || externalIds?.imdb_id) && (
                      <Button asChild variant="outline" size="sm">
                        <a href={`https://www.imdb.com/title/${movie.imdb_id || externalIds?.imdb_id}`} target="_blank" rel="noopener noreferrer">IMDb</a>
                      </Button>
                    )}
                    {externalIds?.wikidata_id && (
                      <Button asChild variant="outline" size="sm">
                        <a href={`https://www.wikidata.org/wiki/${externalIds.wikidata_id}`} target="_blank" rel="noopener noreferrer">Wikidata</a>
                      </Button>
                    )}
                    {externalIds?.facebook_id && (
                      <Button asChild variant="outline" size="sm">
                        <a href={`https://www.facebook.com/${externalIds.facebook_id}`} target="_blank" rel="noopener noreferrer">Facebook</a>
                      </Button>
                    )}
                    {externalIds?.instagram_id && (
                      <Button asChild variant="outline" size="sm">
                        <a href={`https://www.instagram.com/${externalIds.instagram_id}`} target="_blank" rel="noopener noreferrer">Instagram</a>
                      </Button>
                    )}
                    {externalIds?.twitter_id && (
                      <Button asChild variant="outline" size="sm">
                        <a href={`https://twitter.com/${externalIds.twitter_id}`} target="_blank" rel="noopener noreferrer">X/Twitter</a>
                      </Button>
                    )}
                    {movie.homepage && (
                      <Button asChild variant="outline" size="sm">
                        <a href={movie.homepage} target="_blank" rel="noopener noreferrer">{t('detail_homepage')}</a>
                      </Button>
                    )}
                  </div>
                </div>

                {dirPaths.length > 0 && (
                  <div className="space-y-2">
                    <div className="text-sm font-semibold">{t('detail_files')}</div>
                    <div className="space-y-2">
                      {dirPaths.map((p) => {
                        const dirName = p.split('/').pop() || p
                        return (
                          <div key={p} className="flex flex-wrap items-center gap-2 rounded-lg border bg-muted/20 px-3 py-2">
                            <span className="font-mono text-xs text-muted-foreground break-all">{dirName}</span>
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              onClick={() => { navigator.clipboard.writeText(dirName) }}
                              title={t('detail_copy_title')}
                            >
                              <Copy className="h-4 w-4" />
                              {t('detail_copy')}
                            </Button>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                )}

                <Separator />

                <div className="space-y-2">
                  <h3 className="text-lg font-semibold">{t('detail_overview')}</h3>
                  <p className="text-sm leading-relaxed text-muted-foreground whitespace-pre-line">
                    {movie.overview || t('detail_overview_empty')}
                  </p>
                </div>
              </div>
            </div>
          </Card>

          <div className="space-y-4">
            {castCredits.length > 0 && (
              <CollapsibleSection title={t('detail_section_credits')} defaultOpen>
                <div className="space-y-3">
                  <h4 className="text-sm font-semibold text-muted-foreground">{t('detail_credits_cast')}</h4>
                  <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5">
                    {castCredits.slice(0, 20).map(c => (
                      <Link
                        key={c.id}
                        to={`/browse?type=cast&value=${encodeURIComponent(c.person_name)}&name=${encodeURIComponent(c.person_name)}&person_id=${c.tmdb_person_id}`}
                        className="group overflow-hidden rounded-lg border bg-card transition hover:-translate-y-1 hover:shadow-lg"
                      >
                        {c.profile_path ? (
                          <img src={`${TMDB_IMG}/w185${c.profile_path}`} alt={c.person_name} className="h-40 w-full object-cover" />
                        ) : (
                          <div className="flex h-40 w-full items-center justify-center bg-muted text-sm text-muted-foreground">N/A</div>
                        )}
                        <div className="p-2 space-y-1">
                          <div className="text-sm font-semibold leading-tight">{c.person_name}</div>
                          {c.role && <div className="text-xs text-muted-foreground">{t('detail_credits_as')} {c.role}</div>}
                        </div>
                      </Link>
                    ))}
                  </div>
                  {crewCredits.length > 0 && (
                    <div className="space-y-2">
                      <h4 className="text-sm font-semibold text-muted-foreground">{t('detail_credits_crew')}</h4>
                      <div className="grid gap-2 md:grid-cols-2">
                        {crewCredits.map(c => (
                          <div key={c.id} className="flex items-center justify-between rounded-md border bg-muted/20 px-3 py-2 text-sm">
                            <span>{c.person_name}</span>
                            <span className="text-muted-foreground">{c.role || c.department}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </CollapsibleSection>
            )}

            {videos.length > 0 && (
              <CollapsibleSection title={t('detail_section_videos')} defaultOpen>
                <div className="grid gap-4 md:grid-cols-2">
                  {videos.slice(0, 4).map(v => (
                    <div key={v.id} className="space-y-2">
                      <div className="overflow-hidden rounded-lg border">
                        <div className="aspect-video">
                          <iframe
                            src={`https://www.youtube.com/embed/${v.video_key}`}
                            title={v.name || 'Video'}
                            allowFullScreen
                            className="h-full w-full"
                          />
                        </div>
                      </div>
                      {v.name && <div className="text-sm text-muted-foreground">{v.name}</div>}
                    </div>
                  ))}
                </div>
              </CollapsibleSection>
            )}

            {(backdrops.length > 0 || posters.length > 0) && (
              <CollapsibleSection title={t('detail_section_images')}>
                <div className="space-y-4">
                  {backdrops.length > 0 && (
                    <div className="space-y-2">
                      <h4 className="text-sm font-semibold text-muted-foreground">{t('detail_images_backdrops')}</h4>
                      <div className="flex gap-3 overflow-x-auto pb-2">
                        {backdrops.slice(0, 10).map(img => (
                          <a key={img.id} href={`${TMDB_IMG}/original${img.file_path}`} target="_blank" rel="noopener noreferrer">
                            <img src={`${TMDB_IMG}/w300${img.file_path}`} alt="" className="h-32 rounded-lg border bg-muted object-cover" />
                          </a>
                        ))}
                      </div>
                    </div>
                  )}
                  {posters.length > 0 && (
                    <div className="space-y-2">
                      <h4 className="text-sm font-semibold text-muted-foreground">{t('detail_images_posters')}</h4>
                      <div className="flex gap-3 overflow-x-auto pb-2">
                        {posters.slice(0, 10).map(img => (
                          <a key={img.id} href={`${TMDB_IMG}/original${img.file_path}`} target="_blank" rel="noopener noreferrer">
                            <img src={`${TMDB_IMG}/w185${img.file_path}`} alt="" className="h-44 rounded-lg border bg-muted object-cover" />
                          </a>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </CollapsibleSection>
            )}

            {wpByCountry.size > 0 && (
              <CollapsibleSection title={t('detail_section_watch_providers')}>
                <div className="space-y-3">
                  {[...wpByCountry.entries()].map(([country, types]) => (
                    <div key={country} className="space-y-2 rounded-lg border bg-muted/20 p-3">
                      <div className="flex items-center gap-2">
                        <Badge variant="secondary">{country}</Badge>
                      </div>
                      {types.stream.length > 0 && (
                        <div className="space-y-2">
                          <div className="text-sm font-semibold">{t('detail_wp_stream')}</div>
                          <div className="flex flex-wrap gap-2">
                            {types.stream.map(p => (
                              <div key={p.id} className="flex items-center gap-2 rounded-md border bg-card/60 px-2 py-1">
                                {p.logo_path ? (
                                  <img src={`${TMDB_IMG}/w45${p.logo_path}`} alt={p.provider_name || ''} className="h-6 w-6 object-contain" />
                                ) : (
                                  <span className="text-sm">{p.provider_name}</span>
                                )}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                      {types.rent.length > 0 && (
                        <div className="space-y-2">
                          <div className="text-sm font-semibold">{t('detail_wp_rent')}</div>
                          <div className="flex flex-wrap gap-2">
                            {types.rent.map(p => (
                              <div key={p.id} className="flex items-center gap-2 rounded-md border bg-card/60 px-2 py-1">
                                {p.logo_path ? (
                                  <img src={`${TMDB_IMG}/w45${p.logo_path}`} alt={p.provider_name || ''} className="h-6 w-6 object-contain" />
                                ) : (
                                  <span className="text-sm">{p.provider_name}</span>
                                )}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                      {types.buy.length > 0 && (
                        <div className="space-y-2">
                          <div className="text-sm font-semibold">{t('detail_wp_buy')}</div>
                          <div className="flex flex-wrap gap-2">
                            {types.buy.map(p => (
                              <div key={p.id} className="flex items-center gap-2 rounded-md border bg-card/60 px-2 py-1">
                                {p.logo_path ? (
                                  <img src={`${TMDB_IMG}/w45${p.logo_path}`} alt={p.provider_name || ''} className="h-6 w-6 object-contain" />
                                ) : (
                                  <span className="text-sm">{p.provider_name}</span>
                                )}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </CollapsibleSection>
            )}

            {reviews.length > 0 && (
              <CollapsibleSection title={t('detail_section_reviews')}>
                <div className="space-y-3">
                  {reviews.slice(0, 5).map(r => (
                    <div key={r.id} className="rounded-lg border bg-card/70 p-4 shadow-sm">
                      <div className="flex items-center justify-between text-sm">
                        <span className="font-semibold">{r.author || r.author_username || 'Anonymous'}</span>
                        {typeof r.rating === 'number' && (
                          <span className="rounded-md bg-muted px-2 py-1 text-xs text-muted-foreground">{r.rating.toFixed(1)}/10</span>
                        )}
                      </div>
                      {r.content && (
                        <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
                          {r.content.length > 500 ? `${r.content.slice(0, 500)}...` : r.content}
                        </p>
                      )}
                    </div>
                  ))}
                </div>
              </CollapsibleSection>
            )}

            {similar.length > 0 && (
              <CollapsibleSection title={t('detail_section_similar')}>
                <div className="flex flex-wrap gap-2">
                  {similar.map(r => (
                    <Badge key={r.id} variant="outline">
                      <a href={`https://www.themoviedb.org/movie/${r.related_tmdb_id}`} target="_blank" rel="noopener noreferrer" className="hover:underline">
                        TMDB #{r.related_tmdb_id}
                      </a>
                    </Badge>
                  ))}
                </div>
              </CollapsibleSection>
            )}

            {recommendations.length > 0 && (
              <CollapsibleSection title={t('detail_section_recommendations')}>
                <div className="flex flex-wrap gap-2">
                  {recommendations.map(r => (
                    <Badge key={r.id} variant="outline">
                      <a href={`https://www.themoviedb.org/movie/${r.related_tmdb_id}`} target="_blank" rel="noopener noreferrer" className="hover:underline">
                        TMDB #{r.related_tmdb_id}
                      </a>
                    </Badge>
                  ))}
                </div>
              </CollapsibleSection>
            )}

            {rdByCountry.size > 0 && (
              <CollapsibleSection title={t('detail_section_release_dates')}>
                <div className="space-y-3">
                  {[...rdByCountry.entries()].map(([country, rds]) => (
                    <div key={country} className="rounded-lg border bg-muted/20 p-3 space-y-2">
                      <div className="flex items-center gap-2">
                        <Badge variant="secondary">{country}</Badge>
                      </div>
                      <div className="flex flex-wrap gap-2 text-sm">
                        {rds.map(rd => (
                          <span key={rd.id} className="rounded-md border bg-card/60 px-2 py-1">
                            {rd.release_date?.split('T')[0]}
                            {rd.certification && <span className="ml-2 text-xs text-muted-foreground">{rd.certification}</span>}
                            {rd.note && <span className="ml-2 text-xs text-muted-foreground">({rd.note})</span>}
                          </span>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </CollapsibleSection>
            )}

            {altTitles.length > 0 && (
              <CollapsibleSection title={t('detail_section_alt_titles')}>
                <div className="divide-y divide-border text-sm">
                  {altTitles.map(at => (
                    <div key={at.id} className="flex flex-wrap items-center gap-2 py-2">
                      <span className="rounded-md bg-muted px-2 py-1 text-xs text-muted-foreground">{at.iso_3166_1 || '—'}</span>
                      <span className="font-medium">{at.title}</span>
                      {at.title_type && <span className="text-muted-foreground">({at.title_type})</span>}
                    </div>
                  ))}
                </div>
              </CollapsibleSection>
            )}

            {translations.length > 0 && (
              <CollapsibleSection title={t('detail_section_translations')}>
                <div className="divide-y divide-border text-sm">
                  {translations.map(tr => (
                    <div key={tr.id} className="flex flex-wrap items-center gap-2 py-2">
                      <span className="rounded-md bg-muted px-2 py-1 text-xs text-muted-foreground">
                        {tr.language_name || tr.iso_639_1}{tr.iso_3166_1 ? ` (${tr.iso_3166_1})` : ''}
                      </span>
                      {tr.title && <span className="font-medium">{tr.title}</span>}
                      {tr.tagline && <span className="text-muted-foreground">— {tr.tagline}</span>}
                    </div>
                  ))}
                </div>
              </CollapsibleSection>
            )}

            {lists.length > 0 && (
              <CollapsibleSection title={t('detail_section_lists')}>
                <div className="grid gap-3 md:grid-cols-2">
                  {lists.map(l => (
                    <a
                      key={l.id}
                      href={`https://www.themoviedb.org/list/${l.tmdb_list_id}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="rounded-lg border bg-card/70 p-4 transition hover:-translate-y-1 hover:shadow-lg"
                    >
                      <div className="text-base font-semibold">{l.list_name || `List #${l.tmdb_list_id}`}</div>
                      {l.item_count && <div className="text-sm text-muted-foreground">{l.item_count} {t('detail_list_items')}</div>}
                      {l.description && (
                        <div className="mt-2 text-sm text-muted-foreground">
                          {l.description.length > 160 ? `${l.description.slice(0, 160)}...` : l.description}
                        </div>
                      )}
                    </a>
                  ))}
                </div>
              </CollapsibleSection>
            )}
          </div>
        </>
      )}
    </div>
  )
}
