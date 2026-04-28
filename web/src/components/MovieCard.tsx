import { useState, type ReactNode } from 'react'
import { Link } from 'react-router-dom'
import { Download, Check, Sparkles } from 'lucide-react'
import type { Movie } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { pickLocalized } from '../lib/utils'
import { MovieMarkButtons } from './MovieMarkButtons'
import { Button } from './ui/button'
import { buildSearchUrl, copyToClipboard, getExtensionId } from '../lib/ptDepiler'

interface MovieCardProps {
  movie: Movie
  marks?: { want: boolean; watched: boolean; favorite: boolean }
  onToggleMark?: (markType: 'want' | 'watched' | 'favorite') => void
  outOfLibrary?: boolean
  downloading?: boolean
  reason?: ReactNode
}

export function MovieCard({
  movie,
  marks,
  onToggleMark,
  outOfLibrary = false,
  downloading = false,
  reason,
}: MovieCardProps) {
  const { t, locale } = useLocale()
  const { id, year, tmdb_rating: rating, poster_url: posterUrl } = movie
  const title = pickLocalized(locale, movie.title_en, movie.title_zh, movie.title) ?? movie.title
  const [ptCopied, setPtCopied] = useState(false)

  const handlePtDownload = async (e: React.MouseEvent) => {
    e.preventDefault()
    e.stopPropagation()
    if (!movie.imdb_id) return
    const extId = getExtensionId()
    if (!extId) {
      await copyToClipboard(movie.imdb_id)
      setPtCopied(true)
      setTimeout(() => setPtCopied(false), 2000)
      return
    }
    const url = buildSearchUrl(extId, movie.imdb_id)
    await copyToClipboard(url)
    setPtCopied(true)
    setTimeout(() => setPtCopied(false), 2000)
  }

  const hasButtons = (marks && onToggleMark) || (outOfLibrary && movie.imdb_id)

  return (
    <div className="group block overflow-hidden rounded-xl bg-card shadow-md transition hover:shadow-xl">
      <Link to={`/movies/${id}`} aria-label={title} className="block">
        <div className="relative aspect-[2/3] overflow-hidden bg-muted">
          {posterUrl ? (
            <img src={posterUrl} alt={title} loading="lazy" className="h-full w-full object-cover transition duration-300" />
          ) : (
            <div className="flex h-full w-full items-center justify-center text-sm text-muted-foreground" aria-label={t('card_no_poster')}>
              {t('card_no_poster')}
            </div>
          )}
          {downloading && (
            <div className="absolute inset-x-0 top-0 flex items-center justify-center gap-1.5 bg-secondary py-1.5 text-xs font-bold uppercase tracking-wide text-secondary-foreground shadow-md">
              <span>{t('movie.downloading')}</span>
            </div>
          )}
          {outOfLibrary && !downloading && (
            <div className="absolute inset-x-0 top-0 flex items-center justify-center gap-1.5 bg-primary py-1.5 text-xs font-bold uppercase tracking-wide text-primary-foreground shadow-md">
              <Sparkles className="h-3.5 w-3.5" />
              <span>{t('movie.outOfLibrary')}</span>
            </div>
          )}
        </div>
      </Link>
      <div className="space-y-2 p-3">
        <Link to={`/movies/${id}`} className="block space-y-2">
          <div className="line-clamp-2 text-sm font-semibold leading-snug text-foreground" title={title}>
            {title}
          </div>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <span>{year ?? t('card_unknown_year')}</span>
            <span>•</span>
            <span>{typeof rating === 'number' ? rating.toFixed(1) : t('card_unrated')}</span>
          </div>
        </Link>
        {reason && (
          <div className="line-clamp-3 text-sm leading-snug text-muted-foreground">{reason}</div>
        )}
        {hasButtons && (
          <div className="flex items-center gap-2">
            {marks && onToggleMark && (
              <MovieMarkButtons movieId={id} marks={marks} onToggle={onToggleMark} size="sm" />
            )}
            {outOfLibrary && movie.imdb_id && (
              <Button
                variant="outline"
                size="sm"
                onClick={handlePtDownload}
                title={t('pt_card_download')}
                type="button"
                className="hidden bg-background md:inline-flex"
              >
                {ptCopied ? <Check className="h-4 w-4" /> : <Download className="h-4 w-4" />}
              </Button>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

export default MovieCard
