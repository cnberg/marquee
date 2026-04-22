import { Link } from 'react-router-dom'
import type { Movie } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { MovieMarkButtons } from './MovieMarkButtons'
import { Badge } from './ui/badge'

interface MovieCardProps {
  movie: Movie
  marks?: { want: boolean; watched: boolean; favorite: boolean }
  onToggleMark?: (markType: 'want' | 'watched' | 'favorite') => void
  outOfLibrary?: boolean
}

export function MovieCard({ movie, marks, onToggleMark, outOfLibrary = false }: MovieCardProps) {
  const { t } = useLocale()
  const { id, title, year, tmdb_rating: rating, poster_url: posterUrl } = movie

  return (
    <Link
      to={`/movies/${id}`}
      className="group block rounded-xl border bg-card shadow-sm transition hover:-translate-y-1 hover:shadow-lg"
    >
      <div className="relative aspect-[2/3] overflow-hidden rounded-t-xl bg-muted">
        {posterUrl ? (
          <img src={posterUrl} alt={title} loading="lazy" className="h-full w-full object-cover transition duration-300" />
        ) : (
          <div className="flex h-full w-full items-center justify-center text-sm text-muted-foreground" aria-label={t('card_no_poster')}>
            {t('card_no_poster')}
          </div>
        )}
        {outOfLibrary && (
          <Badge variant="secondary" className="absolute left-2 top-2">
            {t('movie.outOfLibrary')}
          </Badge>
        )}
      </div>
      <div className="space-y-2 p-3">
        <div className="line-clamp-2 text-sm font-semibold leading-snug text-foreground" title={title}>
          {title}
        </div>
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span>{year ?? t('card_unknown_year')}</span>
          <span>•</span>
          <span>{typeof rating === 'number' ? rating.toFixed(1) : t('card_unrated')}</span>
        </div>
        {marks && onToggleMark && (
          <MovieMarkButtons movieId={id} marks={marks} onToggle={onToggleMark} size="sm" />
        )}
      </div>
    </Link>
  )
}

export default MovieCard
