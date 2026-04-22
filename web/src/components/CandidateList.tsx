import type { TmdbCandidate } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { Card } from './ui/card'
import { Button } from './ui/button'

interface CandidateListProps {
  candidates: TmdbCandidate[]
  onSelect: (tmdbId: number) => void
  emptyText?: string
}

function getPosterUrl(path?: string) {
  if (!path) return null
  return `https://image.tmdb.org/t/p/w185${path}`
}

export function CandidateList({ candidates, onSelect, emptyText = '暂无候选' }: CandidateListProps) {
  const { t } = useLocale()
  if (!candidates.length) {
    return (
      <div className="rounded-lg border border-dashed bg-muted/20 px-3 py-6 text-center text-sm text-muted-foreground">
        {emptyText ?? t('candidate_empty')}
      </div>
    )
  }

  return (
    <div className="grid gap-2" role="list">
      {candidates.map((candidate) => {
        const posterUrl = getPosterUrl(candidate.poster_path)
        const score =
          typeof candidate.vote_average === 'number' && !Number.isNaN(candidate.vote_average)
            ? candidate.vote_average.toFixed(1)
            : t('candidate_unrated')

        return (
          <Card key={candidate.id} className="flex items-center gap-3 p-3" role="listitem">
            {posterUrl ? (
              <img
                src={posterUrl}
                alt={candidate.title}
                width={46}
                height={69}
                loading="lazy"
                className="h-[69px] w-[46px] rounded-md object-cover"
              />
            ) : (
              <div
                className="flex h-[69px] w-[46px] items-center justify-center rounded-md bg-muted text-[11px] text-muted-foreground"
                aria-label={t('candidate_no_poster')}
              >
                {t('card_no_poster')}
              </div>
            )}
            <div className="flex-1 space-y-1">
              <div className="text-sm font-semibold leading-tight" title={candidate.title}>
                {candidate.title}
              </div>
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <span>{candidate.release_date || t('candidate_unknown_date')}</span>
                <span className="h-1 w-1 rounded-full bg-muted-foreground/70" aria-hidden />
                <span>{score}</span>
              </div>
              {candidate.original_title && candidate.original_title !== candidate.title && (
                <div className="text-xs text-muted-foreground">{candidate.original_title}</div>
              )}
            </div>
            <Button type="button" size="sm" onClick={() => onSelect(candidate.id)}>
              {t('candidate_select')}
            </Button>
          </Card>
        )
      })}
    </div>
  )
}

export default CandidateList
