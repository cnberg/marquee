import { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api } from '../api/client'
import MovieCard from '../components/MovieCard'
import ThinkingPanel from '../components/ThinkingPanel'
import { useAuth } from '../auth/AuthContext'
import { useMovieMarks } from '../hooks/useMovieMarks'
import { useLocale } from '../i18n/LocaleContext'
import { Button } from '../components/ui/button'
import { Card } from '../components/ui/card'
import { Alert, AlertDescription } from '../components/ui/alert'
import type {
  ParsedSseEvent,
  RecommendResult,
  SearchHistoryDetail as HistoryDetailType,
} from '../types'

interface ThinkingEntry { stage: string; label: string; detail: any }

export default function SearchHistoryDetail() {
  const { id } = useParams<{ id: string }>()
  const { t } = useLocale()
  const { user, showAuthModal } = useAuth()
  const navigate = useNavigate()
  const [detail, setDetail] = useState<HistoryDetailType | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => { document.title = t('history_detail_title') }, [t])

  useEffect(() => {
    if (!user) { showAuthModal(); navigate('/'); return }
    if (!id) return
    setLoading(true)
    api.getHistory(Number(id))
      .then(setDetail)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [id, user?.id])

  const { thinkingEntries, finalResult } = useMemo(() => {
    if (!detail) return { thinkingEntries: [], finalResult: null as RecommendResult | null }
    let parsed: ParsedSseEvent[] = []
    try { parsed = JSON.parse(detail.sse_events) } catch { /* ignore */ }
    const thinking: ThinkingEntry[] = []
    let result: RecommendResult | null = null
    for (const ev of parsed) {
      if (ev.event === 'thinking') {
        thinking.push({ stage: ev.data?.stage ?? '', label: ev.data?.label ?? '', detail: ev.data?.detail })
      } else if (ev.event === 'result') {
        result = ev.data as RecommendResult
      }
    }
    return { thinkingEntries: thinking, finalResult: result }
  }, [detail])

  const visibleMovieIds = useMemo(
    () => (finalResult ? finalResult.recommendations.map((r) => r.movie.id) : []),
    [finalResult],
  )
  const { marks, toggle } = useMovieMarks(visibleMovieIds)

  if (!user) return null

  return (
    <div className="space-y-6">
      <div className="flex items-center">
        <Button asChild variant="ghost" size="sm">
          <Link to="/history">← {t('history_back')}</Link>
        </Button>
      </div>

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

      {detail && (
        <>
          <Card className="space-y-2 p-4">
            <div className="text-xs font-semibold uppercase text-muted-foreground">{t('history_prompt_label')}</div>
            <div className="text-base leading-relaxed">{detail.prompt}</div>
            <div className="text-xs text-muted-foreground">{new Date(detail.created_at + 'Z').toLocaleString()}</div>
          </Card>

          <ThinkingPanel entries={thinkingEntries} />

          {finalResult && finalResult.recommendations.length > 0 && (
            <div className="space-y-3">
              <h3 className="text-lg font-semibold">{t('home_rec_results')}</h3>
              <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
                {finalResult.recommendations.map((item) => (
                  <Card key={item.movie.id} className="space-y-2 p-3">
                    <MovieCard
                      movie={item.movie}
                      marks={marks[item.movie.id]}
                      onToggleMark={(mt) => { toggle(item.movie.id, mt) }}
                      outOfLibrary={item.in_library === false}
                    />
                    {item.reason && (
                      <div className="text-sm text-muted-foreground leading-relaxed">{item.reason}</div>
                    )}
                  </Card>
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}
