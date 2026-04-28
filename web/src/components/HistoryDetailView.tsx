import { useMemo } from 'react'
import { MovieGrid } from './MovieGrid'
import ThinkingPanel from './ThinkingPanel'
import { Card } from './ui/card'
import { useLocale } from '../i18n/LocaleContext'
import type {
  ParsedSseEvent,
  RecommendResult,
  SearchHistoryDetail,
} from '../types'

interface ThinkingEntry { stage: string; label_key?: string; label: string; detail: any }

type MovieMarks = { want: boolean; watched: boolean; favorite: boolean }
type MarkType = 'want' | 'watched' | 'favorite'

interface Props {
  detail: SearchHistoryDetail
  marks?: Record<number, MovieMarks>
  onToggleMark?: (movieId: number, markType: MarkType) => void
  hideTimestamp?: boolean
}

export function useParsedHistoryEvents(detail: SearchHistoryDetail | null) {
  return useMemo(() => {
    if (!detail) return { thinkingEntries: [] as ThinkingEntry[], finalResult: null as RecommendResult | null }
    let parsed: ParsedSseEvent[] = []
    try { parsed = JSON.parse(detail.sse_events) } catch { /* ignore */ }
    const thinking: ThinkingEntry[] = []
    let result: RecommendResult | null = null
    for (const ev of parsed) {
      if (ev.event === 'thinking') {
        thinking.push({ stage: ev.data?.stage ?? '', label_key: ev.data?.label_key, label: ev.data?.label ?? '', detail: ev.data?.detail })
      } else if (ev.event === 'result') {
        result = ev.data as RecommendResult
      }
    }
    return { thinkingEntries: thinking, finalResult: result }
  }, [detail])
}

export default function HistoryDetailView({ detail, marks, onToggleMark, hideTimestamp }: Props) {
  const { t } = useLocale()
  const { thinkingEntries, finalResult } = useParsedHistoryEvents(detail)

  return (
    <div className="space-y-6">
      <Card className="space-y-2 p-4">
        <div className="text-xs font-semibold uppercase text-muted-foreground">{t('history_prompt_label')}</div>
        <div className="text-base leading-relaxed">{detail.prompt}</div>
        {!hideTimestamp && <div className="text-xs text-muted-foreground">{new Date(detail.created_at + 'Z').toLocaleString()}</div>}
      </Card>

      <ThinkingPanel entries={thinkingEntries} />

      {finalResult && finalResult.recommendations.length > 0 && (
        <div className="space-y-3">
          <h3 className="text-lg font-semibold">{t('home_rec_results')}</h3>
          <MovieGrid
            items={finalResult.recommendations}
            marks={marks}
            onToggleMark={onToggleMark}
          />
        </div>
      )}
    </div>
  )
}
