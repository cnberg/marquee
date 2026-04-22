import { useEffect, useMemo, useState } from 'react'
import MovieCard from '../components/MovieCard'
import ThinkingPanel from '../components/ThinkingPanel'
import { api } from '../api/client'
import type { Movie, RecommendResult } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { useAuth } from '../auth/AuthContext'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card'
import { Button } from '../components/ui/button'
import { Textarea } from '../components/ui/textarea'
import { Alert, AlertDescription } from '../components/ui/alert'

interface DailyPickSection {
  inspiration: string
  movies: Array<{ movie: Movie; reason?: string | null; in_library?: boolean }>
}

interface ThinkingEntry {
  stage: string
  label: string
  detail: any
}

export default function Library() {
  const { t } = useLocale()
  const { user, showAuthModal } = useAuth()
  const [recPrompt, setRecPrompt] = useState('')
  const [recLoading, setRecLoading] = useState(false)
  const [recStatus, setRecStatus] = useState('')
  const [recResult, setRecResult] = useState<RecommendResult | null>(null)
  const [recError, setRecError] = useState<string | null>(null)
  const [thinkingLog, setThinkingLog] = useState<ThinkingEntry[]>([])
  const [inspireLoading, setInspireLoading] = useState(false)
  const [inspireIdeas, setInspireIdeas] = useState<Array<{ display: string; query: string }>>([])
  const [dailyPicks, setDailyPicks] = useState<DailyPickSection[]>([])
  const [marks, setMarks] = useState<Record<number, { want: boolean; watched: boolean; favorite: boolean }>>({})

  useEffect(() => { document.title = t('home_title') }, [t])

  // Fetch daily picks
  useEffect(() => {
    let retryTimer: ReturnType<typeof setTimeout>
    const load = async () => {
      try {
        const res = await api.dailyPicks()
        if (res.sections.length > 0) {
          setDailyPicks(res.sections)
        } else {
          retryTimer = setTimeout(load, 10000)
        }
      } catch { /* ignore */ }
    }
    load()
    return () => clearTimeout(retryTimer)
  }, [])

  const visibleMovieIds = useMemo(() => {
    const ids = new Set<number>()
    recResult?.recommendations?.forEach((r) => ids.add(r.movie.id))
    dailyPicks.forEach((section) => {
      section.movies.forEach((m) => ids.add(m.movie.id))
    })
    return Array.from(ids)
  }, [recResult, dailyPicks])

  useEffect(() => {
    if (!user) {
      setMarks({})
      return
    }
    if (visibleMovieIds.length === 0) return
    api
      .batchMarks(visibleMovieIds)
      .then((res) => setMarks(res))
      .catch(() => { /* ignore */ })
  }, [user?.id, visibleMovieIds])

  const handleInspire = async () => {
    if (inspireLoading) return
    setInspireLoading(true)
    try {
      const res = await api.inspire()
      setInspireIdeas(res.ideas)
    } catch (err) {
      setRecError(err instanceof Error ? err.message : t('home_inspire_error'))
    } finally {
      setInspireLoading(false)
    }
  }

  const ensureMarkState = (movieId: number) =>
    marks[movieId] ?? { want: false, watched: false, favorite: false }

  const handleToggleMark = async (movieId: number, markType: 'want' | 'watched' | 'favorite') => {
    if (!user) {
      showAuthModal()
      return
    }
    const prev = ensureMarkState(movieId)
    const isActive = prev[markType]
    const optimistic = { ...prev, [markType]: !isActive }
    if (!isActive && markType === 'want') optimistic.watched = false
    if (!isActive && markType === 'watched') optimistic.want = false
    setMarks((m) => ({ ...m, [movieId]: optimistic }))
    try {
      const res = isActive ? await api.removeMark(movieId, markType) : await api.setMark(movieId, markType)
      setMarks((m) => ({ ...m, [movieId]: res }))
    } catch {
      setMarks((m) => ({ ...m, [movieId]: prev }))
    }
  }

  const handleRecommend = async () => {
    if (!recPrompt.trim() || recLoading) return
    setRecLoading(true)
    setRecStatus('正在理解你的查询…')
    setRecResult(null)
    setRecError(null)
    setThinkingLog([])
    try {
      await api.recommend(
        recPrompt.trim(),
        (_stage, message) => {
          if (message.startsWith('status_found:')) {
            const n = message.split(':')[1]
            setRecStatus(t('status_found', { n }))
          } else {
            const translated = t(message)
            setRecStatus(translated === message ? message : translated)
          }
        },
        (data) => {
          setRecResult(data)
          setRecStatus('')
        },
        (message) => {
          const translated = t(message)
          setRecError(translated === message ? message : translated)
          setRecStatus('')
        },
        // onThinking callback
        (stage, label, detail) => {
          setThinkingLog((prev) => [...prev, { stage, label, detail }])
        },
      )
    } catch (err) {
      setRecError(err instanceof Error ? err.message : t('home_rec_error'))
      setRecStatus('')
    } finally {
      setRecLoading(false)
    }
  }

  return (
    <div className="space-y-8">
      <div className="space-y-2">
        <p className="text-muted-foreground">{t('home_subtitle')}</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>{t('home_recommend_btn')}</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <Textarea
            value={recPrompt}
            onChange={(e) => setRecPrompt(e.target.value)}
            placeholder={t('home_placeholder')}
            rows={3}
            disabled={recLoading}
          />
          <div className="flex flex-wrap items-center gap-3">
            <Button type="button" onClick={handleRecommend} disabled={recLoading || !recPrompt.trim()}>
              {recLoading ? t('home_recommending') : t('home_recommend_btn')}
            </Button>
            <Button type="button" variant="secondary" onClick={handleInspire} disabled={inspireLoading}>
              {inspireLoading ? t('home_inspire_loading') : t('home_inspire_btn')}
            </Button>
            {recStatus && <span className="text-sm text-muted-foreground">{recStatus}</span>}
          </div>

          {inspireIdeas.length > 0 && (
            <div className="flex flex-wrap gap-2">
              {inspireIdeas.map((idea, i) => (
                <Button
                  key={i}
                  type="button"
                  variant="secondary"
                  size="sm"
                  onClick={() => {
                    setRecPrompt(idea.query)
                  }}
                  title={idea.query}
                >
                  {idea.display}
                </Button>
              ))}
            </div>
          )}

          <ThinkingPanel entries={thinkingLog} />

          {recError && (
            <Alert variant="destructive">
              <AlertDescription>{recError}</AlertDescription>
            </Alert>
          )}

          {recResult && recResult.recommendations.length > 0 && (
            <div className="space-y-3">
              <h3 className="text-lg font-semibold">{t('home_rec_results')}</h3>
              <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5">
                {recResult.recommendations.map((item) => (
                  <div key={item.movie.id} className="space-y-2">
                    <MovieCard
                      movie={item.movie}
                      marks={marks[item.movie.id]}
                      onToggleMark={(mt) => handleToggleMark(item.movie.id, mt)}
                      outOfLibrary={item.in_library === false}
                    />
                    {item.reason && <div className="text-sm text-muted-foreground">{item.reason}</div>}
                  </div>
                ))}
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {dailyPicks.length > 0 && (
        <div className="space-y-4">
          <h2 className="text-xl font-semibold">{t('home_daily_picks')}</h2>
          {dailyPicks.map((section, i) => (
            <Card key={i}>
              <CardHeader>
                <CardTitle className="text-lg">{section.inspiration}</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5">
                  {section.movies.map((item) => (
                    <div key={item.movie.id} className="space-y-2">
                      <MovieCard
                        movie={item.movie}
                        marks={marks[item.movie.id]}
                        onToggleMark={(mt) => handleToggleMark(item.movie.id, mt)}
                        outOfLibrary={item.in_library === false}
                      />
                      {item.reason && <div className="text-sm text-muted-foreground">{item.reason}</div>}
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  )
}
