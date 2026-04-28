import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { Sparkles } from 'lucide-react'
import { MovieGrid } from '../components/MovieGrid'
import ThinkingPanel from '../components/ThinkingPanel'
import { api } from '../api/client'
import type { Movie, RecommendResult } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { useAuth } from '../auth/AuthContext'
import { Button } from '../components/ui/button'
import { Textarea } from '../components/ui/textarea'
import { Alert, AlertDescription } from '../components/ui/alert'
import { Separator } from '../components/ui/separator'

interface DailyPickSection {
  inspiration_zh: string
  inspiration_en: string
  movies: Array<{ movie: Movie; reason?: string | null; in_library?: boolean }>
}

interface ThinkingEntry {
  stage: string
  label_key?: string
  label: string
  detail: any
}

// In-memory cache: survives route navigation but clears on page refresh.
let recCache: { prompt: string; result: RecommendResult; thinking: ThinkingEntry[] } | null = null

export default function Library() {
  const { t, locale } = useLocale()
  const { user, showAuthModal } = useAuth()

  const cached = useMemo(() => recCache, [])
  const [recPrompt, setRecPrompt] = useState(cached?.prompt ?? '')
  const [recLoading, setRecLoading] = useState(false)
  const [recStatus, setRecStatus] = useState('')
  const [recResult, setRecResult] = useState<RecommendResult | null>(cached?.result ?? null)
  const [recError, setRecError] = useState<string | null>(null)
  const [thinkingLog, setThinkingLog] = useState<ThinkingEntry[]>(cached?.thinking ?? [])
  const [inspireLoading, setInspireLoading] = useState(false)
  const [inspireIdeas, setInspireIdeas] = useState<Array<{ display_zh: string; display_en: string; query: string }>>([])
  const [dailyPicks, setDailyPicks] = useState<DailyPickSection[]>([])
  const [mostRelated, setMostRelated] = useState<Array<{ movie: Movie; ref_count: number; downloading: boolean; reason?: string | null }>>([])

  const [recentLibrary, setRecentLibrary] = useState<Array<{ movie: Movie; downloading: boolean }>>([])
  const [marks, setMarks] = useState<Record<number, { want: boolean; watched: boolean; favorite: boolean }>>({})
  const [libraryTotal, setLibraryTotal] = useState<number | null>(null)
  const [searchParams, setSearchParams] = useSearchParams()

  useEffect(() => { document.title = t('home_title') }, [t])

  useEffect(() => {
    api.statusCounts()
      .then((res) => setLibraryTotal(res.library_total ?? 0))
      .catch(() => { /* keep null → show fallback slogan */ })
  }, [])

  const slogan = libraryTotal && libraryTotal > 0
    ? t('home_subtitle').replace('{count}', String(libraryTotal))
    : t('home_subtitle_empty')

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

  // Fetch bottom sections: 库外热门 + 最新入库
  useEffect(() => {
    api.mostRelated().then((res) => setMostRelated(res.items)).catch(() => { /* ignore */ })
    api.recentLibrary().then((res) => setRecentLibrary(res.items)).catch(() => { /* ignore */ })
  }, [])

  const visibleMovieIds = useMemo(() => {
    const ids = new Set<number>()
    recResult?.recommendations?.forEach((r) => ids.add(r.movie.id))
    dailyPicks.forEach((section) => {
      section.movies.forEach((m) => ids.add(m.movie.id))
    })
    mostRelated.forEach((item) => ids.add(item.movie.id))
    recentLibrary.forEach((item) => ids.add(item.movie.id))
    return Array.from(ids)
  }, [recResult, dailyPicks, mostRelated, recentLibrary])

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

  const handleRecommend = () => runRecommend(recPrompt)

  const handleSelectInspire = (idea: { display_zh: string; display_en: string; query: string }) => {
    if (recLoading) return
    const newPrompt = idea.query.trim()
    if (!newPrompt) return
    const current = recPrompt.trim()
    if (current && current !== newPrompt) {
      if (!window.confirm(t('home_inspire_overwrite_confirm'))) return
    }
    setRecPrompt(newPrompt)
    void runRecommend(newPrompt)
  }

  const runRecommend = async (rawPrompt: string) => {
    const trimmed = rawPrompt.trim()
    if (!trimmed || recLoading) return
    setRecLoading(true)
    setRecStatus('正在理解你的查询…')
    setRecResult(null)
    setRecError(null)
    setThinkingLog([])
    // 标记是否收到过终止事件（result 或 error）。若 SSE 流在 LLM 长等待中
    // 被中间盒断开、或服务器异常关闭而没发终止事件，避免 UI 永远卡在上一条 status。
    let terminated = false
    let finalResult: RecommendResult | null = null
    const thinkingEntries: ThinkingEntry[] = []
    try {
      await api.recommend(
        trimmed,
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
          terminated = true
          finalResult = data
          setRecResult(data)
        },
        (message) => {
          terminated = true
          const translated = t(message)
          setRecError(translated === message ? message : translated)
        },
        // onThinking callback
        (stage, labelKey, label, detail) => {
          thinkingEntries.push({ stage, label_key: labelKey, label, detail })
          setThinkingLog((prev) => [...prev, { stage, label_key: labelKey, label, detail }])
        },
      )
      if (!terminated) {
        setRecError(t('home_rec_error'))
      }
    } catch (err) {
      setRecError(err instanceof Error ? err.message : t('home_rec_error'))
    } finally {
      // 无论成功、失败还是流意外中断，都清 status——不留残影。
      setRecStatus('')
      setRecLoading(false)
      if (finalResult) {
        recCache = { prompt: trimmed, result: finalResult, thinking: thinkingEntries }
      }
    }
  }

  // 来自搜索历史「重新搜索」入口：?q=... → 自动用该 prompt 触发推荐，
  // 然后清掉 URL 参数避免刷新页面时重复触发。
  useEffect(() => {
    const q = searchParams.get('q')?.trim()
    if (!q) return
    setRecPrompt(q)
    void runRecommend(q)
    setSearchParams({}, { replace: true })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams])

  return (
    <div className="space-y-8">
      <div className="space-y-2">
        <p className="text-muted-foreground">{slogan}</p>
      </div>

      <div className="space-y-4">
        <h2 className="text-xl font-semibold">{t('home_recommend_btn')}</h2>
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
                disabled={recLoading}
                onClick={() => handleSelectInspire(idea)}
                title={idea.query}
              >
                {locale === 'zh' ? idea.display_zh : idea.display_en}
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
            <MovieGrid
              items={recResult.recommendations}
              marks={marks}
              onToggleMark={handleToggleMark}
            />
          </div>
        )}
      </div>

      {dailyPicks.length > 0 && (
        <>
          <Separator />
          <div className="space-y-6">
            <h2 className="flex items-center gap-2 text-xl font-semibold">
              <Sparkles className="h-5 w-5 text-accent" />
              {t('home_daily_picks')}
            </h2>
            {dailyPicks.map((section, i) => (
              <div key={i}>
                {i > 0 && <Separator className="mb-6" />}
                <div className="space-y-4">
                  <h3 className="text-lg font-semibold">{locale === 'zh' ? section.inspiration_zh : section.inspiration_en}</h3>
                  <MovieGrid
                    items={section.movies}
                    marks={marks}
                    onToggleMark={handleToggleMark}
                    mobileTrim
                  />
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {mostRelated.length > 0 && (
        <>
          <Separator />
          <div className="space-y-4">
            <h2 className="text-xl font-semibold">{t('home_most_related')}</h2>
            <MovieGrid
              items={mostRelated.map((item) => ({
                movie: item.movie,
                reason: item.reason,
                in_library: false,
                downloading: item.downloading,
              }))}
              marks={marks}
              onToggleMark={handleToggleMark}
              mobileTrim
            />
          </div>
        </>
      )}

      {recentLibrary.length > 0 && (
        <>
          <Separator />
          <div className="space-y-4">
            <h2 className="text-xl font-semibold">{t('home_recent_library')}</h2>
            <MovieGrid
              items={recentLibrary.map((item) => ({
                movie: item.movie,
                in_library: true,
                downloading: item.downloading,
              }))}
              marks={marks}
              onToggleMark={handleToggleMark}
              mobileTrim
            />
          </div>
        </>
      )}
    </div>
  )
}
