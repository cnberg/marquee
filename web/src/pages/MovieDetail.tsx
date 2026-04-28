import type { ReactNode } from 'react'
import { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api } from '../api/client'
import type { LocateCandidate, MovieDetail as MovieDetailType, MovieReleaseDate } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { copyToClipboard, pickLocalized } from '../lib/utils'
import { useAuth } from '../auth/AuthContext'
import { MovieGrid } from '../components/MovieGrid'
import { MovieMarkButtons } from '../components/MovieMarkButtons'
import { PtDepilerSearchButton } from '../components/PtDepilerSearchButton'
import { Badge } from '../components/ui/badge'
import { Alert, AlertDescription } from '../components/ui/alert'
import { Button } from '../components/ui/button'
import { Separator } from '../components/ui/separator'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '../components/ui/dialog'
import { ChevronDown, ChevronUp, Copy, Sparkles } from 'lucide-react'
import { LinkedReason } from '../components/LinkedReason'

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

function formatMoney(amount?: number): string | null {
  if (!amount || amount === 0) return null
  if (amount >= 1_000_000) return `$${(amount / 1_000_000).toFixed(1)}M`
  if (amount >= 1_000) return `$${(amount / 1_000).toFixed(0)}K`
  return `$${amount}`
}

const TMDB_IMG = 'https://image.tmdb.org/t/p'

const LOADING_TIPS_ZH = [
  // 推荐与发现
  '正在翻阅片库的每一个角落…',
  '让我想想，你可能还会喜欢什么…',
  '在海量影片中寻找那颗遗珠…',
  '正在为你组建最佳片单…',
  '正在检查哪些片子你还没看过…',
  '连导演本人都推荐了这几部…',
  '片荒？不存在的。',
  '正在翻找那些被低估的佳作…',
  '好电影就像老朋友，每次重逢都有新感悟。',
  '正在寻找和你口味最搭的电影…',
  '大师的作品总是值得反复品味。',
  '冷门佳作往往藏在最不起眼的角落。',
  '有些电影你现在不看，以后会后悔。',
  '正在从你的片库里找线索…',
  '关联推荐正在为你连线…',
  // 观影氛围
  '一部好电影值得等待。',
  '别急，好酒沉瓮底。',
  '爆米花准备好了吗？',
  '有些电影注定要在深夜被你发现。',
  '据说选电影比看电影还费时间。',
  '今晚的片单，由你的品味决定。',
  '好电影不怕晚，晚看更有味道。',
  '深夜，是电影最好的伴侣。',
  '找一部电影，治愈一整天的疲惫。',
  '准备好被这个故事打动了吗？',
  '最好的影评，是看完后的沉默。',
  '一杯咖啡，一部电影，一个夜晚。',
  '银幕亮起的瞬间，世界安静了。',
  '有时候一部电影就能改变一整天的心情。',
  '沙发已就位，只差一部好电影。',
  '看电影是一种合法的时间旅行。',
  '每个人心里都有一部看了又看的电影。',
  '今天适合看点不一样的。',
  '有些故事，只有电影才讲得出来。',
  '选一部电影，给自己放个假。',
  // 冷知识
  '你知道吗？最早的电影只有 46 秒。',
  '「Marquee」的意思是影院入口的招牌。',
  'IMAX 胶片一帧比普通胶片大 10 倍。',
  '有的导演一个镜头能重拍 70 次。',
  '手绘动画一秒需要 12 到 24 张画。',
  '最长的电影超过 35 小时。',
  '奥斯卡小金人其实只有 3.85 公斤。',
  '电影诞生的前 30 年，全是默片。',
  '第一部有声电影上映时，观众惊呆了。',
  '彩色电影直到 1930 年代才普及。',
  '胶片时代，剪辑师真的在用剪刀和胶水。',
  '一部 90 分钟的电影大约有 13 万帧画面。',
  '有些经典配乐是导演在片场临时决定的。',
  '最贵的电影道具是一双红宝石拖鞋。',
  '世界上第一家电影院开在巴黎。',
  '分镜画得好的导演，漫画也能出道。',
  '有些导演从不看自己拍完的成片。',
  '电影的诞生比飞机还早了几年。',
  '默片时代的字幕卡是手写的。',
  '电影是唯一能同时调动五感的艺术。',
  // 使用小贴士
  '试试自然语言搜索，比如"适合下雨天的电影"。',
  '你标记的「想看」会影响推荐排序。',
  '点击首页「灵感」按钮，获取观影灵感。',
  '可以把搜索结果分享给朋友看。',
]

const LOADING_TIPS_EN = [
  // Discovery
  'Browsing every corner of the library...',
  'Thinking about what else you might like...',
  'Searching for hidden gems among thousands...',
  'Assembling the perfect watchlist for you...',
  'Checking which ones you haven\'t seen...',
  'Even the director recommended these...',
  'Movie drought? Not on our watch.',
  'Digging up underrated masterpieces...',
  'Great movies are like old friends — always something new to discover.',
  'Finding the perfect match for your taste...',
  'A master\'s work always rewards rewatching.',
  'The best hidden gems hide in the most unlikely places.',
  'Some films you\'ll regret not watching sooner.',
  'Looking for clues in your library...',
  'Connecting the dots for you...',
  // Movie vibes
  'A good movie is worth the wait.',
  'Good things come to those who wait.',
  'Got your popcorn ready?',
  'Some movies are meant to be discovered late at night.',
  'They say picking a movie takes longer than watching one.',
  'Tonight\'s lineup is shaped by your taste.',
  'Great films age like fine wine.',
  'Late night — cinema\'s best companion.',
  'Find a movie to heal the whole day.',
  'Ready to be moved by this story?',
  'The best review is your silence after the credits.',
  'A cup of coffee, a film, one evening.',
  'The moment the screen lights up, the world goes quiet.',
  'Sometimes one movie can change your whole day.',
  'Couch is set. Just need the right movie.',
  'Watching movies is legal time travel.',
  'Everyone has that one film they\'ve seen a dozen times.',
  'Today calls for something different.',
  'Some stories can only be told through cinema.',
  'Pick a movie. Give yourself a break.',
  // Fun facts
  'Did you know? The first movie was only 46 seconds.',
  '"Marquee" means the sign above a theater entrance.',
  'An IMAX frame is 10x larger than standard film.',
  'Some directors do 70 takes for a single shot.',
  'Hand-drawn animation needs 12 to 24 frames per second.',
  'The longest film ever made is over 35 hours.',
  'An Oscar statuette weighs only 8.5 pounds.',
  'For the first 30 years, all movies were silent.',
  'Audiences were stunned when the first "talkie" debuted.',
  'Color film didn\'t become common until the 1930s.',
  'In the film era, editors literally used scissors and glue.',
  'A 90-minute movie has roughly 130,000 frames.',
  'Some iconic scores were decided on set at the last minute.',
  'The most expensive movie prop is a pair of ruby slippers.',
  'The world\'s first cinema opened in Paris.',
  'Some directors\' storyboards could pass as comic books.',
  'Some directors never watch their own finished films.',
  'Cinema was invented a few years before the airplane.',
  'Silent-era title cards were hand-lettered.',
  'Film is the only art that engages all five senses.',
  // Usage tips
  'Try a natural language search, like "movies for a rainy day".',
  'Your "want to watch" marks influence recommendations.',
  'Hit the "Inspire" button on the home page for ideas.',
  'You can share search results with friends.',
]

// Generate dynamic tips based on the current movie's metadata
function buildDynamicTips(
  locale: 'zh' | 'en',
  movie: { title?: string; director?: string; year?: number | null; cast?: string | null } | null,
): string[] {
  if (!movie) return []
  const tips: string[] = []
  const title = movie.title ?? ''
  const director = movie.director ?? ''
  const castList: string[] = []
  try {
    const parsed = JSON.parse(movie.cast ?? '[]')
    if (Array.isArray(parsed)) {
      for (const c of parsed.slice(0, 3)) {
        castList.push(typeof c === 'string' ? c : c?.name ?? '')
      }
    }
  } catch { /* ignore */ }
  const decade = movie.year ? `${Math.floor(movie.year / 10) * 10}` : ''

  if (locale === 'zh') {
    if (director) {
      tips.push(`${director} 的作品总有一种独特的气质…`)
      tips.push(`看看还有哪些 ${director} 的电影你可能会喜欢…`)
      tips.push(`${director} 的影迷一定不会错过这些…`)
    }
    if (castList.length > 0) {
      tips.push(`${castList[0]} 的其他作品也值得一看。`)
      if (castList.length > 1) tips.push(`${castList[0]}和${castList[1]}，这个组合本身就是卖点。`)
    }
    if (decade) {
      tips.push(`${decade} 年代的电影有一种特别的味道。`)
      tips.push(`怀念 ${decade} 年代？这些电影带你回去。`)
    }
    if (title) {
      tips.push(`喜欢《${title}》？那你一定也会喜欢这些…`)
      tips.push(`和《${title}》气质相近的电影还有不少。`)
    }
  } else {
    if (director) {
      tips.push(`${director}'s films always have a unique atmosphere...`)
      tips.push(`Exploring more from ${director} you might enjoy...`)
      tips.push(`Fans of ${director} won't want to miss these...`)
    }
    if (castList.length > 0) {
      tips.push(`${castList[0]}'s other work is worth checking out too.`)
      if (castList.length > 1) tips.push(`${castList[0]} and ${castList[1]} — a combination that sells itself.`)
    }
    if (decade) {
      tips.push(`${decade}s cinema has a flavor all its own.`)
      tips.push(`Missing the ${decade}s? These films take you back.`)
    }
    if (title) {
      tips.push(`Liked "${title}"? You'll probably love these too...`)
      tips.push(`Several films share a similar vibe with "${title}."`)
    }
  }
  return tips
}

function LoadingTips({ locale, movie }: { locale: 'zh' | 'en'; movie?: { title?: string; director?: string; year?: number | null; cast?: string | null } | null }) {
  const tips = useMemo(() => {
    const base = locale === 'zh' ? LOADING_TIPS_ZH : LOADING_TIPS_EN
    const dynamic = buildDynamicTips(locale, movie ?? null)
    const all = [...dynamic, ...base]
    // Shuffle so it's not always dynamic first
    for (let i = all.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1))
      ;[all[i], all[j]] = [all[j], all[i]]
    }
    return all
  }, [locale, movie?.title, movie?.director, movie?.year, movie?.cast])

  const [index, setIndex] = useState(0)
  const [fade, setFade] = useState(true)

  useEffect(() => {
    const timer = setInterval(() => {
      setFade(false)
      setTimeout(() => {
        setIndex((i) => (i + 1) % tips.length)
        setFade(true)
      }, 300)
    }, 2500)
    return () => clearInterval(timer)
  }, [tips.length])

  return (
    <div className="flex flex-col items-center gap-4 px-6 py-12">
      <div className="h-6 w-6 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
      <p
        className="text-sm text-muted-foreground transition-opacity duration-300"
        style={{ opacity: fade ? 1 : 0 }}
      >
        {tips[index]}
      </p>
    </div>
  )
}

function CastLine({ castPairs, t, mobileLimit = 1, desktopLimit = 5 }: {
  castPairs: Array<{ name: string; zhName: string; tmdbPersonId?: number; character?: string | null }>
  t: (key: string) => string
  mobileLimit?: number
  desktopLimit?: number
}) {
  const [expanded, setExpanded] = useState(false)
  const isMd = typeof window !== 'undefined' && window.innerWidth >= 768
  const limit = expanded ? castPairs.length : (isMd ? desktopLimit : mobileLimit)
  const visible = castPairs.slice(0, limit)
  const hasMore = castPairs.length > limit
  return (
    <div className="flex flex-wrap items-baseline gap-x-1">
      <span className="shrink-0 text-muted-foreground">{t('detail_cast')}:</span>
      {visible.map((a, i) => (
        <span key={a.zhName + i}>
          {i > 0 && <span className="text-muted-foreground">、</span>}
          <Link
            to={`/browse?type=cast&value=${encodeURIComponent(a.zhName)}&name=${encodeURIComponent(a.name)}${a.tmdbPersonId ? `&person_id=${a.tmdbPersonId}` : ''}`}
            className="hover:text-foreground"
          >
            {a.name}
          </Link>
        </span>
      ))}
      {hasMore && (
        <button onClick={() => setExpanded(true)} className="text-muted-foreground hover:text-foreground">
          {t('detail_more')}
        </button>
      )}
    </div>
  )
}

function ProductionInfoExpand({ t, locale, collection, budgetStr, revenueStr, productionCompanyNames, spokenLanguages, certification, country, releaseDate, runtime }: {
  t: (key: string) => string
  locale: string
  collection: any
  budgetStr: string | null
  revenueStr: string | null
  productionCompanyNames: string[]
  spokenLanguages: any[]
  certification: string | null
  country?: string | null
  releaseDate?: string | null
  runtime?: number | null
}) {
  const [open, setOpen] = useState(false)
  return (
    <div>
      <button onClick={() => setOpen(!open)} className="flex items-center gap-1 text-muted-foreground hover:text-foreground">
        {t('detail_more')}
        {open ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
      </button>
      {open && (
        <div className="mt-1.5 space-y-1 text-sm">
          {/* Mobile-only: country/release/runtime */}
          {country && (
            <div className="md:hidden"><span className="text-muted-foreground">{t('detail_origin_country')}: </span>{country}</div>
          )}
          {releaseDate && (
            <div className="md:hidden"><span className="text-muted-foreground">{t('detail_release_date')}: </span>{releaseDate}</div>
          )}
          {runtime && (
            <div className="md:hidden"><span className="text-muted-foreground">{t('detail_runtime')}: </span>{runtime} {t('detail_minutes')}</div>
          )}
          {collection && collection.name && (
            <div><span className="text-muted-foreground">{t('detail_collection')}: </span>{collection.name}</div>
          )}
          {budgetStr && (
            <div><span className="text-muted-foreground">{t('detail_budget')}: </span>{budgetStr}</div>
          )}
          {revenueStr && (
            <div><span className="text-muted-foreground">{t('detail_revenue')}: </span>{revenueStr}</div>
          )}
          {productionCompanyNames.length > 0 && (
            <div><span className="text-muted-foreground">{t('detail_production')}: </span>{productionCompanyNames.join(', ')}</div>
          )}
          {spokenLanguages.length > 0 && (
            <div><span className="text-muted-foreground">{t('detail_spoken_lang')}: </span>{spokenLanguages.map(l => locale === 'en' ? (l.english_name || l.name || l.iso_639_1) : (l.name || l.english_name || l.iso_639_1)).filter(Boolean).join(', ')}</div>
          )}
          {certification && (
            <div><span className="text-muted-foreground">{t('detail_release_certification')}: </span>{certification}</div>
          )}
        </div>
      )}
    </div>
  )
}

function CollapsibleSection({ title, defaultOpen = false, children }: { title: string; defaultOpen?: boolean; children: ReactNode }) {
  const [open, setOpen] = useState(defaultOpen)
  const { t } = useLocale()
  return (
    <div>
      <button
        className="flex w-full items-center justify-between py-3 text-left"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
      >
        <span className="text-base font-semibold">{title}</span>
        <span className="flex items-center gap-1 text-sm text-muted-foreground">
          {open ? t('detail_show_less') : t('detail_show_more')}
          {open ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
        </span>
      </button>
      {open && <div className="pb-4">{children}</div>}
    </div>
  )
}

export default function MovieDetail() {
  const { id } = useParams<{ id: string }>()
  const { t, locale } = useLocale()
  const { user, showAuthModal } = useAuth()
  const [movie, setMovie] = useState<MovieDetailType | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [marks, setMarks] = useState({ want: false, watched: false, favorite: false })
  const [aiInsight, setAiInsight] = useState<{ verdict: string | null; picks: Array<{ movie: any; reason: string }> } | null>(null)
  const [aiLoading, setAiLoading] = useState(false)
  const [relatedMarks, setRelatedMarks] = useState<Record<number, { want: boolean; watched: boolean; favorite: boolean }>>({})
  const [locateOpen, setLocateOpen] = useState(false)
  const [locateLoading, setLocateLoading] = useState(false)
  const [locateCandidates, setLocateCandidates] = useState<LocateCandidate[] | null>(null)
  const [locateBindingDirId, setLocateBindingDirId] = useState<number | null>(null)

  const handleLocate = async () => {
    if (!movie) return
    setLocateOpen(true)
    setLocateLoading(true)
    setLocateCandidates(null)
    try {
      const res = await api.locateMovie(movie.id)
      setLocateCandidates(res.candidates)
    } catch {
      setLocateCandidates([])
    } finally {
      setLocateLoading(false)
    }
  }

  const handleLocateBind = async (dirId: number) => {
    if (!movie || locateBindingDirId !== null) return
    setLocateBindingDirId(dirId)
    try {
      await api.bind(dirId, movie.tmdb_id)
      // 简单粗暴 reload——避免手工同步详情页本地文件 / dirPaths / download_status
      window.location.reload()
    } catch {
      setLocateBindingDirId(null)
    }
  }

  const localeStatusKey = (status: string | null): string => {
    if (status === 'pending') return 'detail_locate_status_pending'
    if (status === 'failed') return 'detail_locate_status_failed'
    return 'detail_locate_status_unmapped'
  }

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

  const similar = movie?.similar ?? []
  const recommendations = movie?.recommendations ?? []
  // Merge recommendations + similar, deduplicate by id
  const mergedRelated = useMemo(() => {
    const seen = new Set<number>()
    const result: typeof recommendations = []
    for (const m of [...recommendations, ...similar]) {
      if (!seen.has(m.id)) {
        seen.add(m.id)
        result.push(m)
      }
    }
    return result
  }, [recommendations, similar])

  // Fetch marks for related movies
  const relatedMovieIds = useMemo(() => mergedRelated.map((m) => m.id), [mergedRelated])

  useEffect(() => {
    if (!user || relatedMovieIds.length === 0) {
      setRelatedMarks({})
      return
    }
    api.batchMarks(relatedMovieIds)
      .then((res) => setRelatedMarks(res))
      .catch(() => { /* ignore */ })
  }, [user?.id, relatedMovieIds])

  // Fetch AI insight (async, separate from main load)
  useEffect(() => {
    const movieId = Number(id)
    if (!id || Number.isNaN(movieId)) return
    setAiLoading(true)
    setAiInsight(null)
    api
      .getMovieAiInsight(movieId)
      .then((res) => {
        setAiInsight(res)
        // Batch fetch marks for related movies
        if (user && res.picks.length > 0) {
          const ids = res.picks.map((p: any) => p.movie.id)
          api.batchMarks(ids).then(setRelatedMarks).catch(() => {})
        }
      })
      .catch(() => { /* AI insight is optional */ })
      .finally(() => setAiLoading(false))
  }, [id, user?.id])

  // Bilingual scalars (title/overview/tagline): fall back through the other
  // language so rows that haven't been re-enriched after the 012 migration
  // still render in zh instead of going blank.
  const displayTitle = pickLocalized(locale, movie?.title_en, movie?.title_zh, movie?.title) ?? movie?.title ?? ''
  const displayOverview = pickLocalized(locale, movie?.overview_en, movie?.overview_zh, movie?.overview) ?? undefined
  const displayTagline = pickLocalized(locale, movie?.tagline_en, movie?.tagline_zh) ?? undefined

  useEffect(() => {
    if (movie) {
      const year = movie.year ? ` - ${movie.year}` : ''
      document.title = `${displayTitle}${year} - ${t('detail_title_suffix')}`
    } else {
      document.title = t('detail_title_suffix')
    }
  }, [movie, t, displayTitle])

  // Genre pairs: display uses locale-appropriate name; link `value=` stays zh
  // because the browse filter queries `movies.genres` which only stores zh.
  const genrePairs = useMemo(() => {
    const zh = parseStringList(movie?.genres_zh ?? movie?.genres)
    const en = parseStringList(movie?.genres_en)
    return zh.map((z, i) => ({ zh: z, en: en[i] ?? z }))
  }, [movie?.genres, movie?.genres_zh, movie?.genres_en])

  // Cast pairs (same strategy — link value uses zh name, display uses current locale).
  const castPairs = useMemo(() => {
    const zh = parseCastList(movie?.cast)
    const en = parseCastList(movie?.cast_en)
    return zh.map((z) => {
      const m = z.tmdbPersonId !== undefined
        ? en.find((e) => e.tmdbPersonId === z.tmdbPersonId)
        : undefined
      return {
        name: pickLocalized(locale, m?.name, z.name, z.name) ?? z.name,
        zhName: z.name,
        tmdbPersonId: z.tmdbPersonId,
        character: pickLocalized(locale, m?.character, z.character),
      }
    })
  }, [movie?.cast, movie?.cast_en, locale])

  const directorPairs = useMemo(() => {
    const zh = parseDirectorInfo(movie?.director_info)
    const en = parseDirectorInfo(movie?.director_info_en)
    if (zh.length === 0 && movie?.director) {
      return [{ name: movie.director, zhName: movie.director, tmdbPersonId: undefined as number | undefined }]
    }
    return zh.map((z) => {
      const m = z.tmdbPersonId !== undefined
        ? en.find((e) => e.tmdbPersonId === z.tmdbPersonId)
        : undefined
      return {
        name: pickLocalized(locale, m?.name, z.name, z.name) ?? z.name,
        zhName: z.name,
        tmdbPersonId: z.tmdbPersonId,
      }
    })
  }, [movie?.director_info, movie?.director_info_en, movie?.director, locale])

  const keywordPairs = useMemo(() => {
    const zh = parseStringList(movie?.keywords)
    const en = parseStringList(movie?.keywords_en)
    return zh.map((z, i) => ({ zh: z, en: en[i] ?? z }))
  }, [movie?.keywords, movie?.keywords_en])

  const llmTags = useMemo(() => parseStringList(movie?.llm_tags), [movie?.llm_tags])
  const showOriginalTitle = Boolean(movie?.original_title && movie.original_title !== displayTitle)

  const collection = useMemo(() => {
    const zhCol = parseJsonObject(movie?.collection)
    const enCol = parseJsonObject(movie?.collection_en)
    if (!zhCol && !enCol) return null
    const base = zhCol ?? enCol
    return { ...base, name: pickLocalized(locale, enCol?.name, zhCol?.name, base?.name) }
  }, [movie?.collection, movie?.collection_en, locale])

  // Production companies: zip by index to display locale-appropriate name.
  const productionCompanyNames = useMemo(() => {
    const zh = parseJsonArray(movie?.production_companies)
    const en = parseJsonArray(movie?.production_companies_en)
    return zh
      .map((c: any, i: number) => {
        const enName = en[i]?.name
        return pickLocalized(locale, enName, c?.name, c?.name)
      })
      .filter((n): n is string => Boolean(n))
  }, [movie?.production_companies, movie?.production_companies_en, locale])

  const spokenLanguages = useMemo(() => parseJsonArray(movie?.spoken_languages), [movie?.spoken_languages])
  const dirPaths = movie?.dir_paths ?? []

  const budgetStr = formatMoney(movie?.budget)
  const revenueStr = formatMoney(movie?.revenue)

  // Sub-resources
  const credits = movie?.credits ?? []
  const castCredits = credits.filter(c => c.credit_type === 'cast').sort((a, b) => (a.order ?? 999) - (b.order ?? 999))
  const images = movie?.images ?? []
  const backdrops = images.filter(i => i.image_type === 'backdrop')
  const posters = images.filter(i => i.image_type === 'poster')
  const videos = (movie?.videos ?? []).filter(v => v.site === 'YouTube')
  const reviews = movie?.reviews ?? []
  const releaseDates = movie?.release_dates ?? []
  const externalIds = movie?.external_ids
  const altTitles = movie?.alternative_titles ?? []

  // Group release dates by country (used for certification lookup)
  const rdByCountry = useMemo(() => {
    const map = new Map<string, MovieReleaseDate[]>()
    for (const rd of releaseDates) {
      if (!map.has(rd.iso_3166_1)) map.set(rd.iso_3166_1, [])
      map.get(rd.iso_3166_1)!.push(rd)
    }
    return map
  }, [releaseDates])

  // Earliest release date across all regions, for the header fact row
  const earliestRelease = useMemo(() => {
    let earliest: MovieReleaseDate | null = null
    for (const rd of releaseDates) {
      if (!rd.release_date) continue
      if (!earliest || rd.release_date < earliest.release_date!) earliest = rd
    }
    return earliest
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

  // Build entity list for LinkedReason: movies + directors from current movie + AI picks
  const reasonEntities = useMemo(() => {
    const entities: Array<{ name: string; link: string }> = []
    const seen = new Set<string>()
    const add = (name: string, link: string) => {
      if (!name || name.length < 2 || seen.has(name)) return
      seen.add(name)
      entities.push({ name, link })
    }

    // Current movie titles
    if (movie) {
      if (movie.title) add(movie.title, `/movies/${movie.id}`)
      if (movie.title_zh && movie.title_zh !== movie.title) add(movie.title_zh, `/movies/${movie.id}`)
      if (movie.title_en && movie.title_en !== movie.title) add(movie.title_en, `/movies/${movie.id}`)
      if (movie.original_title && movie.original_title !== movie.title) add(movie.original_title, `/movies/${movie.id}`)
    }

    // Directors
    for (const d of directorPairs) {
      const dirLink = `/browse?type=director&value=${encodeURIComponent(d.zhName)}&name=${encodeURIComponent(d.name)}${d.tmdbPersonId ? `&person_id=${d.tmdbPersonId}` : ''}`
      add(d.name, dirLink)
      if (d.zhName !== d.name) add(d.zhName, dirLink)
    }

    // AI picks: titles + directors
    if (aiInsight) {
      for (const pick of aiInsight.picks) {
        const m = pick.movie
        if (m.title) add(m.title, `/movies/${m.id}`)
        if (m.title_zh && m.title_zh !== m.title) add(m.title_zh, `/movies/${m.id}`)
        if (m.title_en && m.title_en !== m.title) add(m.title_en, `/movies/${m.id}`)
        if (m.director) {
          add(m.director, `/browse?type=director&value=${encodeURIComponent(m.director)}&name=${encodeURIComponent(m.director)}`)
        }
      }
    }

    return entities
  }, [movie, directorPairs, aiInsight])

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

  const formatSize = (size: number) =>
    size > 1024 * 1024 * 1024
      ? `${(size / 1024 / 1024 / 1024).toFixed(1)} GB`
      : `${(size / 1024 / 1024).toFixed(0)} MB`

  const handleToggleRelatedMark = async (movieId: number, markType: 'want' | 'watched' | 'favorite') => {
    if (!user) {
      showAuthModal()
      return
    }
    const prev = relatedMarks[movieId] ?? { want: false, watched: false, favorite: false }
    const isActive = prev[markType]
    const optimistic = { ...prev, [markType]: !isActive }
    if (!isActive && markType === 'want') optimistic.watched = false
    if (!isActive && markType === 'watched') optimistic.want = false
    setRelatedMarks((m) => ({ ...m, [movieId]: optimistic }))
    try {
      const res = isActive ? await api.removeMark(movieId, markType) : await api.setMark(movieId, markType)
      setRelatedMarks((m) => ({ ...m, [movieId]: res }))
    } catch {
      setRelatedMarks((m) => ({ ...m, [movieId]: prev }))
    }
  }


  return (
    <div className="space-y-6">
      <Link to="/" className="inline-flex items-center gap-2 text-sm text-muted-foreground transition hover:text-foreground">
        {t('detail_back')}
      </Link>

      {loading && (
        <div className="flex justify-center px-4 py-8 text-sm text-muted-foreground">
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
          {/* === Layer 1: Title line above poster === */}
          <div>
            <h1 className="text-xl font-semibold leading-tight sm:text-2xl">
              {displayTitle}
              {showOriginalTitle && (
                <span className="font-normal text-muted-foreground"> / {movie.original_title}</span>
              )}
              {movie.year && (
                <span className="font-normal text-muted-foreground"> ({movie.year})</span>
              )}
            </h1>
            {displayTagline && <div className="mt-1 text-sm text-muted-foreground">{displayTagline}</div>}
          </div>

          {/* === Layer 2: Poster + core facts === */}
          <div className="flex gap-4">
              <div className="shrink-0">
                {movie.poster_url ? (
                  <img
                    src={movie.poster_url}
                    alt={displayTitle}
                    className="w-[100px] rounded-md bg-muted/40 object-cover shadow-sm sm:w-[140px] md:w-[160px]"
                  />
                ) : (
                  <div className="flex h-[150px] w-[100px] items-center justify-center rounded-md bg-muted/30 text-xs text-muted-foreground shadow-sm sm:h-[210px] sm:w-[140px] md:h-[240px] md:w-[160px]">
                    {t('card_no_poster')}
                  </div>
                )}
              </div>

              <div className="min-w-0 flex-1 space-y-1.5 text-sm">
                {/* Rating */}
                {typeof movie.tmdb_rating === 'number' && (
                  <div className="flex items-baseline gap-2">
                    <span className="text-lg font-bold sm:text-xl">{movie.tmdb_rating.toFixed(1)}</span>
                    <span className="text-xs text-muted-foreground">
                      {t('detail_tmdb_rating')}
                      {typeof movie.tmdb_votes === 'number' && movie.tmdb_votes > 0 && (
                        <> · {movie.tmdb_votes.toLocaleString()} {t('detail_votes_suffix')}</>
                      )}
                    </span>
                  </div>
                )}

                {/* AI verdict (one-liner) */}
                {aiInsight?.verdict && (
                  <div className="flex items-start gap-1.5 text-sm text-muted-foreground">
                    <Sparkles className="mt-0.5 h-3.5 w-3.5 shrink-0 text-accent" />
                    <span className="min-w-0">
                      <LinkedReason text={aiInsight.verdict} entities={reasonEntities} />
                    </span>
                  </div>
                )}

                {/* Director: localized / original */}
                <div className="flex flex-wrap items-baseline gap-x-1">
                  <span className="shrink-0 text-muted-foreground">{t('detail_director')}:</span>
                  {directorPairs.length > 0
                    ? directorPairs.map((d, i) => (
                        <span key={d.zhName + i}>
                          {i > 0 && <span className="text-muted-foreground">、</span>}
                          <Link
                            to={`/browse?type=director&value=${encodeURIComponent(d.zhName)}&name=${encodeURIComponent(d.name)}${d.tmdbPersonId ? `&person_id=${d.tmdbPersonId}` : ''}`}
                            className="hover:text-foreground"
                          >
                            {d.name !== d.zhName ? `${d.name} / ${d.zhName}` : d.name}
                          </Link>
                        </span>
                      ))
                    : <span>{t('detail_director_unknown')}</span>}
                </div>

                {/* Cast: top 5 + expand */}
                {castPairs.length > 0 && (
                  <CastLine castPairs={castPairs} t={t} />
                )}

                {/* Genre: plain text */}
                {genrePairs.length > 0 && (
                  <div className="flex flex-wrap items-baseline gap-x-1">
                    <span className="shrink-0 text-muted-foreground">{t('browse_genre')}:</span>
                    <span>
                      {genrePairs.map((g, i) => {
                        const label = pickLocalized(locale, g.en, g.zh, g.zh) ?? g.zh
                        return (
                          <span key={g.zh}>
                            {i > 0 && ' / '}
                            <Link
                              to={`/browse?type=genre&value=${encodeURIComponent(g.zh)}&name=${encodeURIComponent(label)}`}
                              className="hover:text-foreground"
                            >
                              {label}
                            </Link>
                          </span>
                        )
                      })}
                    </span>
                  </div>
                )}

                {/* Country — desktop only */}
                {movie.country && (
                  <div className="hidden flex-wrap items-baseline gap-x-1 md:flex">
                    <span className="shrink-0 text-muted-foreground">{t('detail_origin_country')}:</span>
                    <Link
                      to={`/browse?type=country&value=${encodeURIComponent(movie.country)}&name=${encodeURIComponent(movie.country)}`}
                      className="hover:text-foreground"
                    >
                      {movie.country}
                    </Link>
                  </div>
                )}

                {/* Release date — desktop only */}
                {earliestRelease?.release_date && (
                  <div className="hidden items-baseline gap-x-1 md:flex">
                    <span className="shrink-0 text-muted-foreground">{t('detail_release_date')}:</span>
                    <span>{earliestRelease.release_date.split('T')[0]}</span>
                  </div>
                )}

                {/* Runtime — desktop only */}
                {movie.runtime && (
                  <div className="hidden items-baseline gap-x-1 md:flex">
                    <span className="shrink-0 text-muted-foreground">{t('detail_runtime') || t('detail_minutes')}:</span>
                    <span>{movie.runtime} {t('detail_minutes')}</span>
                  </div>
                )}

                {/* More (production info + mobile-only fields) */}
                <ProductionInfoExpand
                  t={t}
                  locale={locale}
                  collection={collection}
                  budgetStr={budgetStr}
                  revenueStr={revenueStr}
                  productionCompanyNames={productionCompanyNames}
                  spokenLanguages={spokenLanguages}
                  certification={certification}
                  country={movie.country}
                  releaseDate={earliestRelease?.release_date?.split('T')[0]}
                  runtime={movie.runtime}
                />
              </div>
            </div>

            {/* === Mark buttons === */}
            <MovieMarkButtons movieId={movie.id} marks={marks} onToggle={handleToggleMark} size="lg" />

            <div className="space-y-4">
                <div className="flex flex-wrap items-center gap-2">
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
                    {(() => {
                      const imdb = movie.imdb_id || externalIds?.imdb_id
                      const query = imdb || movie.title_zh || movie.title
                      if (!query) return null
                      return (
                        <Button asChild variant="outline" size="sm">
                          <a
                            href={`https://search.douban.com/movie/subject_search?search_text=${encodeURIComponent(query)}`}
                            target="_blank"
                            rel="noopener noreferrer"
                          >豆瓣</a>
                        </Button>
                      )
                    })()}
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

                <Separator className="hidden md:block" />

                <div className="hidden space-y-2 rounded-lg bg-card p-4 shadow-md md:block">
                  <div className="text-sm font-semibold">
                    {dirPaths.length > 0
                      ? (movie.download_status && movie.download_status.progress < 1.0
                          ? t('detail_files_downloading')
                          : t('detail_files'))
                      : (movie.download_status && movie.download_status.progress < 1.0
                          ? t('detail_files_downloading')
                          : t('detail_files_none'))}
                  </div>
                  {movie.download_status && movie.download_status.progress < 1.0 && (
                    <div className="rounded-lg bg-secondary/10 p-2">
                      <div className="flex items-center justify-between text-xs">
                        <span className="font-medium text-secondary">
                          {(movie.download_status.progress * 100).toFixed(1)}%
                        </span>
                        <span className="text-muted-foreground">
                          {movie.download_status.dlspeed > 0
                            ? `${(movie.download_status.dlspeed / 1024 / 1024).toFixed(1)} MB/s`
                            : movie.download_status.state}
                        </span>
                      </div>
                      <div className="mt-1.5 h-1 overflow-hidden rounded-full bg-muted">
                        <div
                          className="h-full rounded-full bg-secondary transition-all"
                          style={{ width: `${movie.download_status.progress * 100}%` }}
                        />
                      </div>
                    </div>
                  )}
                  {dirPaths.length > 0 && (
                    <div className="space-y-2">
                      {dirPaths.map((p) => {
                        const dirName = p.split('/').pop() || p
                        return (
                          <div key={p} className="flex flex-wrap items-center gap-2 rounded-lg bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
                            <span className="min-w-0 flex-1 truncate font-mono">{dirName}</span>
                            {movie.download_status && movie.download_status.media_type && movie.download_status.media_type !== 'unknown' && (
                              <span className="shrink-0 rounded bg-muted px-1.5 py-0.5 font-medium">{movie.download_status.media_type}</span>
                            )}
                            {movie.download_status?.size && (
                              <span className="shrink-0">{formatSize(movie.download_status.size)}</span>
                            )}
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              className="h-7 shrink-0 px-2"
                              onClick={(e) => { e.preventDefault(); copyToClipboard(dirName) }}
                              title={t('detail_copy_title')}
                            >
                              <Copy className="h-3.5 w-3.5" />
                            </Button>
                          </div>
                        )
                      })}
                    </div>
                  )}
                  {(dirPaths.length === 0 || movie.imdb_id || externalIds?.imdb_id) && (
                    <div className="flex flex-wrap items-center gap-2 pt-1">
                      {dirPaths.length === 0 && (
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          onClick={handleLocate}
                        >
                          {t('detail_locate_btn')}
                        </Button>
                      )}
                      {(movie.imdb_id || externalIds?.imdb_id) && (
                        <PtDepilerSearchButton imdbId={(movie.imdb_id || externalIds?.imdb_id)!} />
                      )}
                    </div>
                  )}
                </div>
            </div>

          <Separator />

          <div className="space-y-2">
            <h3 className="text-lg font-semibold">{t('detail_overview')}</h3>
            <p className="text-sm leading-relaxed text-muted-foreground whitespace-pre-line">
              {displayOverview || t('detail_overview_empty')}
            </p>
          </div>

          {/* === Related recommendations (AI-powered or fallback) === */}
          {(() => {
            if (aiLoading) {
              return (
                <>
                  <Separator />
                  <LoadingTips locale={locale} movie={movie} />
                </>
              )
            }
            if (aiInsight && aiInsight.picks.length > 0) {
              return (
                <>
                  <Separator />
                  <div className="space-y-2">
                    <h3 className="flex items-center gap-2 text-xl font-semibold">
                      <Sparkles className="h-5 w-5 text-accent" />
                      {t('detail_section_related')}
                    </h3>
                    <MovieGrid
                      items={aiInsight.picks.map((pick) => ({
                        movie: pick.movie,
                        reason: <LinkedReason text={pick.reason} entities={reasonEntities} />,
                        in_library: pick.movie.source !== 'related',
                      }))}
                      marks={relatedMarks}
                      onToggleMark={handleToggleRelatedMark}
                    />
                  </div>
                </>
              )
            }
            if (mergedRelated.length > 0) {
              return (
                <>
                  <Separator />
                  <div className="space-y-2">
                    <h3 className="text-xl font-semibold">{t('detail_section_related')}</h3>
                    <MovieGrid
                      items={mergedRelated.map((m) => ({ movie: m, in_library: m.source === 'library' }))}
                      marks={relatedMarks}
                      onToggleMark={handleToggleRelatedMark}
                    />
                  </div>
                </>
              )
            }
            return null
          })()}

          <div className="space-y-4">
            {castCredits.length > 0 && (
              <>
                <Separator />
                <CollapsibleSection title={t('detail_section_credits')} defaultOpen>
                {(() => {
                  const top16 = castCredits.slice(0, 16)
                  const renderRow = (c: (typeof castCredits)[number]) => {
                    const displayName = pickLocalized(locale, c.person_name_en, c.person_name, c.person_name) ?? c.person_name
                    const displayRole = pickLocalized(locale, c.role_en, c.role)
                    return (
                      <Link
                        key={c.id}
                        to={`/browse?type=cast&value=${encodeURIComponent(c.person_name)}&name=${encodeURIComponent(displayName)}&person_id=${c.tmdb_person_id}`}
                        className="flex items-center gap-3 py-2"
                      >
                        {c.profile_path ? (
                          <img
                            src={`${TMDB_IMG}/w185${c.profile_path}`}
                            alt={displayName}
                            className="aspect-[2/3] w-12 shrink-0 rounded-md bg-muted object-cover"
                          />
                        ) : (
                          <div className="flex aspect-[2/3] w-12 shrink-0 items-center justify-center rounded-md bg-muted text-xs text-muted-foreground">
                            N/A
                          </div>
                        )}
                        <div className="min-w-0 flex-1">
                          <div className="truncate text-sm font-semibold">{displayName}</div>
                          {displayRole && <div className="truncate text-xs text-muted-foreground">{t('detail_credits_as')} {displayRole}</div>}
                        </div>
                      </Link>
                    )
                  }
                  return (
                    <div className="grid grid-cols-2 gap-x-6 md:grid-cols-4">
                      {[0, 1, 2, 3].map((colIdx) => {
                        const col = top16.slice(colIdx * 4, colIdx * 4 + 4)
                        if (col.length === 0) return <div key={colIdx} />
                        return (
                          <div key={colIdx} className="divide-y divide-border">
                            {col.map(renderRow)}
                          </div>
                        )
                      })}
                    </div>
                  )
                })()}
                </CollapsibleSection>
              </>
            )}

            {(keywordPairs.length > 0 || llmTags.length > 0) && (
              <>
                <Separator />
                <CollapsibleSection title={t('detail_keywords')}>
                  <div className="flex flex-wrap gap-1.5">
                    {keywordPairs.map((kw) => {
                      const label = pickLocalized(locale, kw.en, kw.zh, kw.zh) ?? kw.zh
                      return (
                        <Link
                          key={kw.zh}
                          to={`/browse?type=keyword&value=${encodeURIComponent(kw.zh)}&name=${encodeURIComponent(label)}`}
                          className="hover:no-underline"
                        >
                          <Badge variant="secondary" className="text-xs">{label}</Badge>
                        </Link>
                      )
                    })}
                    {llmTags.map((tag) => (
                      <Badge key={tag} variant="outline" className="text-xs">{tag}</Badge>
                    ))}
                  </div>
                </CollapsibleSection>
              </>
            )}

            {videos.length > 0 && (
              <>
                <Separator />
                <CollapsibleSection title={t('detail_section_videos')} defaultOpen>
                  <div className="grid gap-4 md:grid-cols-2">
                    {videos.slice(0, 4).map(v => (
                      <div key={v.id} className="space-y-2">
                        <div className="overflow-hidden rounded-lg shadow-sm">
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
              </>
            )}

            {(backdrops.length > 0 || posters.length > 0) && (
              <>
                <Separator />
                <CollapsibleSection title={t('detail_section_images')}>
                  <div className="space-y-4">
                    {backdrops.length > 0 && (
                      <div className="space-y-2">
                        <h4 className="text-sm font-semibold text-muted-foreground">{t('detail_images_backdrops')}</h4>
                        <div className="flex gap-3 overflow-x-auto pb-2">
                          {backdrops.slice(0, 10).map(img => (
                            <a key={img.id} href={`${TMDB_IMG}/original${img.file_path}`} target="_blank" rel="noopener noreferrer">
                              <img src={`${TMDB_IMG}/w300${img.file_path}`} alt="" className="h-32 rounded-lg bg-muted object-cover shadow-sm" />
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
                              <img src={`${TMDB_IMG}/w185${img.file_path}`} alt="" className="h-44 rounded-lg bg-muted object-cover shadow-sm" />
                            </a>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                </CollapsibleSection>
              </>
            )}

            {reviews.length > 0 && (
              <>
                <Separator />
                <CollapsibleSection title={t('detail_section_reviews')}>
                  <div className="space-y-3">
                    {reviews.slice(0, 5).map(r => (
                      <div key={r.id} className="rounded-lg bg-card/70 p-4 shadow-sm">
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
              </>
            )}

            {altTitles.length > 0 && (
              <>
                <Separator />
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
              </>
            )}
          </div>
        </>
      )}

      <Dialog open={locateOpen} onOpenChange={setLocateOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>{t('detail_locate_modal_title')}</DialogTitle>
          </DialogHeader>
          {locateLoading && (
            <p className="text-sm text-muted-foreground">{t('detail_locate_loading')}</p>
          )}
          {!locateLoading && locateCandidates && locateCandidates.length === 0 && (
            <p className="text-sm text-muted-foreground">{t('detail_locate_empty')}</p>
          )}
          {!locateLoading && locateCandidates && locateCandidates.length > 0 && (
            <div className="space-y-2">
              {locateCandidates.map((c) => (
                <div
                  key={c.dir_id}
                  className="rounded-lg border border-border bg-card p-3 text-sm space-y-1"
                >
                  <div className="font-mono text-xs break-all">{c.dir_name}</div>
                  <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                    <span>
                      {t('detail_locate_parsed_as')}: <span className="font-medium text-foreground">{c.parsed_title}</span>
                      {c.parsed_year ? ` (${c.parsed_year})` : ''}
                    </span>
                    <span>·</span>
                    <span>{t('detail_locate_score')}: {c.score.toFixed(2)}</span>
                    <span>·</span>
                    <span className="rounded bg-muted px-1.5 py-0.5">
                      {t(localeStatusKey(c.status))}
                    </span>
                  </div>
                  <div className="flex justify-end pt-1">
                    <Button
                      type="button"
                      size="sm"
                      onClick={() => handleLocateBind(c.dir_id)}
                      disabled={locateBindingDirId !== null}
                    >
                      {locateBindingDirId === c.dir_id
                        ? t('detail_locate_bind_success')
                        : t('detail_locate_bind')}
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          )}
          <DialogFooter>
            <Button type="button" variant="ghost" onClick={() => setLocateOpen(false)}>
              {t('detail_locate_close')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
