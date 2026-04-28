import { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { api } from '../api/client'
import type {
  BenchmarkAggregateMovie,
  BenchmarkAggregateResponse,
  BenchmarkMovieAppearance,
} from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { Card } from '../components/ui/card'
import { Button } from '../components/ui/button'
import { Alert, AlertDescription } from '../components/ui/alert'

const PAGE_SIZE = 50

export default function AdminBenchmarkDetail() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const { t } = useLocale()

  const queryId = Number(id)
  const [data, setData] = useState<BenchmarkAggregateResponse | null>(null)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [savedNotice, setSavedNotice] = useState<string | null>(null)
  // 「应该」/「不应」working sets——独立于服务器状态。两组互斥，由 toggle 维护。
  const [selected, setSelected] = useState<Set<number>>(new Set())
  const [negated, setNegated] = useState<Set<number>>(new Set())
  // Track which row is expanded plus its appearance data.
  const [expandedTmdbId, setExpandedTmdbId] = useState<number | null>(null)
  const [appearances, setAppearances] = useState<BenchmarkMovieAppearance[] | null>(null)
  const [appearancesLoading, setAppearancesLoading] = useState(false)

  // 缓存原始服务端值，用于显示 +X/-Y diff（两组各自）。
  const [originalExpected, setOriginalExpected] = useState<Set<number>>(new Set())
  const [originalNegated, setOriginalNegated] = useState<Set<number>>(new Set())
  const [seeded, setSeeded] = useState(false)

  const load = async (targetPage: number) => {
    if (!Number.isFinite(queryId)) return
    setLoading(true)
    setError(null)
    try {
      const resp = await api.benchmarkGetAggregate(queryId, targetPage, PAGE_SIZE)
      setData(resp)
      // 仅首次 load 用 query 的 expected/not_expected 初始化勾选。
      if (!seeded) {
        const initExpected = new Set(resp.query.expected_ids)
        const initNegated = new Set(resp.query.not_expected_ids)
        setSelected(initExpected)
        setOriginalExpected(initExpected)
        setNegated(initNegated)
        setOriginalNegated(initNegated)
        setSeeded(true)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load(page)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryId, page])

  // 互斥 toggle：勾「应该」时若已勾「不应」→ 自动解除「不应」（反之亦然）。
  const toggleExpected = (tmdbId: number) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(tmdbId)) {
        next.delete(tmdbId)
      } else {
        next.add(tmdbId)
        setNegated((negPrev) => {
          if (!negPrev.has(tmdbId)) return negPrev
          const negNext = new Set(negPrev)
          negNext.delete(tmdbId)
          return negNext
        })
      }
      return next
    })
  }

  const toggleNegated = (tmdbId: number) => {
    setNegated((prev) => {
      const next = new Set(prev)
      if (next.has(tmdbId)) {
        next.delete(tmdbId)
      } else {
        next.add(tmdbId)
        setSelected((selPrev) => {
          if (!selPrev.has(tmdbId)) return selPrev
          const selNext = new Set(selPrev)
          selNext.delete(tmdbId)
          return selNext
        })
      }
      return next
    })
  }

  const expandRow = async (tmdbId: number) => {
    if (expandedTmdbId === tmdbId) {
      setExpandedTmdbId(null)
      setAppearances(null)
      return
    }
    setExpandedTmdbId(tmdbId)
    setAppearances(null)
    setAppearancesLoading(true)
    try {
      const resp = await api.benchmarkGetMovieAppearances(queryId, tmdbId)
      setAppearances(resp.appearances)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setAppearancesLoading(false)
    }
  }

  const diff = useMemo(() => {
    const setDiff = (current: Set<number>, original: Set<number>) => {
      let added = 0
      let removed = 0
      current.forEach((id) => {
        if (!original.has(id)) added++
      })
      original.forEach((id) => {
        if (!current.has(id)) removed++
      })
      return { added, removed }
    }
    const exp = setDiff(selected, originalExpected)
    const neg = setDiff(negated, originalNegated)
    return {
      expected: exp,
      negated: neg,
      total: exp.added + exp.removed + neg.added + neg.removed,
    }
  }, [selected, negated, originalExpected, originalNegated])

  const handleSave = async () => {
    if (!data) return
    setSaving(true)
    setError(null)
    setSavedNotice(null)
    try {
      const expected_ids = Array.from(selected).sort((a, b) => a - b)
      const not_expected_ids = Array.from(negated).sort((a, b) => a - b)
      await api.benchmarkUpdateQuery(queryId, {
        query: data.query.query,
        note: data.query.note ?? undefined,
        expected_ids,
        not_expected_ids,
      })
      setOriginalExpected(new Set(expected_ids))
      setOriginalNegated(new Set(not_expected_ids))
      setSavedNotice(t('benchmark_detail_saved'))
      // Reload current page to reflect new is_expected / is_not_expected flags + sort.
      await load(page)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setSaving(false)
    }
  }

  if (!Number.isFinite(queryId)) {
    return <div className="text-sm text-muted-foreground">{t('benchmark_detail_invalid_id')}</div>
  }

  if (loading && !data) {
    return <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>
  }

  if (error && !data) {
    return (
      <Alert variant="destructive">
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    )
  }

  if (!data) return null

  const totalPages = Math.max(1, Math.ceil(data.total_movies / data.page_size))
  const sourceHistoryLink = data.query.source_history_id
    ? `/history/${data.query.source_history_id}`
    : null

  return (
    <div className="space-y-4 pb-24">
      {/* Header */}
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-1">
          <Button variant="ghost" size="sm" onClick={() => navigate('/admin/benchmark')}>
            ← {t('benchmark_detail_back')}
          </Button>
          <h2 className="text-2xl font-semibold">{data.query.query}</h2>
          {data.query.note && (
            <div className="text-sm text-muted-foreground">{data.query.note}</div>
          )}
          <div className="flex flex-wrap items-center gap-3 pt-1 text-xs text-muted-foreground">
            <span>
              {t('benchmark_detail_history_count').replace('{n}', String(data.history_count))}
            </span>
            <span>
              {t('benchmark_detail_total_movies').replace('{n}', String(data.total_movies))}
            </span>
            {sourceHistoryLink && (
              <Link to={sourceHistoryLink} className="text-blue-600 hover:underline">
                {t('benchmark_detail_source_history').replace(
                  '{id}',
                  String(data.query.source_history_id),
                )}
              </Link>
            )}
          </div>
        </div>
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {savedNotice && (
        <Alert>
          <AlertDescription>{savedNotice}</AlertDescription>
        </Alert>
      )}

      {/* Movie table */}
      {data.movies.length === 0 ? (
        <Card className="p-6 text-center text-sm text-muted-foreground">
          {t('benchmark_detail_empty')}
        </Card>
      ) : (
        <Card className="overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-muted/40 text-xs uppercase text-muted-foreground">
              <tr>
                <th className="px-3 py-2 text-left">{t('benchmark_detail_col_check')}</th>
                <th className="px-3 py-2 text-left">{t('benchmark_detail_col_negate')}</th>
                <th className="px-3 py-2 text-left">{t('benchmark_detail_col_poster')}</th>
                <th className="px-3 py-2 text-left">{t('benchmark_detail_col_title')}</th>
                <th className="px-3 py-2 text-left">{t('benchmark_detail_col_tmdb')}</th>
                <th className="px-3 py-2 text-right">{t('benchmark_detail_col_appearances')}</th>
                <th className="px-3 py-2 text-right">{t('benchmark_detail_col_best_rank')}</th>
                <th className="px-3 py-2 text-right">{t('benchmark_detail_col_avg_rank')}</th>
                <th className="px-3 py-2 text-left">{t('benchmark_detail_col_latest')}</th>
              </tr>
            </thead>
            <tbody>
              {data.movies.map((m) => (
                <MovieRow
                  key={m.tmdb_id}
                  movie={m}
                  checked={selected.has(m.tmdb_id)}
                  negated={negated.has(m.tmdb_id)}
                  onToggleExpected={() => toggleExpected(m.tmdb_id)}
                  onToggleNegated={() => toggleNegated(m.tmdb_id)}
                  expanded={expandedTmdbId === m.tmdb_id}
                  appearances={expandedTmdbId === m.tmdb_id ? appearances : null}
                  appearancesLoading={
                    expandedTmdbId === m.tmdb_id && appearancesLoading
                  }
                  onExpand={() => expandRow(m.tmdb_id)}
                />
              ))}
            </tbody>
          </table>
        </Card>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <div className="text-xs text-muted-foreground">
            {t('benchmark_detail_pagination')
              .replace('{page}', String(data.page))
              .replace('{total}', String(totalPages))}
          </div>
          <div className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              disabled={page <= 1 || loading}
              onClick={() => setPage((p) => Math.max(1, p - 1))}
            >
              {t('benchmark_detail_prev')}
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={page >= totalPages || loading}
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            >
              {t('benchmark_detail_next')}
            </Button>
          </div>
        </div>
      )}

      {/* Sticky save bar */}
      <div className="fixed bottom-0 left-0 right-0 z-30 border-t bg-background/95 px-4 py-3 shadow-lg backdrop-blur">
        <div className="mx-auto flex max-w-screen-xl items-center justify-between gap-4">
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm">
            <span className="font-medium">
              {t('benchmark_detail_selected').replace('{n}', String(selected.size))}
            </span>
            <span className="font-medium">
              {t('benchmark_detail_negated_count').replace('{n}', String(negated.size))}
            </span>
            {diff.total > 0 && (
              <span className="text-muted-foreground">
                {t('benchmark_detail_diff_label')}
                {diff.expected.added > 0 && ` ✓+${diff.expected.added}`}
                {diff.expected.removed > 0 && ` ✓-${diff.expected.removed}`}
                {diff.negated.added > 0 && ` ✗+${diff.negated.added}`}
                {diff.negated.removed > 0 && ` ✗-${diff.negated.removed}`}
              </span>
            )}
          </div>
          <Button onClick={handleSave} disabled={saving || diff.total === 0}>
            {saving ? t('benchmark_detail_saving') : t('benchmark_detail_save')}
          </Button>
        </div>
      </div>
    </div>
  )
}

function MovieRow({
  movie,
  checked,
  negated,
  onToggleExpected,
  onToggleNegated,
  expanded,
  appearances,
  appearancesLoading,
  onExpand,
}: {
  movie: BenchmarkAggregateMovie
  checked: boolean
  negated: boolean
  onToggleExpected: () => void
  onToggleNegated: () => void
  expanded: boolean
  appearances: BenchmarkMovieAppearance[] | null
  appearancesLoading: boolean
  onExpand: () => void
}) {
  const { t } = useLocale()
  const title = movie.title_zh || movie.title || movie.title_en
  const poster = movie.poster_url
  const yearStr = movie.year ? ` (${movie.year})` : ''
  // Internal movies/:id route requires the local DB id; out-of-library rows
  // (movie_id = null) have no detail page and stay non-clickable.
  const detailHref = movie.movie_id != null ? `/movies/${movie.movie_id}` : null

  const titleNode = title
    ? `${title}${yearStr}`
    : t('benchmark_detail_unknown_title')
  const titleCell = detailHref ? (
    <Link
      to={detailHref}
      target="_blank"
      rel="noopener noreferrer"
      className={
        title
          ? 'hover:underline'
          : 'text-muted-foreground hover:underline'
      }
    >
      {titleNode}
    </Link>
  ) : (
    <span className={title ? '' : 'text-muted-foreground'}>{titleNode}</span>
  )

  const posterNode = poster ? (
    <img
      src={poster}
      alt=""
      loading="lazy"
      className="h-12 w-8 rounded object-cover"
    />
  ) : (
    <div className="h-12 w-8 rounded bg-muted" />
  )
  const posterCell = detailHref ? (
    <Link to={detailHref} target="_blank" rel="noopener noreferrer">
      {posterNode}
    </Link>
  ) : (
    posterNode
  )

  return (
    <>
      <tr
        className={`border-t hover:bg-accent/30 ${
          negated ? 'bg-destructive/10' : ''
        }`}
      >
        <td className="px-3 py-2">
          <input
            type="checkbox"
            checked={checked}
            onChange={onToggleExpected}
            aria-label={t('benchmark_detail_col_check')}
          />
        </td>
        <td className="px-3 py-2">
          <input
            type="checkbox"
            checked={negated}
            onChange={onToggleNegated}
            aria-label={t('benchmark_detail_col_negate')}
            className="accent-red-500"
          />
        </td>
        <td className="px-3 py-2">{posterCell}</td>
        <td className="px-3 py-2">
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={onExpand}
              className="text-muted-foreground hover:text-foreground"
              aria-label={t('benchmark_detail_expand_history')}
              title={t('benchmark_detail_expand_history')}
            >
              {expanded ? (
                <ChevronDown className="h-4 w-4" />
              ) : (
                <ChevronRight className="h-4 w-4" />
              )}
            </button>
            {titleCell}
          </div>
        </td>
        <td className="px-3 py-2 font-mono text-xs">{movie.tmdb_id}</td>
        <td className="px-3 py-2 text-right">{movie.appearance_count}</td>
        <td className="px-3 py-2 text-right">
          {movie.best_rank ?? '—'}
        </td>
        <td className="px-3 py-2 text-right">
          {movie.avg_rank != null ? movie.avg_rank.toFixed(1) : '—'}
        </td>
        <td className="px-3 py-2 text-xs text-muted-foreground">
          {movie.latest_at ? new Date(movie.latest_at + 'Z').toLocaleString() : '—'}
        </td>
      </tr>
      {expanded && (
        <tr className="border-t bg-muted/20">
          <td colSpan={9} className="px-6 py-3">
            {appearancesLoading ? (
              <div className="text-xs text-muted-foreground">{t('errors_loading')}</div>
            ) : appearances && appearances.length > 0 ? (
              <ul className="space-y-1 text-xs">
                {appearances.map((a) => (
                  <li key={`${a.history_id}-${a.rank}`}>
                    <Link
                      to={`/history/${a.history_id}`}
                      className="text-blue-600 hover:underline"
                    >
                      #{a.history_id}
                    </Link>
                    <span className="ml-2">
                      {t('benchmark_detail_appearance_rank').replace('{rank}', String(a.rank))}
                    </span>
                    <span className="ml-2 text-muted-foreground">
                      {new Date(a.created_at + 'Z').toLocaleString()}
                    </span>
                  </li>
                ))}
              </ul>
            ) : (
              <div className="text-xs text-muted-foreground">
                {t('benchmark_detail_no_appearances')}
              </div>
            )}
          </td>
        </tr>
      )}
    </>
  )
}
