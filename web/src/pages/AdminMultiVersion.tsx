import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client'
import { useLocale } from '../i18n/LocaleContext'
import { Card } from '../components/ui/card'
import { Button } from '../components/ui/button'
import { Badge } from '../components/ui/badge'
import { Alert, AlertDescription } from '../components/ui/alert'
import type { MultiVersionDir, MultiVersionMovie } from '../types'

const PAGE_SIZE = 50

function formatBytes(n: number | null): string {
  if (n === null || n === undefined) return '—'
  if (n < 1024) return `${n} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let v = n / 1024
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v.toFixed(2)} ${units[i]}`
}

function formatProgress(p: number | null): string {
  if (p === null || p === undefined) return '—'
  return `${(p * 100).toFixed(0)}%`
}

function sourceLabel(t: (k: string) => string, source: string | null): string {
  if (source === 'qbittorrent') return t('admin_multiver_source_qbittorrent')
  if (source === 'local') return t('admin_multiver_source_local')
  return source ?? '—'
}

function DirRow({ dir }: { dir: MultiVersionDir }) {
  const { t } = useLocale()
  return (
    <tr className="border-t border-border align-top">
      <td className="px-3 py-2 font-mono text-xs">
        {dir.dir_name}
        {dir.torrent_name && dir.torrent_name !== dir.dir_name && (
          <div className="mt-0.5 text-[11px] text-muted-foreground">
            torrent: {dir.torrent_name}
          </div>
        )}
      </td>
      <td className="px-3 py-2 whitespace-nowrap">
        <Badge variant={dir.source === 'qbittorrent' ? 'secondary' : 'outline'}>
          {sourceLabel(t, dir.source)}
        </Badge>
      </td>
      <td className="px-3 py-2 whitespace-nowrap">{dir.media_type ?? '—'}</td>
      <td className="px-3 py-2 whitespace-nowrap text-right tabular-nums">
        {formatBytes(dir.size_bytes)}
      </td>
      <td className="px-3 py-2 whitespace-nowrap tabular-nums">
        {formatProgress(dir.torrent_progress)}
        {dir.torrent_state && (
          <div className="text-[11px] text-muted-foreground">{dir.torrent_state}</div>
        )}
      </td>
      <td className="px-3 py-2 whitespace-nowrap">
        <Badge variant={dir.match_status === 'auto' ? 'default' : 'secondary'}>
          {dir.match_status}
          {dir.match_confidence !== null && dir.match_confidence !== undefined && (
            <span className="ml-1 opacity-70">{dir.match_confidence.toFixed(2)}</span>
          )}
        </Badge>
      </td>
      <td className="px-3 py-2 font-mono text-[11px] text-muted-foreground break-all">
        {dir.dir_path}
      </td>
    </tr>
  )
}

function MovieGroup({ group }: { group: MultiVersionMovie }) {
  const { t } = useLocale()
  const { movie, version_count, dirs } = group
  const titleZh = movie.title_zh && movie.title_zh !== movie.title ? movie.title_zh : null
  return (
    <Card className="overflow-hidden p-0">
      <div className="flex items-start gap-4 border-b border-border bg-muted/30 p-4">
        <Link to={`/movies/${movie.id}`} className="shrink-0">
          {movie.poster_url ? (
            <img
              src={movie.poster_url}
              alt={movie.title}
              className="h-24 w-16 rounded object-cover"
              loading="lazy"
            />
          ) : (
            <div className="h-24 w-16 rounded bg-muted" />
          )}
        </Link>
        <div className="flex-1">
          <Link
            to={`/movies/${movie.id}`}
            className="text-lg font-semibold hover:underline"
          >
            {movie.title}
          </Link>
          {titleZh && (
            <span className="ml-2 text-base text-muted-foreground">{titleZh}</span>
          )}
          {movie.year && (
            <span className="ml-2 text-sm text-muted-foreground">({movie.year})</span>
          )}
          <div className="mt-1">
            <Badge variant="secondary">
              {t('admin_multiver_versions', { n: version_count })}
            </Badge>
          </div>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-muted/20">
            <tr>
              <th className="px-3 py-2 text-left font-medium">{t('admin_multiver_col_dir')}</th>
              <th className="px-3 py-2 text-left font-medium">{t('admin_multiver_col_source')}</th>
              <th className="px-3 py-2 text-left font-medium">{t('admin_multiver_col_type')}</th>
              <th className="px-3 py-2 text-right font-medium">{t('admin_multiver_col_size')}</th>
              <th className="px-3 py-2 text-left font-medium">{t('admin_multiver_col_progress')}</th>
              <th className="px-3 py-2 text-left font-medium">{t('admin_multiver_col_match')}</th>
              <th className="px-3 py-2 text-left font-medium">{t('admin_multiver_col_path')}</th>
            </tr>
          </thead>
          <tbody>
            {dirs.map((d) => (
              <DirRow key={d.dir_id} dir={d} />
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  )
}

export default function AdminMultiVersion() {
  const { t } = useLocale()
  const [items, setItems] = useState<MultiVersionMovie[]>([])
  const [total, setTotal] = useState(0)
  const [offset, setOffset] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    api
      .adminMultiVersion({ limit: PAGE_SIZE, offset })
      .then((res) => {
        if (cancelled) return
        setItems(res.items)
        setTotal(res.total)
      })
      .catch((e) => {
        if (cancelled) return
        setError(e instanceof Error ? e.message : t('admin_multiver_error'))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [offset, t])

  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-2xl font-semibold">{t('admin_multiver_title')}</h2>
        <p className="mt-1 text-sm text-muted-foreground">{t('admin_multiver_subtitle')}</p>
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {loading && (
        <Card className="p-4 text-sm text-muted-foreground">
          {t('admin_multiver_loading')}
        </Card>
      )}

      {!loading && !error && total === 0 && (
        <Card className="p-4 text-sm text-muted-foreground">
          {t('admin_multiver_empty')}
        </Card>
      )}

      {!loading && total > 0 && (
        <>
          <div className="text-sm text-muted-foreground">
            {t('admin_multiver_summary', { total, count: items.length })}
          </div>
          <div className="space-y-4">
            {items.map((g) => (
              <MovieGroup key={g.movie.id} group={g} />
            ))}
          </div>
          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-3 pt-2">
              <Button
                variant="outline"
                size="sm"
                disabled={offset === 0}
                onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
              >
                {t('admin_multiver_prev')}
              </Button>
              <span className="text-sm tabular-nums">
                {t('admin_multiver_page', { current: currentPage, total: totalPages })}
              </span>
              <Button
                variant="outline"
                size="sm"
                disabled={offset + PAGE_SIZE >= total}
                onClick={() => setOffset(offset + PAGE_SIZE)}
              >
                {t('admin_multiver_next')}
              </Button>
            </div>
          )}
        </>
      )}
    </div>
  )
}
