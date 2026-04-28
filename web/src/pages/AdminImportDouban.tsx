import { useCallback, useEffect, useRef, useState } from 'react'
import { api } from '../api/client'
import { useLocale } from '../i18n/LocaleContext'
import { Card } from '../components/ui/card'
import { Button } from '../components/ui/button'
import { Input } from '../components/ui/input'
import { Alert, AlertDescription } from '../components/ui/alert'
import { Separator } from '../components/ui/separator'

type PendingItem = {
  id: number
  raw_title: string
  parsed_title_zh: string | null
  parsed_title_en: string | null
  year: number | null
  country: string | null
  douban_url: string
  error_msg: string | null
}

type TmdbCandidate = {
  id: number
  title: string
  original_title?: string
  release_date?: string
  popularity?: number
  poster_path?: string
}

type StatusCounts = Record<string, number>

export default function AdminImportDouban() {
  const { t } = useLocale()

  const [busy, setBusy] = useState(false)
  const [importMsg, setImportMsg] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [counts, setCounts] = useState<StatusCounts>({})
  const [pending, setPending] = useState<PendingItem[]>([])
  const fileInputRef = useRef<HTMLInputElement>(null)

  const refreshAll = useCallback(async () => {
    try {
      const [s, p] = await Promise.all([api.doubanStatus(), api.doubanListPending()])
      setCounts(s.counts)
      setPending(p)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'load failed')
    }
  }, [])

  useEffect(() => {
    refreshAll()
  }, [refreshAll])

  // While the import worker is still draining pending rows (matched/created
  // counts climbing, pending shrinking), keep polling. When nothing has
  // changed for a few cycles, back off.
  useEffect(() => {
    const id = setInterval(refreshAll, 5000)
    return () => clearInterval(id)
  }, [refreshAll])

  const handleFile = async (file: File) => {
    setBusy(true)
    setError(null)
    setImportMsg(null)
    try {
      const text = await file.text()
      const res = await api.doubanImport(text)
      setImportMsg(
        t('douban_import_done')
          .replace('{total}', String(res.total_received))
          .replace('{queued}', String(res.newly_queued))
          .replace('{existed}', String(res.already_existed)),
      )
      await refreshAll()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'upload failed')
    } finally {
      setBusy(false)
      if (fileInputRef.current) fileInputRef.current.value = ''
    }
  }

  return (
    <div className="space-y-6">
      <Card className="space-y-4 p-6">
        <div>
          <h2 className="text-2xl font-semibold">{t('douban_import_title')}</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            {t('douban_import_subtitle')}{' '}
            <a
              href="https://github.com/UlyC/DouBanExport"
              target="_blank"
              rel="noreferrer"
              className="text-primary underline-offset-4 hover:underline"
            >
              UlyC/DouBanExport
            </a>
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv,text/csv"
            disabled={busy}
            onChange={(e) => {
              const f = e.target.files?.[0]
              if (f) handleFile(f)
            }}
            className="block text-sm file:mr-3 file:rounded-md file:border-0 file:bg-primary file:px-4 file:py-2 file:text-sm file:font-medium file:text-primary-foreground hover:file:bg-primary/90"
          />
          {busy && <span className="text-sm text-muted-foreground">{t('douban_uploading')}</span>}
        </div>

        {importMsg && (
          <Alert>
            <AlertDescription>{importMsg}</AlertDescription>
          </Alert>
        )}
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        <Separator />

        <CountsRow counts={counts} t={t} />
      </Card>

      <PendingList items={pending} onChanged={refreshAll} t={t} />
    </div>
  )
}

function CountsRow({ counts, t }: { counts: StatusCounts; t: (k: string) => string }) {
  const order = ['matched', 'created', 'pending', 'skipped'] as const
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      {order.map((k) => (
        <div key={k} className="rounded-md border bg-card/40 p-3">
          <div className="text-xs uppercase tracking-wide text-muted-foreground">
            {t(`douban_status_${k}`)}
          </div>
          <div className="mt-1 text-2xl font-semibold">{counts[k] ?? 0}</div>
        </div>
      ))}
    </div>
  )
}

function PendingList({
  items,
  onChanged,
  t,
}: {
  items: PendingItem[]
  onChanged: () => void
  t: (k: string) => string
}) {
  if (items.length === 0) {
    return (
      <Card className="p-6 text-sm text-muted-foreground">
        {t('douban_pending_empty')}
      </Card>
    )
  }

  return (
    <Card className="space-y-3 p-6">
      <h3 className="text-lg font-semibold">
        {t('douban_pending_heading')} ({items.length})
      </h3>
      <p className="text-sm text-muted-foreground">{t('douban_pending_help')}</p>
      <div className="space-y-3">
        {items.map((item) => (
          <PendingRow key={item.id} item={item} onChanged={onChanged} t={t} />
        ))}
      </div>
    </Card>
  )
}

function PendingRow({
  item,
  onChanged,
  t,
}: {
  item: PendingItem
  onChanged: () => void
  t: (k: string) => string
}) {
  const [searching, setSearching] = useState(false)
  const [searchQuery, setSearchQuery] = useState(item.parsed_title_zh ?? item.parsed_title_en ?? item.raw_title)
  const [results, setResults] = useState<TmdbCandidate[] | null>(null)
  const [busy, setBusy] = useState(false)
  const [rowError, setRowError] = useState<string | null>(null)

  const doSearch = async () => {
    if (!searchQuery.trim()) return
    setBusy(true)
    setRowError(null)
    try {
      const res = await api.tmdbSearch(searchQuery.trim())
      setResults(res?.results ?? [])
    } catch (err) {
      setRowError(err instanceof Error ? err.message : 'tmdb search failed')
    } finally {
      setBusy(false)
    }
  }

  const doBind = async (tmdbId: number) => {
    setBusy(true)
    setRowError(null)
    try {
      await api.doubanBindPending(item.id, tmdbId)
      onChanged()
    } catch (err) {
      setRowError(err instanceof Error ? err.message : 'bind failed')
    } finally {
      setBusy(false)
    }
  }

  const doSkip = async () => {
    setBusy(true)
    setRowError(null)
    try {
      await api.doubanSkipPending(item.id)
      onChanged()
    } catch (err) {
      setRowError(err instanceof Error ? err.message : 'skip failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="rounded-md border bg-card/40 p-3">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="font-medium">{item.raw_title}</div>
          <div className="mt-0.5 text-xs text-muted-foreground">
            {[item.year, item.country, <a key="db" href={item.douban_url} target="_blank" rel="noreferrer" className="text-primary hover:underline">{t('douban_view_on_douban')}</a>]
              .filter(Boolean)
              .map((x, i) => (
                <span key={i}>{i > 0 && ' · '}{x}</span>
              ))}
          </div>
          {item.error_msg && (
            <div className="mt-1 text-xs text-amber-600 dark:text-amber-400">{item.error_msg}</div>
          )}
        </div>
        <div className="flex shrink-0 gap-2">
          <Button size="sm" variant="outline" disabled={busy} onClick={() => setSearching((x) => !x)}>
            {t('douban_action_bind')}
          </Button>
          <Button size="sm" variant="ghost" disabled={busy} onClick={doSkip}>
            {t('douban_action_skip')}
          </Button>
        </div>
      </div>

      {searching && (
        <div className="mt-3 space-y-2">
          <div className="flex gap-2">
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') doSearch()
              }}
              placeholder={t('douban_search_placeholder')}
              disabled={busy}
            />
            <Button size="sm" disabled={busy} onClick={doSearch}>
              {t('douban_search_button')}
            </Button>
          </div>
          {results && (
            <div className="space-y-1">
              {results.length === 0 ? (
                <div className="text-xs text-muted-foreground">{t('douban_search_empty')}</div>
              ) : (
                results.slice(0, 8).map((r) => (
                  <button
                    key={r.id}
                    disabled={busy}
                    onClick={() => doBind(r.id)}
                    className="flex w-full items-center gap-2 rounded border bg-background px-2 py-1.5 text-left text-sm hover:bg-accent disabled:opacity-50"
                  >
                    <span className="flex-1 truncate">
                      {r.title}
                      {r.original_title && r.original_title !== r.title && (
                        <span className="ml-1 text-muted-foreground">/ {r.original_title}</span>
                      )}
                      {r.release_date && (
                        <span className="ml-1 text-muted-foreground">({r.release_date.slice(0, 4)})</span>
                      )}
                    </span>
                  </button>
                ))
              )}
            </div>
          )}
        </div>
      )}
      {rowError && <div className="mt-2 text-xs text-destructive">{rowError}</div>}
    </div>
  )
}
