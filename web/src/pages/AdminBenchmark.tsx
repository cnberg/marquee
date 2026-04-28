import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../api/client'
import type {
  BenchmarkCompareResponse,
  BenchmarkQuery,
  BenchmarkQueryRunResult,
  BenchmarkRun,
  BenchmarkRunDetail,
  ParsedSseEvent,
  RecommendResult,
  SearchHistoryDetail,
  SearchHistoryItem,
} from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { Card } from '../components/ui/card'
import { Button } from '../components/ui/button'
import { Input } from '../components/ui/input'
import { Textarea } from '../components/ui/textarea'
import { Badge } from '../components/ui/badge'
import { Alert, AlertDescription } from '../components/ui/alert'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '../components/ui/dialog'

type Tab = 'queries' | 'runs'

export default function AdminBenchmark() {
  const { t } = useLocale()
  const [tab, setTab] = useState<Tab>('queries')

  return (
    <div className="space-y-4">
      <div className="space-y-1">
        <h2 className="text-2xl font-semibold">{t('benchmark_title')}</h2>
        <p className="text-sm text-muted-foreground">{t('benchmark_hint')}</p>
      </div>
      <Tabs value={tab} onValueChange={(v) => setTab(v as Tab)}>
        <TabsList>
          <TabsTrigger value="queries">{t('benchmark_tab_queries')}</TabsTrigger>
          <TabsTrigger value="runs">{t('benchmark_tab_runs')}</TabsTrigger>
        </TabsList>
        <TabsContent value="queries" className="mt-4">
          <QueriesPanel />
        </TabsContent>
        <TabsContent value="runs" className="mt-4">
          <RunsPanel />
        </TabsContent>
      </Tabs>
    </div>
  )
}

// ========== Queries ==========

function QueriesPanel() {
  const { t } = useLocale()
  const navigate = useNavigate()
  const [items, setItems] = useState<BenchmarkQuery[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [editing, setEditing] = useState<BenchmarkQuery | 'new' | null>(null)
  const [importing, setImporting] = useState(false)
  const [detail, setDetail] = useState<BenchmarkQuery | null>(null)

  const load = async () => {
    setLoading(true)
    try {
      setItems(await api.benchmarkListQueries())
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
  }, [])

  const handleDelete = async (q: BenchmarkQuery) => {
    if (!window.confirm(t('benchmark_delete_confirm'))) return
    try {
      await api.benchmarkDeleteQuery(q.id)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  if (loading) return <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          {t('benchmark_queries_count').replace('{n}', String(items.length))}
        </p>
        <div className="flex gap-2">
          <Button size="sm" variant="outline" onClick={() => setImporting(true)}>
            {t('benchmark_import_btn')}
          </Button>
          <Button size="sm" onClick={() => setEditing('new')}>
            {t('benchmark_query_add')}
          </Button>
        </div>
      </div>
      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      <div className="space-y-2">
        {items.map((q) => (
          <Card
            key={q.id}
            className="cursor-pointer p-3 hover:bg-accent/50"
            onClick={() => navigate(`/admin/benchmark/queries/${q.id}`)}
          >
            <div className="flex items-start justify-between gap-4">
              <div className="flex-1 space-y-1">
                <div className="font-mono text-sm">{q.query}</div>
                {q.note && (
                  <div className="text-xs text-muted-foreground">{q.note}</div>
                )}
                {q.expected_ids.length > 0 && (
                  <div className="flex flex-wrap gap-1 pt-1">
                    <span className="text-xs text-muted-foreground">
                      {t('benchmark_expected')}:
                    </span>
                    {q.expected_ids.map((id) => (
                      <Badge key={id} variant="secondary" className="text-xs">
                        {id}
                      </Badge>
                    ))}
                  </div>
                )}
                {q.not_expected_ids && q.not_expected_ids.length > 0 && (
                  <div className="flex flex-wrap gap-1 pt-1">
                    <span className="text-xs text-muted-foreground">
                      {t('benchmark_not_expected')}:
                    </span>
                    {q.not_expected_ids.map((id) => (
                      <Badge
                        key={id}
                        variant="destructive"
                        className="text-xs"
                      >
                        {id}
                      </Badge>
                    ))}
                  </div>
                )}
              </div>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={(e) => {
                    e.stopPropagation()
                    setDetail(q)
                  }}
                >
                  {t('benchmark_edit')}
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={(e) => {
                    e.stopPropagation()
                    handleDelete(q)
                  }}
                >
                  {t('benchmark_delete')}
                </Button>
              </div>
            </div>
          </Card>
        ))}
        {items.length === 0 && (
          <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
            {t('benchmark_queries_empty')}
          </div>
        )}
      </div>
      {editing && (
        <QueryEditorDialog
          initial={editing === 'new' ? null : editing}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null)
            load()
          }}
        />
      )}
      {importing && (
        <ImportFromHistoryDialog
          onClose={() => setImporting(false)}
          onSaved={() => {
            setImporting(false)
            load()
          }}
        />
      )}
      {detail && (
        <QueryDetailDialog
          query={detail}
          onClose={() => setDetail(null)}
          onSaved={() => {
            setDetail(null)
            load()
          }}
        />
      )}
    </div>
  )
}

function QueryEditorDialog({
  initial,
  onClose,
  onSaved,
}: {
  initial: BenchmarkQuery | null
  onClose: () => void
  onSaved: () => void
}) {
  const { t } = useLocale()
  const [query, setQuery] = useState(initial?.query ?? '')
  const [note, setNote] = useState(initial?.note ?? '')
  const [expectedRaw, setExpectedRaw] = useState(
    initial ? initial.expected_ids.join(', ') : '',
  )
  const [notExpectedRaw, setNotExpectedRaw] = useState(
    initial && initial.not_expected_ids ? initial.not_expected_ids.join(', ') : '',
  )
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const parseExpected = (raw: string): number[] => {
    return raw
      .split(/[,;\s]+/)
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => Number(s))
      .filter((n) => Number.isFinite(n) && n > 0)
  }

  const handleSave = async () => {
    if (!query.trim()) {
      setError(t('benchmark_query_required'))
      return
    }
    setSaving(true)
    setError(null)
    try {
      const body = {
        query: query.trim(),
        note: note.trim() || undefined,
        expected_ids: parseExpected(expectedRaw),
        not_expected_ids: parseExpected(notExpectedRaw),
      }
      if (initial) {
        await api.benchmarkUpdateQuery(initial.id, body)
      } else {
        await api.benchmarkCreateQuery(body)
      }
      onSaved()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      setSaving(false)
    }
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-xl">
        <DialogHeader>
          <DialogTitle>
            {initial ? t('benchmark_query_edit_title') : t('benchmark_query_new_title')}
          </DialogTitle>
        </DialogHeader>
        <div className="space-y-3">
          <div className="space-y-1">
            <label className="text-sm font-medium">{t('benchmark_query_text')}</label>
            <Textarea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="min-h-20"
              placeholder={t('benchmark_query_placeholder')}
            />
          </div>
          <div className="space-y-1">
            <label className="text-sm font-medium">{t('benchmark_note')}</label>
            <Input value={note} onChange={(e) => setNote(e.target.value)} />
          </div>
          <div className="space-y-1">
            <label className="text-sm font-medium">
              {t('benchmark_expected_ids')}
            </label>
            <Input
              value={expectedRaw}
              onChange={(e) => setExpectedRaw(e.target.value)}
              placeholder="238, 240, 424"
            />
            <p className="text-xs text-muted-foreground">
              {t('benchmark_expected_hint')}
            </p>
          </div>
          <div className="space-y-1">
            <label className="text-sm font-medium">
              {t('benchmark_not_expected_ids')}
            </label>
            <Input
              value={notExpectedRaw}
              onChange={(e) => setNotExpectedRaw(e.target.value)}
              placeholder="99, 100"
            />
            <p className="text-xs text-muted-foreground">
              {t('benchmark_not_expected_hint')}
            </p>
          </div>
          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={saving}>
            {t('benchmark_cancel')}
          </Button>
          <Button onClick={handleSave} disabled={saving}>
            {saving ? t('benchmark_saving') : t('benchmark_save')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function QueryDetailDialog({
  query,
  onClose,
  onSaved,
}: {
  query: BenchmarkQuery
  onClose: () => void
  onSaved: () => void
}) {
  const { t } = useLocale()
  const navigate = useNavigate()
  const [runs, setRuns] = useState<BenchmarkQueryRunResult[]>([])
  const [history, setHistory] = useState<SearchHistoryDetail | null>(null)
  const [loadingRuns, setLoadingRuns] = useState(true)
  const [loadingHistory, setLoadingHistory] = useState(false)

  const [queryText, setQueryText] = useState(query.query)
  const [note, setNote] = useState(query.note ?? '')
  const [expectedRaw, setExpectedRaw] = useState(query.expected_ids.join(', '))
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoadingRuns(true)
    api
      .benchmarkGetQueryRuns(query.id)
      .then(setRuns)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoadingRuns(false))
  }, [query.id])

  useEffect(() => {
    if (query.source_history_id == null) return
    setLoadingHistory(true)
    api
      .getHistory(query.source_history_id)
      .then(setHistory)
      .catch(() => {
        /* 起源 history 找不到不致命 */
      })
      .finally(() => setLoadingHistory(false))
  }, [query.source_history_id])

  const parseExpected = (raw: string): number[] =>
    raw
      .split(/[,;\s]+/)
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => Number(s))
      .filter((n) => Number.isFinite(n) && n > 0)

  const handleSave = async () => {
    if (!queryText.trim()) {
      setError(t('benchmark_query_required'))
      return
    }
    setSaving(true)
    setError(null)
    try {
      await api.benchmarkUpdateQuery(query.id, {
        query: queryText.trim(),
        note: note.trim() || undefined,
        expected_ids: parseExpected(expectedRaw),
      })
      onSaved()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setSaving(false)
    }
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-3xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>{t('benchmark_query_detail_title')}</DialogTitle>
        </DialogHeader>

        <div className="space-y-6">
          <section>
            <h3 className="mb-2 text-sm font-semibold">
              {t('benchmark_query_detail_runs_section')}
            </h3>
            {loadingRuns ? (
              <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>
            ) : runs.length === 0 ? (
              <div className="rounded-md border border-dashed p-3 text-sm text-muted-foreground">
                {t('benchmark_no_runs_yet')}
              </div>
            ) : (
              <div className="space-y-1">
                {runs.map((r) => (
                  <div
                    key={r.run_id}
                    className="flex items-center gap-2 rounded border bg-card/40 p-2 text-sm"
                  >
                    <Badge
                      variant={r.hit === true ? 'default' : r.hit === false ? 'destructive' : 'secondary'}
                    >
                      {r.hit === true
                        ? t('benchmark_hit_yes')
                        : r.hit === false
                        ? t('benchmark_hit_no')
                        : t('benchmark_hit_unknown')}
                    </Badge>
                    <span className="text-xs text-muted-foreground">
                      {t('benchmark_run_at')} {r.run_started_at}
                    </span>
                    {r.elapsed_ms != null && (
                      <span className="text-xs text-muted-foreground">
                        · {t('benchmark_run_elapsed').replace('{ms}', String(r.elapsed_ms))}
                      </span>
                    )}
                    <span className="text-xs text-muted-foreground">
                      · {t('benchmark_run_picks').replace('{n}', String(r.top_movies?.length ?? 0))}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </section>

          <section>
            <h3 className="mb-2 text-sm font-semibold">
              {t('benchmark_query_detail_history_section')}
            </h3>
            {query.source_history_id == null ? (
              <div className="rounded-md border border-dashed p-3 text-sm text-muted-foreground">
                {t('benchmark_no_source_history')}
              </div>
            ) : loadingHistory ? (
              <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>
            ) : history ? (
              <div className="space-y-2 rounded-md border p-3">
                <div className="text-sm">"{history.prompt}"</div>
                <div className="text-xs text-muted-foreground">{history.created_at}</div>
                <Button
                  variant="link"
                  size="sm"
                  className="h-auto p-0"
                  onClick={() => navigate(`/history/${history.id}`)}
                >
                  {t('benchmark_view_history_full')}
                </Button>
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">
                history #{query.source_history_id} (failed to load)
              </div>
            )}
          </section>

          <section>
            <h3 className="mb-2 text-sm font-semibold">
              {t('benchmark_query_detail_expected_section')}
            </h3>
            <div className="space-y-3">
              <div className="space-y-1">
                <label className="text-sm font-medium">{t('benchmark_query_text')}</label>
                <Textarea
                  value={queryText}
                  onChange={(e) => setQueryText(e.target.value)}
                  className="min-h-20"
                />
              </div>
              <div className="space-y-1">
                <label className="text-sm font-medium">{t('benchmark_note')}</label>
                <Input value={note} onChange={(e) => setNote(e.target.value)} />
              </div>
              <div className="space-y-1">
                <label className="text-sm font-medium">{t('benchmark_expected_ids')}</label>
                <Input
                  value={expectedRaw}
                  onChange={(e) => setExpectedRaw(e.target.value)}
                  placeholder="238, 240, 424"
                />
                <p className="text-xs text-muted-foreground">{t('benchmark_expected_hint')}</p>
              </div>
            </div>
          </section>

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={saving}>
            {t('benchmark_cancel')}
          </Button>
          <Button onClick={handleSave} disabled={saving}>
            {saving ? t('benchmark_saving') : t('benchmark_save')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

type PickRow = { tmdb_id: number; title: string; checked: boolean }

function extractPicksFromSseEvents(raw: string): { tmdb_id: number; title: string }[] {
  try {
    const events = JSON.parse(raw) as ParsedSseEvent[]
    const result = events.find((e) => e.event === 'result')
    if (!result?.data) return []
    const data = result.data as RecommendResult
    return data.recommendations
      .map((r) => ({ tmdb_id: r.movie?.tmdb_id ?? 0, title: r.movie?.title ?? '?' }))
      .filter((p) => p.tmdb_id > 0)
  } catch {
    return []
  }
}

function ImportFromHistoryDialog({
  onClose,
  onSaved,
}: {
  onClose: () => void
  onSaved: () => void
}) {
  const { t } = useLocale()
  const [step, setStep] = useState<'select' | 'pick' | 'confirm'>('select')
  const [historyList, setHistoryList] = useState<SearchHistoryItem[]>([])
  const [selectedHistory, setSelectedHistory] = useState<SearchHistoryDetail | null>(null)
  const [picks, setPicks] = useState<PickRow[]>([])
  const [queryText, setQueryText] = useState('')
  const [note, setNote] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    api
      .listHistory(50)
      .then(setHistoryList)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [])

  const handleSelectHistory = async (h: SearchHistoryItem) => {
    setLoading(true)
    setError(null)
    try {
      const detail = await api.getHistory(h.id)
      setSelectedHistory(detail)
      setQueryText(detail.prompt)
      const fromSse = extractPicksFromSseEvents(detail.sse_events)
      setPicks(fromSse.map((p) => ({ ...p, checked: true })))
      setStep('pick')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  const togglePick = (tmdb_id: number) =>
    setPicks((prev) => prev.map((p) => (p.tmdb_id === tmdb_id ? { ...p, checked: !p.checked } : p)))
  const setAllPicks = (checked: boolean) =>
    setPicks((prev) => prev.map((p) => ({ ...p, checked })))

  const handleSave = async () => {
    if (!queryText.trim()) {
      setError(t('benchmark_query_required'))
      return
    }
    setLoading(true)
    setError(null)
    try {
      const expected_ids = picks.filter((p) => p.checked).map((p) => p.tmdb_id)
      await api.benchmarkCreateQuery({
        query: queryText.trim(),
        note: note.trim() || undefined,
        expected_ids,
        source_history_id: selectedHistory?.id,
      })
      onSaved()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setLoading(false)
    }
  }

  const checkedCount = picks.filter((p) => p.checked).length

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>{t('benchmark_import_dialog_title')}</DialogTitle>
        </DialogHeader>

        {step === 'select' && (
          <div className="space-y-3">
            <div>
              <h4 className="text-sm font-semibold">{t('benchmark_import_step_select_title')}</h4>
              <p className="text-xs text-muted-foreground">{t('benchmark_import_step_select_hint')}</p>
            </div>
            {loading ? (
              <div className="text-sm text-muted-foreground">{t('benchmark_import_history_loading')}</div>
            ) : historyList.length === 0 ? (
              <div className="rounded-md border border-dashed p-4 text-sm text-muted-foreground">
                {t('benchmark_import_history_empty')}
              </div>
            ) : (
              <div className="space-y-1">
                {historyList.map((h) => (
                  <button
                    key={h.id}
                    type="button"
                    onClick={() => handleSelectHistory(h)}
                    className="w-full rounded border bg-card/40 p-2 text-left text-sm hover:bg-accent/50"
                  >
                    <div className="font-mono">{h.prompt}</div>
                    <div className="text-xs text-muted-foreground">
                      {h.created_at} · {t('benchmark_import_picks_count').replace('{n}', String(h.result_count))}
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>
        )}

        {step === 'pick' && (
          <div className="space-y-3">
            <div>
              <h4 className="text-sm font-semibold">{t('benchmark_import_step_pick_expected_title')}</h4>
              <p className="text-xs text-muted-foreground">{t('benchmark_import_step_pick_expected_hint')}</p>
            </div>
            <div className="flex gap-2">
              <Button size="sm" variant="outline" onClick={() => setAllPicks(true)}>
                {t('benchmark_import_pick_all')}
              </Button>
              <Button size="sm" variant="outline" onClick={() => setAllPicks(false)}>
                {t('benchmark_import_pick_none')}
              </Button>
              <span className="ml-auto self-center text-xs text-muted-foreground">
                {t('benchmark_import_picks_count').replace('{n}', String(checkedCount))}
              </span>
            </div>
            <div className="space-y-1">
              {picks.map((p) => (
                <label
                  key={p.tmdb_id}
                  className="flex cursor-pointer items-center gap-2 rounded border p-2 text-sm hover:bg-accent/40"
                >
                  <input
                    type="checkbox"
                    checked={p.checked}
                    onChange={() => togglePick(p.tmdb_id)}
                  />
                  <span className="flex-1">{p.title}</span>
                  <Badge variant="secondary" className="text-xs">{p.tmdb_id}</Badge>
                </label>
              ))}
              {picks.length === 0 && (
                <div className="rounded-md border border-dashed p-3 text-sm text-muted-foreground">
                  {t('benchmark_import_history_empty')}
                </div>
              )}
            </div>
          </div>
        )}

        {step === 'confirm' && (
          <div className="space-y-3">
            <div>
              <h4 className="text-sm font-semibold">{t('benchmark_import_step_confirm_title')}</h4>
            </div>
            <div className="space-y-1">
              <label className="text-sm font-medium">{t('benchmark_query_text')}</label>
              <Textarea
                value={queryText}
                onChange={(e) => setQueryText(e.target.value)}
                className="min-h-20"
              />
            </div>
            <div className="space-y-1">
              <label className="text-sm font-medium">{t('benchmark_note')}</label>
              <Input value={note} onChange={(e) => setNote(e.target.value)} />
            </div>
            <div className="rounded border p-3 text-sm text-muted-foreground">
              {t('benchmark_expected')}: {t('benchmark_import_picks_count').replace('{n}', String(checkedCount))}
            </div>
          </div>
        )}

        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={loading}>
            {t('benchmark_cancel')}
          </Button>
          {step === 'pick' && (
            <Button variant="outline" onClick={() => setStep('select')} disabled={loading}>
              {t('benchmark_import_step_back')}
            </Button>
          )}
          {step === 'pick' && (
            <Button onClick={() => setStep('confirm')} disabled={loading}>
              {t('benchmark_import_step_next')}
            </Button>
          )}
          {step === 'confirm' && (
            <Button variant="outline" onClick={() => setStep('pick')} disabled={loading}>
              {t('benchmark_import_step_back')}
            </Button>
          )}
          {step === 'confirm' && (
            <Button onClick={handleSave} disabled={loading}>
              {loading ? t('benchmark_saving') : t('benchmark_import_step_finish')}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// ========== Runs ==========

function RunsPanel() {
  const { t } = useLocale()
  const [runs, setRuns] = useState<BenchmarkRun[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<number | null>(null)
  const [startNote, setStartNote] = useState('')
  const [starting, setStarting] = useState(false)
  const [showStart, setShowStart] = useState(false)

  const runningRun = runs.find((r) => r.status === 'running')

  const load = async () => {
    try {
      setRuns(await api.benchmarkListRuns(50))
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
  }, [])

  useEffect(() => {
    if (!runningRun) return
    const timer = setInterval(load, 3000)
    return () => clearInterval(timer)
  }, [runningRun?.id])

  const handleStart = async () => {
    setStarting(true)
    try {
      const res = await api.benchmarkStartRun(startNote.trim() || undefined)
      setShowStart(false)
      setStartNote('')
      setSelected(res.run_id)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setStarting(false)
    }
  }

  const handleSetBaseline = async (run: BenchmarkRun) => {
    try {
      await api.benchmarkSetBaseline(run.id)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const handleCancel = async (run: BenchmarkRun) => {
    try {
      await api.benchmarkCancelRun(run.id)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  if (loading) return <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>

  if (selected !== null) {
    return <RunDetailPanel runId={selected} onBack={() => setSelected(null)} />
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div className="text-sm text-muted-foreground">
          {runningRun
            ? t('benchmark_running_hint')
            : t('benchmark_run_hint')}
        </div>
        <Button
          size="sm"
          disabled={starting || !!runningRun}
          onClick={() => setShowStart(true)}
        >
          {t('benchmark_start_run')}
        </Button>
      </div>
      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      <div className="space-y-2">
        {runs.map((r) => (
          <Card key={r.id} className="p-3">
            <div className="flex items-start justify-between gap-4">
              <div className="flex-1 space-y-1">
                <div className="flex items-center gap-2">
                  <span className="font-mono text-sm">#{r.id}</span>
                  <StatusBadge status={r.status} />
                  {r.is_baseline && (
                    <Badge variant="secondary">{t('benchmark_baseline')}</Badge>
                  )}
                </div>
                <div className="text-xs text-muted-foreground">
                  {t('benchmark_run_started')}: {r.started_at}
                  {r.finished_at && <> · {t('benchmark_run_finished')}: {r.finished_at}</>}
                </div>
                <div className="text-xs">
                  {t('benchmark_run_progress')}: {r.passed + r.failed} / {r.total} ·{' '}
                  <span className="text-green-600">pass {r.passed}</span> ·{' '}
                  <span className="text-red-600">fail {r.failed}</span>
                </div>
                {r.note && (
                  <div className="text-xs italic text-muted-foreground">{r.note}</div>
                )}
              </div>
              <div className="flex flex-col gap-1">
                <Button variant="outline" size="sm" onClick={() => setSelected(r.id)}>
                  {t('benchmark_view')}
                </Button>
                {r.status === 'running' && (
                  <Button variant="outline" size="sm" onClick={() => handleCancel(r)}>
                    {t('benchmark_cancel_run')}
                  </Button>
                )}
                {r.status === 'done' && !r.is_baseline && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleSetBaseline(r)}
                  >
                    {t('benchmark_set_baseline')}
                  </Button>
                )}
              </div>
            </div>
          </Card>
        ))}
        {runs.length === 0 && (
          <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
            {t('benchmark_runs_empty')}
          </div>
        )}
      </div>
      {showStart && (
        <Dialog open onOpenChange={(open) => !open && setShowStart(false)}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>{t('benchmark_start_run_title')}</DialogTitle>
            </DialogHeader>
            <div className="space-y-3">
              <div className="space-y-1">
                <label className="text-sm font-medium">{t('benchmark_note')}</label>
                <Input
                  value={startNote}
                  onChange={(e) => setStartNote(e.target.value)}
                  placeholder={t('benchmark_note_placeholder')}
                />
              </div>
              <p className="text-xs text-muted-foreground">
                {t('benchmark_start_hint')}
              </p>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setShowStart(false)}>
                {t('benchmark_cancel')}
              </Button>
              <Button onClick={handleStart} disabled={starting}>
                {starting ? t('benchmark_saving') : t('benchmark_run_now')}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      )}
    </div>
  )
}

function StatusBadge({ status }: { status: string }) {
  const variant: 'default' | 'secondary' | 'destructive' | 'outline' =
    status === 'done' ? 'default' : status === 'error' ? 'destructive' : 'secondary'
  return <Badge variant={variant}>{status}</Badge>
}

// ========== Run detail + compare ==========

function RunDetailPanel({ runId, onBack }: { runId: number; onBack: () => void }) {
  const { t } = useLocale()
  const [detail, setDetail] = useState<BenchmarkRunDetail | null>(null)
  const [compare, setCompare] = useState<BenchmarkCompareResponse | null>(null)
  const [showCompare, setShowCompare] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const load = async () => {
    try {
      setDetail(await api.benchmarkGetRun(runId))
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  useEffect(() => {
    load()
  }, [runId])

  useEffect(() => {
    if (!detail || detail.run.status !== 'running') return
    const timer = setInterval(load, 3000)
    return () => clearInterval(timer)
  }, [detail?.run.status])

  const handleCompare = async () => {
    try {
      const res = await api.benchmarkCompareRun(runId)
      setCompare(res)
      setShowCompare(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  if (!detail) return <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>

  const { run, results } = detail

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <Button variant="outline" size="sm" onClick={onBack}>
          ← {t('benchmark_back_to_runs')}
        </Button>
        <div className="flex gap-2">
          {run.status === 'done' && !run.is_baseline && (
            <Button variant="outline" size="sm" onClick={handleCompare}>
              {t('benchmark_compare_baseline')}
            </Button>
          )}
        </div>
      </div>

      <Card className="p-3">
        <div className="flex items-center gap-2">
          <span className="font-mono text-sm">Run #{run.id}</span>
          <StatusBadge status={run.status} />
          {run.is_baseline && (
            <Badge variant="secondary">{t('benchmark_baseline')}</Badge>
          )}
        </div>
        <div className="mt-1 text-xs text-muted-foreground">
          {run.started_at}
          {run.finished_at && <> → {run.finished_at}</>}
        </div>
        <div className="mt-1 text-xs">
          {t('benchmark_run_progress')}: {run.passed + run.failed} / {run.total} ·
          <span className="ml-1 text-green-600">pass {run.passed}</span> ·
          <span className="ml-1 text-red-600">fail {run.failed}</span>
        </div>
        {run.note && <div className="mt-1 text-xs italic">{run.note}</div>}
      </Card>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      <div className="space-y-2">
        {results.map((r) => (
          <Card key={r.id} className="p-3">
            <div className="flex items-start justify-between gap-4">
              <div className="flex-1 space-y-1">
                <div className="font-mono text-sm">{r.query_text}</div>
                {r.expected_ids.length > 0 && (
                  <div className="flex flex-wrap gap-1 text-xs">
                    <span className="text-muted-foreground">
                      {t('benchmark_expected')}:
                    </span>
                    {r.expected_ids.map((id) => (
                      <Badge key={id} variant="outline" className="text-xs">
                        {id}
                      </Badge>
                    ))}
                  </div>
                )}
                <div className="pt-1 text-xs">
                  <span className="text-muted-foreground">
                    {t('benchmark_top_results')}:
                  </span>{' '}
                  {r.top_movies.length === 0 ? (
                    <span className="italic text-muted-foreground">—</span>
                  ) : (
                    r.top_movies.map((m) => (
                      <span
                        key={m.tmdb_id}
                        className="ml-1 inline-block rounded bg-muted px-1.5 py-0.5"
                      >
                        {m.title} <span className="text-muted-foreground">#{m.tmdb_id}</span>
                      </span>
                    ))
                  )}
                </div>
                {r.error && (
                  <div className="text-xs text-red-600">{r.error}</div>
                )}
                {r.intent_json && (
                  <details className="text-xs">
                    <summary className="cursor-pointer text-muted-foreground">
                      {t('benchmark_show_intent')}
                    </summary>
                    <pre className="mt-1 overflow-x-auto rounded bg-muted p-2 font-mono text-[10px]">
                      {JSON.stringify(r.intent_json, null, 2)}
                    </pre>
                  </details>
                )}
              </div>
              <div className="text-right text-xs">
                <HitBadge hit={r.hit} t={t} />
                {r.elapsed_ms !== null && (
                  <div className="mt-1 text-muted-foreground">{r.elapsed_ms}ms</div>
                )}
              </div>
            </div>
          </Card>
        ))}
        {results.length === 0 && run.status === 'running' && (
          <div className="rounded-md border border-dashed p-4 text-center text-sm text-muted-foreground">
            {t('benchmark_running_empty')}
          </div>
        )}
      </div>

      {showCompare && compare && (
        <CompareDialog data={compare} onClose={() => setShowCompare(false)} />
      )}
    </div>
  )
}

function HitBadge({ hit, t }: { hit: boolean | null; t: (k: string) => string }) {
  if (hit === null)
    return (
      <Badge variant="outline" className="text-xs">
        —
      </Badge>
    )
  if (hit)
    return (
      <Badge className="bg-green-600 text-white hover:bg-green-700">
        {t('benchmark_hit')}
      </Badge>
    )
  return <Badge variant="destructive">{t('benchmark_miss')}</Badge>
}

function CompareDialog({
  data,
  onClose,
}: {
  data: BenchmarkCompareResponse
  onClose: () => void
}) {
  const { t } = useLocale()
  const changedCount = data.items.filter(
    (i) => i.added_movies.length > 0 || i.removed_movies.length > 0 || i.intent_changed,
  ).length

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-h-[90vh] max-w-4xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>
            {t('benchmark_compare_title')} — #{data.baseline_run.id} → #{data.current_run.id}
          </DialogTitle>
        </DialogHeader>
        <div className="space-y-3">
          <div className="grid grid-cols-3 gap-2 text-sm">
            <Card className="p-2">
              <div className="text-muted-foreground">{t('benchmark_baseline')}</div>
              <div>#{data.baseline_run.id} · pass {data.baseline_run.passed}</div>
            </Card>
            <Card className="p-2">
              <div className="text-muted-foreground">{t('benchmark_current')}</div>
              <div>#{data.current_run.id} · pass {data.current_run.passed}</div>
            </Card>
            <Card className="p-2">
              <div className="text-muted-foreground">
                {t('benchmark_compare_changed')}
              </div>
              <div>{changedCount} / {data.items.length}</div>
            </Card>
          </div>
          <div className="space-y-2">
            {data.items.map((it) => {
              const unchanged =
                it.added_movies.length === 0 &&
                it.removed_movies.length === 0 &&
                !it.intent_changed
              return (
                <Card
                  key={it.query_id}
                  className={unchanged ? 'p-2 opacity-60' : 'p-2'}
                >
                  <div className="text-sm font-mono">{it.query_text}</div>
                  {unchanged ? (
                    <div className="text-xs text-muted-foreground">
                      {t('benchmark_no_change')}
                    </div>
                  ) : (
                    <div className="mt-1 space-y-1 text-xs">
                      {it.added_movies.length > 0 && (
                        <div>
                          <span className="text-green-600">
                            + {t('benchmark_added')}:
                          </span>
                          {it.added_movies.map((m) => (
                            <span
                              key={m.tmdb_id}
                              className="ml-1 inline-block rounded bg-green-100 px-1.5 py-0.5 dark:bg-green-950"
                            >
                              {m.title} #{m.tmdb_id}
                            </span>
                          ))}
                        </div>
                      )}
                      {it.removed_movies.length > 0 && (
                        <div>
                          <span className="text-red-600">
                            − {t('benchmark_removed')}:
                          </span>
                          {it.removed_movies.map((m) => (
                            <span
                              key={m.tmdb_id}
                              className="ml-1 inline-block rounded bg-red-100 px-1.5 py-0.5 dark:bg-red-950"
                            >
                              {m.title} #{m.tmdb_id}
                            </span>
                          ))}
                        </div>
                      )}
                      {it.intent_changed && (
                        <div className="text-amber-600">
                          ⚠ {t('benchmark_intent_changed')}
                        </div>
                      )}
                      {it.hit_delta !== null && it.hit_delta !== 0 && (
                        <div>
                          {t('benchmark_hit_delta')}: {it.hit_delta > 0 ? '+' : ''}
                          {it.hit_delta}
                        </div>
                      )}
                    </div>
                  )}
                </Card>
              )
            })}
          </div>
        </div>
        <DialogFooter>
          <Button onClick={onClose}>{t('benchmark_close')}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
