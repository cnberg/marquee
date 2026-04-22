import { useCallback, useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import CandidateList from '../components/CandidateList'
import { api } from '../api/client'
import type { PendingDir, TmdbCandidate } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { Card } from '../components/ui/card'
import { Button } from '../components/ui/button'
import { Input } from '../components/ui/input'
import { Badge } from '../components/ui/badge'
import { Alert, AlertDescription } from '../components/ui/alert'
import { ScrollArea } from '../components/ui/scroll-area'

export default function AdminPending() {
  const { t } = useLocale()
  const [pendingDirs, setPendingDirs] = useState<PendingDir[]>([])
  const [pendingLoading, setPendingLoading] = useState(true)
  const [pendingError, setPendingError] = useState<string | null>(null)
  const [selectedDirId, setSelectedDirId] = useState<number | null>(null)

  const [candidates, setCandidates] = useState<TmdbCandidate[]>([])
  const [candidatesLoading, setCandidatesLoading] = useState(false)
  const [candidatesError, setCandidatesError] = useState<string | null>(null)

  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState<TmdbCandidate[]>([])
  const [searchLoading, setSearchLoading] = useState(false)
  const [searchError, setSearchError] = useState<string | null>(null)

  const [bindingId, setBindingId] = useState<number | null>(null)

  const loadPending = useCallback(async (): Promise<PendingDir[]> => {
    setPendingLoading(true)
    setPendingError(null)
    try {
      // Sidebar lists all pending dirs without pagination UI, so override
      // backend's default per_page=20 limit. Real data is a few hundred rows.
      const res = await api.listPending({ per_page: '5000' })
      const list: PendingDir[] = Array.isArray(res) ? res : res?.data ?? []

      setPendingDirs(list)
      setSelectedDirId((current) => {
        if (!list.length) return null
        if (current && list.some((item) => item.dir_id === current)) return current
        return list[0].dir_id
      })

      return list
    } catch (err) {
      setPendingError(err instanceof Error ? err.message : t('pending_load_error'))
      return []
    } finally {
      setPendingLoading(false)
    }
  }, [])

  const loadCandidates = useCallback(async (dirId: number) => {
    setCandidatesLoading(true)
    setCandidatesError(null)
    try {
      const res = await api.getCandidates(dirId)
      const list: TmdbCandidate[] = Array.isArray(res?.candidates) ? res.candidates : Array.isArray(res) ? res : res?.data ?? []
      setCandidates(list)
    } catch (err) {
      setCandidatesError(err instanceof Error ? err.message : t('pending_load_error'))
      setCandidates([])
    } finally {
      setCandidatesLoading(false)
    }
  }, [])

  useEffect(() => {
    loadPending()
  }, [loadPending])

  useEffect(() => {
    if (!selectedDirId) {
      setCandidates([])
      return
    }

    let cancelled = false
    const run = async () => {
      await loadCandidates(selectedDirId)
      if (cancelled) return
    }

    run()

    return () => {
      cancelled = true
    }
  }, [loadCandidates, selectedDirId])

  const selectedDir = useMemo(
    () => pendingDirs.find((item) => item.dir_id === selectedDirId) ?? null,
    [pendingDirs, selectedDirId],
  )

  const handleBind = async (tmdbId: number) => {
    if (!selectedDirId) return
    setBindingId(tmdbId)
    try {
      await api.bind(selectedDirId, tmdbId)
      const updatedList = await loadPending()

      const currentId = selectedDirId
      if (currentId) {
        const stillExists = updatedList.some((item) => item.dir_id === currentId)
        if (stillExists) {
          await loadCandidates(currentId)
        } else {
          setCandidates([])
        }
      }

      setSearchResults([])
    } catch (err) {
      setCandidatesError(err instanceof Error ? err.message : '绑定失败')
    } finally {
      setBindingId(null)
    }
  }

  const handleSearch = async (evt: FormEvent) => {
    evt.preventDefault()
    if (!searchQuery.trim()) return

    setSearchLoading(true)
    setSearchError(null)
    try {
      const res = await api.tmdbSearch(searchQuery.trim())
      const list: TmdbCandidate[] = Array.isArray(res?.results) ? res.results : Array.isArray(res) ? res : res?.data ?? []
      setSearchResults(list)
    } catch (err) {
      setSearchError(err instanceof Error ? err.message : t('pending_search_error'))
      setSearchResults([])
    } finally {
      setSearchLoading(false)
    }
  }

  return (
    <div className="grid gap-4 lg:grid-cols-[320px,1fr]">
      <Card className="p-4">
        <h2 className="text-lg font-semibold">{t('admin_nav_pending')}</h2>
        {pendingLoading && <div className="mt-3 text-sm text-muted-foreground">{t('pending_loading')}</div>}
        {pendingError && (
          <Alert variant="destructive" className="mt-3">
            <AlertDescription>{pendingError}</AlertDescription>
          </Alert>
        )}
        {!pendingLoading && !pendingError && pendingDirs.length === 0 && (
          <div className="mt-3 text-sm text-muted-foreground">{t('pending_empty')}</div>
        )}
        <ScrollArea className="mt-3 h-[520px]">
          <div className="space-y-2">
            {pendingDirs.map((dir) => (
              <button
                key={dir.dir_id}
                type="button"
                className={`w-full rounded-md border px-3 py-2 text-left transition hover:border-primary ${selectedDirId === dir.dir_id ? 'border-primary bg-primary/10' : 'border-border bg-card'}`}
                onClick={() => setSelectedDirId(dir.dir_id)}
              >
                <div className="font-semibold leading-tight">{dir.dir_name}</div>
                <div className="truncate text-xs text-muted-foreground">{dir.dir_path}</div>
                <div className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
                  <span>{dir.match_status}</span>
                  {typeof dir.confidence === 'number' && (
                    <Badge variant="secondary">{t('pending_confidence')} {Math.round(dir.confidence * 100)}%</Badge>
                  )}
                </div>
              </button>
            ))}
          </div>
        </ScrollArea>
      </Card>

      <div className="space-y-4">
        {!pendingLoading && !pendingDirs.length && (
          <div className="rounded-lg border border-dashed bg-muted/20 px-4 py-10 text-center text-sm text-muted-foreground">
            {t('pending_empty')}
          </div>
        )}

        {pendingDirs.length > 0 && selectedDir && (
          <Card className="space-y-4 p-4">
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
              <div className="space-y-1">
                <h2 className="text-xl font-semibold">{selectedDir.dir_name}</h2>
                <div className="text-sm text-muted-foreground">{selectedDir.dir_path}</div>
              </div>
              <Badge variant="secondary">{t('pending_system_candidates')}</Badge>
            </div>

            {candidatesLoading ? (
              <div className="text-sm text-muted-foreground">{t('pending_candidates_loading')}</div>
            ) : candidatesError ? (
              <Alert variant="destructive">
                <AlertDescription>{candidatesError}</AlertDescription>
              </Alert>
            ) : (
              <CandidateList
                candidates={candidates}
                onSelect={(tmdbId) => !bindingId && handleBind(tmdbId)}
                emptyText={t('pending_no_candidates')}
              />
            )}

            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-lg font-semibold">{t('pending_manual_search')}</h3>
                  <p className="text-sm text-muted-foreground">{t('pending_search_hint')}</p>
                </div>
                <Badge variant="outline">{t('pending_search_tab')}</Badge>
              </div>

              <form className="flex flex-col gap-2 sm:flex-row" onSubmit={handleSearch}>
                <Input
                  type="search"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder={t('pending_search_placeholder')}
                  aria-label={t('pending_search_aria')}
                  className="flex-1"
                />
                <Button type="submit" disabled={searchLoading}>
                  {searchLoading ? t('pending_searching') : t('pending_search_btn')}
                </Button>
              </form>

              {searchError && (
                <Alert variant="destructive">
                  <AlertDescription>{searchError}</AlertDescription>
                </Alert>
              )}

              {searchLoading ? (
                <div className="text-sm text-muted-foreground">{t('pending_searching')}</div>
              ) : (
                <CandidateList
                  candidates={searchResults}
                  onSelect={(tmdbId) => !bindingId && handleBind(tmdbId)}
                  emptyText={t('pending_no_search_results')}
                />
              )}
            </div>
          </Card>
        )}
      </div>
    </div>
  )
}
