import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { api } from '../api/client'
import { useAuth } from '../auth/AuthContext'
import { useLocale } from '../i18n/LocaleContext'
import type { SearchHistoryItem } from '../types'
import { Button } from '../components/ui/button'
import { Card } from '../components/ui/card'
import { Alert, AlertDescription } from '../components/ui/alert'

export default function SearchHistory() {
  const { t } = useLocale()
  const { user, showAuthModal } = useAuth()
  const navigate = useNavigate()
  const [items, setItems] = useState<SearchHistoryItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [hasMore, setHasMore] = useState(false)

  const PAGE = 20

  useEffect(() => { document.title = t('history_title') }, [t])

  useEffect(() => {
    if (!user) {
      showAuthModal()
      navigate('/')
      return
    }
    loadFirst()
  }, [user?.id])

  const loadFirst = async () => {
    setLoading(true)
    try {
      const res = await api.listHistory(PAGE, 0)
      setItems(res)
      setHasMore(res.length === PAGE)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }

  const loadMore = async () => {
    try {
      const res = await api.listHistory(PAGE, items.length)
      setItems((prev) => [...prev, ...res])
      setHasMore(res.length === PAGE)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const handleDelete = async (id: number, e: React.MouseEvent) => {
    e.stopPropagation()
    e.preventDefault()
    if (!window.confirm(t('history_delete_confirm'))) return
    try {
      await api.deleteHistory(id)
      setItems((prev) => prev.filter((i) => i.id !== id))
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const handleClearAll = async () => {
    if (!window.confirm(t('history_clear_confirm'))) return
    try {
      await api.clearHistory()
      setItems([])
      setHasMore(false)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  if (!user) return null

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold">{t('history_title')}</h1>
        {items.length > 0 && (
          <Button variant="destructive" size="sm" onClick={handleClearAll}>
            {t('history_clear_all')}
          </Button>
        )}
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

      {!loading && items.length === 0 && (
        <div className="rounded-lg border border-dashed bg-muted/20 px-4 py-10 text-center text-sm text-muted-foreground">
          {t('history_empty')}
        </div>
      )}

      <div className="space-y-3">
        {items.map((item) => (
          <Card key={item.id} className="transition hover:-translate-y-0.5 hover:shadow-lg">
            <Link to={`/history/${item.id}`} className="flex items-start justify-between gap-3 p-4">
              <div className="space-y-2">
                <div className="text-base font-semibold leading-snug line-clamp-2">{item.prompt}</div>
                <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
                  <span>{new Date(item.created_at + 'Z').toLocaleString()}</span>
                  <span>·</span>
                  <span>{t('history_result_count', { n: String(item.result_count) })}</span>
                </div>
              </div>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                onClick={(e) => handleDelete(item.id, e)}
                aria-label={t('history_delete')}
              >
                ×
              </Button>
            </Link>
          </Card>
        ))}
      </div>

      {hasMore && (
        <div className="flex justify-center">
          <Button variant="secondary" onClick={loadMore}>{t('common_load_more')}</Button>
        </div>
      )}
    </div>
  )
}
