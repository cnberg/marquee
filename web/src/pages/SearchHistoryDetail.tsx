import { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { RotateCcw, Share2 } from 'lucide-react'
import { api } from '../api/client'
import HistoryDetailView, { useParsedHistoryEvents } from '../components/HistoryDetailView'
import { useAuth } from '../auth/AuthContext'
import { useMovieMarks } from '../hooks/useMovieMarks'
import { useLocale } from '../i18n/LocaleContext'
import { copyToClipboard } from '../lib/utils'
import { Button } from '../components/ui/button'
import { Card } from '../components/ui/card'
import { Alert, AlertDescription } from '../components/ui/alert'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '../components/ui/dialog'
import type { SearchHistoryDetail as HistoryDetailType } from '../types'

export default function SearchHistoryDetail() {
  const { id } = useParams<{ id: string }>()
  const { t } = useLocale()
  const { user, showAuthModal } = useAuth()
  const navigate = useNavigate()
  const [detail, setDetail] = useState<HistoryDetailType | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [shareDialogOpen, setShareDialogOpen] = useState(false)
  const [sharing, setSharing] = useState(false)
  const [copied, setCopied] = useState(false)

  useEffect(() => { document.title = t('history_detail_title') }, [t])

  useEffect(() => {
    if (!user) { showAuthModal(); navigate('/'); return }
    if (!id) return
    setLoading(true)
    api.getHistory(Number(id))
      .then(setDetail)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [id, user?.id])

  const { finalResult } = useParsedHistoryEvents(detail)
  const visibleMovieIds = useMemo(
    () => (finalResult ? finalResult.recommendations.map((r) => r.movie.id) : []),
    [finalResult],
  )
  const { marks, toggle } = useMovieMarks(visibleMovieIds)

  const shareUrl = detail?.share_token
    ? `${window.location.origin}/shared/${detail.share_token}`
    : ''

  const openShareDialog = async () => {
    if (!detail || !id) return
    setShareDialogOpen(true)
    setCopied(false)

    let token = detail.share_token
    if (!token) {
      setSharing(true)
      try {
        const res = await api.shareHistory(Number(id))
        token = res.token
        setDetail({ ...detail, share_token: token })
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e))
        setShareDialogOpen(false)
        setSharing(false)
        return
      }
      setSharing(false)
    }

    // Auto-copy on dialog open. The legacy fallback in copyToClipboard uses
    // Selection API rather than focus, so it survives both async-await user-
    // gesture expiry and Radix's dialog focus trap.
    const url = `${window.location.origin}/shared/${token}`
    const ok = await copyToClipboard(url)
    setCopied(ok)
  }

  const handleCopy = async () => {
    const ok = await copyToClipboard(shareUrl)
    setCopied(ok)
  }

  const handleRevoke = async () => {
    if (!detail || !id) return
    if (!window.confirm(t('history_share_revoke_confirm'))) return
    try {
      await api.unshareHistory(Number(id))
      setDetail({ ...detail, share_token: null })
      setShareDialogOpen(false)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  if (!user) return null

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <Button asChild variant="ghost" size="sm">
          <Link to="/history">← {t('history_back')}</Link>
        </Button>
        {detail && (
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => navigate(`/?q=${encodeURIComponent(detail.prompt)}`)}
            >
              <RotateCcw className="mr-1.5 h-4 w-4" />
              {t('history_rerun')}
            </Button>
            <Button variant="outline" size="sm" onClick={openShareDialog}>
              <Share2 className="mr-1.5 h-4 w-4" />
              {t('history_share_button')}
            </Button>
          </div>
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

      {detail && (
        <HistoryDetailView
          detail={detail}
          marks={marks}
          onToggleMark={(movieId, mt) => { toggle(movieId, mt) }}
        />
      )}

      <Dialog open={shareDialogOpen} onOpenChange={setShareDialogOpen}>
        <DialogContent className="max-w-xl">
          <DialogHeader>
            <DialogTitle>{t('history_share_dialog_title')}</DialogTitle>
          </DialogHeader>
          {sharing ? (
            <div className="text-sm text-muted-foreground">{t('common_loading')}</div>
          ) : shareUrl ? (
            <div className="space-y-3 text-sm">
              {copied && (
                <p className="text-emerald-600 dark:text-emerald-400">{t('history_share_copied')}</p>
              )}
              <Card className="select-all break-all p-3 font-mono text-xs leading-relaxed">
                {shareUrl}
              </Card>
              <p className="text-muted-foreground">{t('history_share_dialog_desc')}</p>
            </div>
          ) : null}
          <DialogFooter className="gap-2 sm:justify-between">
            {shareUrl && (
              <Button variant="ghost" onClick={handleRevoke} className="text-destructive hover:text-destructive">
                {t('history_share_revoke')}
              </Button>
            )}
            <div className="flex gap-2">
              <Button variant="outline" onClick={handleCopy} disabled={!shareUrl}>
                {t('history_share_copy')}
              </Button>
              <Button onClick={() => setShareDialogOpen(false)}>{t('common_close')}</Button>
            </div>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
