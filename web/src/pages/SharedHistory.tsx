import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import { api } from '../api/client'
import HistoryDetailView from '../components/HistoryDetailView'
import { useLocale } from '../i18n/LocaleContext'
import { Alert, AlertDescription } from '../components/ui/alert'
import type { SearchHistoryDetail as HistoryDetailType } from '../types'

export default function SharedHistory() {
  const { token } = useParams<{ token: string }>()
  const { t } = useLocale()
  const [detail, setDetail] = useState<HistoryDetailType | null>(null)
  const [loading, setLoading] = useState(true)
  const [notFound, setNotFound] = useState(false)

  useEffect(() => { document.title = t('shared_page_title') }, [t])

  useEffect(() => {
    if (!token) return
    setLoading(true)
    setNotFound(false)
    api.getSharedHistory(token)
      .then(setDetail)
      .catch(() => setNotFound(true))
      .finally(() => setLoading(false))
  }, [token])

  if (loading) {
    return (
      <div className="rounded-lg border bg-card px-4 py-6 text-sm text-muted-foreground">
        {t('common_loading')}
      </div>
    )
  }

  if (notFound || !detail) {
    return (
      <Alert variant="destructive">
        <AlertDescription>{t('shared_not_found')}</AlertDescription>
      </Alert>
    )
  }

  return (
    <div className="space-y-4">
      <HistoryDetailView detail={detail} hideTimestamp />
    </div>
  )
}
