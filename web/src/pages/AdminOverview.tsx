import { useEffect, useState } from 'react'
import { api } from '../api/client'
import type { AdminOverviewData } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { Card } from '../components/ui/card'
import { Button } from '../components/ui/button'
import { Badge } from '../components/ui/badge'
import { Alert, AlertDescription } from '../components/ui/alert'

export default function AdminOverview() {
  const { t, locale, setLocale } = useLocale()
  const [data, setData] = useState<AdminOverviewData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [scanning, setScanning] = useState(false)
  const [regenerating, setRegenerating] = useState(false)
  const [regenMessage, setRegenMessage] = useState<string | null>(null)

  const load = async () => {
    setLoading(true)
    try {
      const res = await api.adminOverview()
      setData(res)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : t('overview_load_error'))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
  }, [])

  const handleScan = async () => {
    setScanning(true)
    try {
      await api.triggerScan()
      setTimeout(load, 2000)
    } catch (err) {
      setError(err instanceof Error ? err.message : t('overview_load_error'))
    } finally {
      setScanning(false)
    }
  }

  const handleRegenerateDailyPicks = async () => {
    setRegenerating(true)
    setRegenMessage(null)
    try {
      await api.adminRegenerateDailyPicks()
      setRegenMessage(t('overview_regen_success'))
    } catch (err) {
      setRegenMessage(err instanceof Error ? err.message : t('overview_regen_error'))
    } finally {
      setRegenerating(false)
    }
  }

  if (loading) return <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>
  if (error) return (
    <Alert variant="destructive">
      <AlertDescription>{error}</AlertDescription>
    </Alert>
  )
  if (!data) return null

  const statusCount = (s: string) => data.dir_status.find(([k]) => k === s)?.[1] ?? 0
  const matched = statusCount('matched')
  const parsed = statusCount('parsed')
  const failed = statusCount('failed')
  const newCount = statusCount('new')
  const total = data.dir_total
  const processed = matched + parsed + failed
  const progressPct = total > 0 ? Math.round((processed / total) * 100) : 0

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <h2 className="text-2xl font-semibold">{t('overview_title')}</h2>
        <div className="flex flex-wrap gap-2">
          <Button type="button" onClick={handleScan} disabled={scanning}>
            {scanning ? t('overview_scanning') : t('overview_scan_btn')}
          </Button>
          <Button type="button" variant="secondary" onClick={handleRegenerateDailyPicks} disabled={regenerating}>
            {regenerating ? t('overview_regenerating') : t('overview_regen_btn')}
          </Button>
        </div>
      </div>

      {regenMessage && (
        <Alert>
          <AlertDescription>{regenMessage}</AlertDescription>
        </Alert>
      )}

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="space-y-3 p-4">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold">{t('settings_language')}</h3>
            <div className="flex gap-2">
              <Button variant={locale === 'en' ? 'default' : 'outline'} size="sm" onClick={() => setLocale('en')}>
                {t('settings_language_en')}
              </Button>
              <Button variant={locale === 'zh' ? 'default' : 'outline'} size="sm" onClick={() => setLocale('zh')}>
                {t('settings_language_zh')}
              </Button>
            </div>
          </div>
        </Card>

        <Card className="space-y-3 p-4">
          <h3 className="text-lg font-semibold">{t('overview_progress')}</h3>
          <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
            <div className="h-full rounded-full bg-primary" style={{ width: `${progressPct}%` }} />
          </div>
          <div className="text-sm text-muted-foreground">
            {processed} / {total} {t('overview_processed')} ({progressPct}%)
            {newCount > 0 && (
              <span className="ml-2">· {newCount} {t('overview_pending_program')}</span>
            )}
          </div>
          <div className="text-sm text-muted-foreground">
            {t('overview_dir_matched')} {matched} · {t('overview_dir_parsed')} {parsed} · {t('overview_dir_failed')} {failed}
          </div>
        </Card>
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        <Card className="space-y-3 p-4">
          <h3 className="text-lg font-semibold">{t('overview_dir_status')}</h3>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
            {data.dir_status.map(([status, count]) => (
              <div key={status} className="rounded-lg border bg-muted/30 p-3">
                <div className="text-2xl font-semibold">{count}</div>
                <div className="text-sm text-muted-foreground">{t(`overview_dir_${status}`)}</div>
              </div>
            ))}
          </div>
        </Card>

        <Card className="space-y-3 p-4">
          <h3 className="text-lg font-semibold">{t('overview_match_status')}</h3>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
            {data.match_status.map(([status, count]) => (
              <div key={status} className="rounded-lg border bg-muted/30 p-3">
                <div className="text-2xl font-semibold">{count}</div>
                <div className="text-sm text-muted-foreground">{status}</div>
              </div>
            ))}
          </div>
        </Card>
      </div>

      <Card className="space-y-3 p-4">
        <h3 className="text-lg font-semibold">{t('overview_tasks')}</h3>
        {Object.keys(data.tasks).length === 0 ? (
          <div className="text-sm text-muted-foreground">{t('overview_no_tasks')}</div>
        ) : (
          <div className="overflow-hidden rounded-lg border">
            <table className="w-full text-sm">
              <thead className="bg-muted/50">
                <tr>
                  <th className="px-3 py-2 text-left font-medium">{t('overview_task_type')}</th>
                  <th className="px-3 py-2 text-left font-medium">pending</th>
                  <th className="px-3 py-2 text-left font-medium">running</th>
                  <th className="px-3 py-2 text-left font-medium">done</th>
                  <th className="px-3 py-2 text-left font-medium">failed</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(data.tasks).map(([type, statuses]) => (
                  <tr key={type} className="border-t border-border">
                    <td className="px-3 py-2">{type}</td>
                    <td className="px-3 py-2">{statuses.pending ?? 0}</td>
                    <td className="px-3 py-2">{statuses.running ?? 0}</td>
                    <td className="px-3 py-2">{statuses.done ?? 0}</td>
                    <td className="px-3 py-2">
                      <Badge variant={statuses.failed ? 'destructive' : 'secondary'}>
                        {statuses.failed ?? 0}
                      </Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  )
}
