import { useEffect, useRef, useState } from 'react'
import { api } from '../api/client'
import type { AppConfig } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card'
import { Button } from '../components/ui/button'
import { Input } from '../components/ui/input'
import { Label } from '../components/ui/label'
import { nextRowId } from '../lib/utils'

type Flash = { type: 'success' | 'error'; text: string }

function MovieDirsEditor({
  value,
  onChange,
}: {
  value: string[]
  onChange: (next: string[]) => void
}) {
  const { t } = useLocale()
  // 维护 (id, value) 对，避免用 index 当 React key 导致输入框光标跳。
  const idsRef = useRef<string[]>([])
  if (idsRef.current.length !== value.length) {
    // 初始化或父组件外部 reset 时同步长度（保持已有 id，不足处补新 id）。
    while (idsRef.current.length < value.length) {
      idsRef.current.push(nextRowId())
    }
    if (idsRef.current.length > value.length) {
      idsRef.current.length = value.length
    }
  }

  const handleEdit = (idx: number, next: string) => {
    const arr = [...value]
    arr[idx] = next
    onChange(arr)
  }
  const handleRemove = (idx: number) => {
    const arr = value.filter((_, i) => i !== idx)
    idsRef.current.splice(idx, 1)
    onChange(arr)
  }
  const handleAdd = (prefix: '' | 'ssh://') => {
    idsRef.current.push(nextRowId())
    onChange([...value, prefix])
  }

  return (
    <div className="space-y-2">
      <Label className="text-sm font-medium">{t('config_movie_dirs')}</Label>
      <div className="space-y-2">
        {value.map((entry, idx) => {
          const isSsh = entry.startsWith('ssh://')
          return (
            <div key={idsRef.current[idx]} className="flex items-center gap-2">
              <span
                className={`shrink-0 rounded px-2 py-0.5 text-xs ${
                  isSsh
                    ? 'bg-secondary/20 text-secondary-foreground'
                    : 'bg-muted text-muted-foreground'
                }`}
              >
                {isSsh ? t('config_movie_dir_ssh_badge') : t('config_movie_dir_local_badge')}
              </span>
              <Input
                value={entry}
                onChange={(e) => handleEdit(idx, e.target.value)}
                className="font-mono text-sm"
              />
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => handleRemove(idx)}
                aria-label={t('config_movie_dir_remove')}
              >
                ✕
              </Button>
            </div>
          )
        })}
      </div>
      <div className="flex flex-wrap gap-2">
        <Button type="button" variant="secondary" size="sm" onClick={() => handleAdd('')}>
          + {t('config_movie_dir_add_local')}
        </Button>
        <Button type="button" variant="secondary" size="sm" onClick={() => handleAdd('ssh://')}>
          + {t('config_movie_dir_add_ssh')}
        </Button>
      </div>
      <p className="text-xs text-muted-foreground">{t('config_movie_dirs_hint')}</p>
    </div>
  )
}

function ConfigField({
  label,
  value,
  onChange,
  type = 'text',
  description,
}: {
  label: string
  value: string
  onChange: (v: string) => void
  type?: string
  description?: string
}) {
  return (
    <div className="space-y-1">
      <Label className="text-sm font-medium">{label}</Label>
      <Input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="font-mono text-sm"
      />
      {description && (
        <p className="text-xs text-muted-foreground">{description}</p>
      )}
    </div>
  )
}

export default function AdminConfig() {
  const { t } = useLocale()
  const [config, setConfig] = useState<AppConfig | null>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [flash, setFlash] = useState<Flash | null>(null)

  useEffect(() => {
    api.getConfig().then((c) => {
      setConfig(c)
      setLoading(false)
    }).catch((e) => {
      setFlash({ type: 'error', text: String(e) })
      setLoading(false)
    })
  }, [])

  const handleSave = async () => {
    if (!config) return
    setSaving(true)
    setFlash(null)
    try {
      const res = await api.updateConfig(config)
      const msg = t(res.message) !== res.message ? t(res.message) : res.message
      setFlash({ type: 'success', text: msg })
    } catch (e) {
      setFlash({ type: 'error', text: String(e) })
    } finally {
      setSaving(false)
    }
  }

  if (loading) return <p className="text-muted-foreground">{t('loading')}</p>
  if (!config) return <p className="text-destructive">{t('error_loading')}</p>

  const update = <S extends keyof AppConfig, K extends keyof AppConfig[S]>(
    section: S,
    key: K,
    value: AppConfig[S][K],
  ) => {
    setConfig((prev) => {
      if (!prev) return prev
      return { ...prev, [section]: { ...prev[section], [key]: value } }
    })
  }

  const updateStr = (section: keyof AppConfig, key: string, value: string) => {
    setConfig((prev) => {
      if (!prev) return prev
      return { ...prev, [section]: { ...(prev[section] as Record<string, unknown>), [key]: value } }
    })
  }

  const updateNum = (section: keyof AppConfig, key: string, value: string) => {
    const n = Number(value)
    if (!isNaN(n)) {
      setConfig((prev) => {
        if (!prev) return prev
        return { ...prev, [section]: { ...(prev[section] as Record<string, unknown>), [key]: n } }
      })
    }
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t('config_section_scan')}</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2">
          <div className="col-span-full flex items-center gap-2">
            <Label className="text-sm font-medium">enabled</Label>
            <input
              type="checkbox"
              checked={config.scan.enabled}
              onChange={(e) => update('scan', 'enabled', e.target.checked)}
            />
          </div>
          <div className="col-span-full">
            <MovieDirsEditor
              value={config.scan.movie_dirs}
              onChange={(next) =>
                setConfig((prev) => (prev ? { ...prev, scan: { ...prev.scan, movie_dirs: next } } : prev))
              }
            />
          </div>
          <div className="col-span-full">
            <ConfigField
              label={t('config_ssh_key_path')}
              value={config.scan.ssh_key_path ?? ''}
              onChange={(v) => {
                const trimmed = v.trim()
                setConfig((prev) =>
                  prev
                    ? {
                        ...prev,
                        scan: {
                          ...prev.scan,
                          ssh_key_path: trimmed === '' ? undefined : trimmed,
                        },
                      }
                    : prev,
                )
              }}
              description={t('config_ssh_key_path_hint')}
            />
          </div>
          <ConfigField
            label={t('config_scan_interval_hours')}
            value={String(config.scan.interval_hours)}
            onChange={(v) => updateNum('scan', 'interval_hours', v)}
            type="number"
            description={t('config_scan_interval_hours_desc')}
          />
          <ConfigField
            label={t('config_scan_worker_poll_secs')}
            value={String(config.scan.worker_poll_secs)}
            onChange={(v) => updateNum('scan', 'worker_poll_secs', v)}
            type="number"
            description={t('config_scan_worker_poll_secs_desc')}
          />
          <ConfigField
            label={t('config_scan_refresh_interval_hours')}
            value={String(config.scan.refresh_interval_hours)}
            onChange={(v) => updateNum('scan', 'refresh_interval_hours', v)}
            type="number"
            description={t('config_scan_refresh_interval_hours_desc')}
          />
          <ConfigField
            label={t('config_scan_refresh_batch_size')}
            value={String(config.scan.refresh_batch_size)}
            onChange={(v) => updateNum('scan', 'refresh_batch_size', v)}
            type="number"
            description={t('config_scan_refresh_batch_size_desc')}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t('config_section_tmdb')}</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2">
          <ConfigField
            label={t('config_tmdb_api_key')}
            value={config.tmdb.api_key}
            onChange={(v) => updateStr('tmdb', 'api_key', v)}
            type="password"
          />
          <ConfigField
            label={t('config_tmdb_language')}
            value={config.tmdb.language}
            onChange={(v) => updateStr('tmdb', 'language', v)}
            description={t('config_tmdb_language_desc')}
          />
          <ConfigField
            label={t('config_tmdb_auto_confirm_threshold')}
            value={String(config.tmdb.auto_confirm_threshold)}
            onChange={(v) => updateNum('tmdb', 'auto_confirm_threshold', v)}
            type="number"
            description={t('config_tmdb_auto_confirm_threshold_desc')}
          />
          <ConfigField
            label={t('config_tmdb_proxy')}
            value={config.tmdb.proxy ?? ''}
            onChange={(v) => update('tmdb', 'proxy', v || null)}
            description={t('config_tmdb_proxy_desc')}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t('config_section_llm')}</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2">
          <ConfigField
            label={t('config_llm_backend')}
            value={config.llm.backend}
            onChange={(v) => updateStr('llm', 'backend', v)}
            description={t('config_llm_backend_desc')}
          />
          <ConfigField
            label={t('config_llm_base_url')}
            value={config.llm.base_url}
            onChange={(v) => updateStr('llm', 'base_url', v)}
          />
          <ConfigField
            label={t('config_llm_api_key')}
            value={config.llm.api_key}
            onChange={(v) => updateStr('llm', 'api_key', v)}
            type="password"
          />
          <ConfigField
            label={t('config_llm_model')}
            value={config.llm.model}
            onChange={(v) => updateStr('llm', 'model', v)}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t('config_section_server')}</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2">
          <ConfigField
            label={t('config_server_host')}
            value={config.server.host}
            onChange={(v) => updateStr('server', 'host', v)}
          />
          <ConfigField
            label={t('config_server_port')}
            value={String(config.server.port)}
            onChange={(v) => updateNum('server', 'port', v)}
            type="number"
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t('config_section_database')}</CardTitle>
        </CardHeader>
        <CardContent>
          <ConfigField
            label={t('config_database_path')}
            value={config.database.path}
            onChange={(v) => updateStr('database', 'path', v)}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t('config_section_auth')}</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2">
          <ConfigField
            label={t('config_auth_jwt_secret')}
            value={config.auth.jwt_secret}
            onChange={(v) => updateStr('auth', 'jwt_secret', v)}
            type="password"
          />
          <ConfigField
            label={t('config_auth_jwt_expiry_days')}
            value={String(config.auth.jwt_expiry_days)}
            onChange={(v) => updateNum('auth', 'jwt_expiry_days', v)}
            type="number"
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t('config_section_qbittorrent')}</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2">
          <div className="col-span-full flex items-center gap-2">
            <Label className="text-sm font-medium">{t('config_qbt_enabled')}</Label>
            <input
              type="checkbox"
              checked={config.qbittorrent.enabled}
              onChange={(e) => update('qbittorrent', 'enabled', e.target.checked)}
            />
          </div>
          <ConfigField
            label={t('config_qbt_base_url')}
            value={config.qbittorrent.base_url}
            onChange={(v) => updateStr('qbittorrent', 'base_url', v)}
          />
          <ConfigField
            label={t('config_qbt_username')}
            value={config.qbittorrent.username}
            onChange={(v) => updateStr('qbittorrent', 'username', v)}
          />
          <ConfigField
            label={t('config_qbt_password')}
            value={config.qbittorrent.password}
            onChange={(v) => updateStr('qbittorrent', 'password', v)}
            type="password"
          />
          <ConfigField
            label={t('config_qbt_save_path')}
            value={config.qbittorrent.save_path}
            onChange={(v) => updateStr('qbittorrent', 'save_path', v)}
          />
          <ConfigField
            label={t('config_qbt_poll_interval_hours')}
            value={String(config.qbittorrent.poll_interval_hours)}
            onChange={(v) => updateNum('qbittorrent', 'poll_interval_hours', v)}
            type="number"
          />
        </CardContent>
      </Card>

      <div className="flex items-center justify-end gap-3">
        {flash && (
          <span className={flash.type === 'error' ? 'text-sm text-destructive' : 'text-sm text-green-600'}>
            {flash.text}
          </span>
        )}
        <Button onClick={handleSave} disabled={saving}>
          {saving ? t('saving') : t('config_save')}
        </Button>
      </div>
    </div>
  )
}
