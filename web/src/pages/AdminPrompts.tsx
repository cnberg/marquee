import { useEffect, useState } from 'react'
import { api } from '../api/client'
import type { PromptInfo } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { Card } from '../components/ui/card'
import { Alert, AlertDescription } from '../components/ui/alert'
import { Button } from '../components/ui/button'
import { Textarea } from '../components/ui/textarea'
import { Badge } from '../components/ui/badge'

type EditState = {
  draft: string
  saving: boolean
  resetting: boolean
  flash: { type: 'success' | 'error'; text: string } | null
}

export default function AdminPrompts() {
  const { t } = useLocale()
  const [prompts, setPrompts] = useState<PromptInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [editStates, setEditStates] = useState<Record<string, EditState>>({})

  const loadPrompts = async () => {
    setLoading(true)
    try {
      const res = await api.adminPrompts()
      setPrompts(res)
      const states: Record<string, EditState> = {}
      for (const p of res) {
        states[p.name] = {
          draft: p.content,
          saving: false,
          resetting: false,
          flash: null,
        }
      }
      setEditStates(states)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : t('prompts_load_error'))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadPrompts()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [t])

  if (loading) return <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>

  const promptLabels: Record<string, string> = {
    'recommend-filter': t('prompt_label_recommend-filter'),
    'recommend-pick': t('prompt_label_recommend-pick'),
    inspire: t('prompt_label_inspire'),
    'query-understand': t('prompt_label_query-understand'),
    'smart-rank': t('prompt_label_smart-rank'),
  }

  const fileNameOf = (p: PromptInfo): string => {
    const hasEn = ['recommend-filter', 'recommend-pick', 'inspire'].includes(p.name)
    if (hasEn && p.locale === 'en') return `${p.name}.en.md`
    return `${p.name}.md`
  }

  const updateState = (name: string, patch: Partial<EditState>) => {
    setEditStates((prev) => ({
      ...prev,
      [name]: { ...prev[name], ...patch },
    }))
  }

  const handleSave = async (p: PromptInfo) => {
    const state = editStates[p.name]
    if (!state) return
    updateState(p.name, { saving: true, flash: null })
    try {
      await api.adminUpdatePrompt(p.name, state.draft)
      updateState(p.name, {
        saving: false,
        flash: { type: 'success', text: t('prompts_save_success') },
      })
      // Reflect new overridden state in the list without full reload.
      setPrompts((prev) =>
        prev.map((item) =>
          item.name === p.name ? { ...item, content: state.draft, overridden: true } : item,
        ),
      )
    } catch (err) {
      updateState(p.name, {
        saving: false,
        flash: {
          type: 'error',
          text: err instanceof Error ? err.message : t('prompts_save_error'),
        },
      })
    }
  }

  const handleReset = async (p: PromptInfo) => {
    if (!window.confirm(t('prompts_reset_confirm'))) return
    updateState(p.name, { resetting: true, flash: null })
    try {
      await api.adminResetPrompt(p.name)
      updateState(p.name, {
        resetting: false,
        draft: p.default_content,
        flash: { type: 'success', text: t('prompts_reset_success') },
      })
      setPrompts((prev) =>
        prev.map((item) =>
          item.name === p.name
            ? { ...item, content: item.default_content, overridden: false }
            : item,
        ),
      )
    } catch (err) {
      updateState(p.name, {
        resetting: false,
        flash: {
          type: 'error',
          text: err instanceof Error ? err.message : t('prompts_reset_error'),
        },
      })
    }
  }

  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <h2 className="text-2xl font-semibold">{t('prompts_title')}</h2>
        <p className="text-sm text-muted-foreground">{t('prompts_edit_hint')}</p>
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
      </div>

      <div className="grid gap-3 md:grid-cols-2">
        {prompts.map((p) => {
          const state = editStates[p.name]
          const dirty = state && state.draft !== p.content
          return (
            <Card key={p.name} className="space-y-3 p-4">
              <div className="flex items-center justify-between gap-2">
                <div>
                  <h3 className="text-lg font-semibold">
                    {promptLabels[p.name] ?? p.name}
                  </h3>
                  <span className="text-xs text-muted-foreground">{fileNameOf(p)}</span>
                </div>
                {p.overridden && (
                  <Badge variant="secondary">{t('prompts_overridden_badge')}</Badge>
                )}
              </div>
              <Textarea
                value={state?.draft ?? ''}
                onChange={(e) => updateState(p.name, { draft: e.target.value, flash: null })}
                className="min-h-64 font-mono text-xs leading-relaxed"
                spellCheck={false}
              />
              {state?.flash && (
                <Alert variant={state.flash.type === 'error' ? 'destructive' : 'default'}>
                  <AlertDescription>{state.flash.text}</AlertDescription>
                </Alert>
              )}
              <div className="flex items-center justify-end gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handleReset(p)}
                  disabled={!p.overridden || state?.resetting || state?.saving}
                >
                  {state?.resetting ? t('prompts_resetting') : t('prompts_reset')}
                </Button>
                <Button
                  size="sm"
                  onClick={() => handleSave(p)}
                  disabled={!dirty || state?.saving || state?.resetting}
                >
                  {state?.saving ? t('prompts_saving') : t('prompts_save')}
                </Button>
              </div>
            </Card>
          )
        })}
      </div>
    </div>
  )
}
