import { useState } from 'react'
import { useLocale } from '../i18n/LocaleContext'
import { Button } from './ui/button'
import { Input } from './ui/input'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from './ui/dialog'
import {
  buildSearchUrl,
  copyToClipboard,
  getExtensionId,
  setExtensionId,
} from '../lib/ptDepiler'

interface Props {
  imdbId: string
}

export function PtDepilerSearchButton({ imdbId }: Props) {
  const { t } = useLocale()
  const [setupOpen, setSetupOpen] = useState(false)
  const [linkOpen, setLinkOpen] = useState(false)
  const [draftId, setDraftId] = useState('')
  const [currentUrl, setCurrentUrl] = useState('')
  const [copied, setCopied] = useState(false)

  const openLinkDialog = async (extId: string) => {
    const url = buildSearchUrl(extId, imdbId)
    setCurrentUrl(url)
    const ok = await copyToClipboard(url)
    setCopied(ok)
    setLinkOpen(true)
  }

  const handleClick = async () => {
    const extId = getExtensionId()
    if (!extId) {
      setDraftId('')
      setSetupOpen(true)
      return
    }
    await openLinkDialog(extId)
  }

  const handleSave = async () => {
    const trimmed = draftId.trim()
    if (!trimmed) return
    setExtensionId(trimmed)
    setSetupOpen(false)
    await openLinkDialog(trimmed)
  }

  const handleRecopy = async () => {
    const ok = await copyToClipboard(currentUrl)
    setCopied(ok)
  }

  return (
    <>
      <Button variant="outline" size="sm" onClick={handleClick}>
        {t('pt_depiler_search')}
      </Button>

      <Dialog open={setupOpen} onOpenChange={setSetupOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>{t('pt_depiler_setup_title')}</DialogTitle>
          </DialogHeader>
          <div className="space-y-3">
            <p className="text-sm text-muted-foreground">{t('pt_depiler_setup_intro')}</p>
            <div className="space-y-1">
              <label className="text-sm font-medium">{t('pt_depiler_id_label')}</label>
              <Input
                value={draftId}
                onChange={(e) => setDraftId(e.target.value)}
                placeholder={t('pt_depiler_id_placeholder')}
                autoFocus
                onKeyDown={(e) => {
                  if (e.key === 'Enter') handleSave()
                }}
              />
            </div>
            <p className="text-xs text-muted-foreground whitespace-pre-line">
              {t('pt_depiler_id_help')}
            </p>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setSetupOpen(false)}>
              {t('pt_depiler_cancel')}
            </Button>
            <Button onClick={handleSave} disabled={!draftId.trim()}>
              {t('pt_depiler_save_and_copy')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={linkOpen} onOpenChange={setLinkOpen}>
        <DialogContent className="max-w-xl">
          <DialogHeader>
            <DialogTitle>{t('pt_depiler_link_title')}</DialogTitle>
          </DialogHeader>
          <div className="space-y-3 text-sm">
            <p className={copied ? 'text-emerald-600 dark:text-emerald-400' : 'text-amber-600 dark:text-amber-400'}>
              {copied ? t('pt_depiler_link_copied') : t('pt_depiler_link_copy_failed')}
            </p>
            <code className="block select-all break-all rounded bg-muted p-3 font-mono text-xs leading-relaxed">
              {currentUrl}
            </code>
            <p className="whitespace-pre-line text-muted-foreground">
              {t('pt_depiler_link_instructions')}
            </p>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={handleRecopy}>
              {t('pt_depiler_link_copy_again')}
            </Button>
            <Button onClick={() => setLinkOpen(false)}>{t('pt_depiler_got_it')}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
