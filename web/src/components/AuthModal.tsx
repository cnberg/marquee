import type { FormEvent } from 'react'
import { useState } from 'react'
import { useAuth } from '../auth/AuthContext'
import { useLocale } from '../i18n/LocaleContext'
import { Button } from './ui/button'
import { Input } from './ui/input'
import { Label } from './ui/label'
import { Alert, AlertDescription } from './ui/alert'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from './ui/dialog'
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs'

type Tab = 'login' | 'register'

export function AuthModal() {
  const { t } = useLocale()
  const { authModalOpen, closeAuthModal, login, register } = useAuth()
  const [tab, setTab] = useState<Tab>('login')
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)

  const isRegister = tab === 'register'

  const reset = () => {
    setUsername('')
    setPassword('')
    setConfirmPassword('')
    setError(null)
  }

  const handleClose = () => {
    closeAuthModal()
    reset()
    setTab('login')
  }

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    if (!username.trim() || !password.trim()) {
      setError(t('auth_error_empty'))
      return
    }
    if (isRegister && password !== confirmPassword) {
      setError(t('auth_error_password_mismatch'))
      return
    }
    setSubmitting(true)
    setError(null)
    try {
      if (isRegister) {
        await register(username.trim(), password)
      } else {
        await login(username.trim(), password)
      }
      handleClose()
    } catch (err: any) {
      const msg = err?.message || ''
      if (isRegister && msg.includes('409')) {
        setError(t('auth_error_conflict'))
      } else if (!isRegister && msg.includes('401')) {
        setError(t('auth_error_invalid'))
      } else {
        setError(t('auth_error_unknown'))
      }
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Dialog open={authModalOpen} onOpenChange={(open) => (!open ? handleClose() : null)}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>{t('auth_login_tab')}</DialogTitle>
        </DialogHeader>
        <Tabs value={tab} onValueChange={(v) => setTab(v as Tab)} className="space-y-4">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="login">{t('auth_login_tab')}</TabsTrigger>
            <TabsTrigger value="register">{t('auth_register_tab')}</TabsTrigger>
          </TabsList>

          <TabsContent value="login" className="space-y-4 pt-2">
            <form className="space-y-4" onSubmit={submit}>
              <div className="space-y-2">
                <Label htmlFor="login-username">{t('auth_username')}</Label>
                <Input
                  id="login-username"
                  value={username}
                  autoComplete="username"
                  onChange={(e) => setUsername(e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="login-password">{t('auth_password')}</Label>
                <Input
                  id="login-password"
                  type="password"
                  value={password}
                  autoComplete="current-password"
                  onChange={(e) => setPassword(e.target.value)}
                />
              </div>
              {error && (
                <Alert variant="destructive">
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}
              <Button type="submit" className="w-full" disabled={submitting}>
                {t('auth_login_btn')}
              </Button>
            </form>
          </TabsContent>

          <TabsContent value="register" className="space-y-4 pt-2">
            <form className="space-y-4" onSubmit={submit}>
              <div className="space-y-2">
                <Label htmlFor="register-username">{t('auth_username')}</Label>
                <Input
                  id="register-username"
                  value={username}
                  autoComplete="new-username"
                  onChange={(e) => setUsername(e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="register-password">{t('auth_password')}</Label>
                <Input
                  id="register-password"
                  type="password"
                  value={password}
                  autoComplete="new-password"
                  onChange={(e) => setPassword(e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="register-confirm">{t('auth_confirm_password')}</Label>
                <Input
                  id="register-confirm"
                  type="password"
                  value={confirmPassword}
                  autoComplete="new-password"
                  onChange={(e) => setConfirmPassword(e.target.value)}
                />
              </div>
              {error && (
                <Alert variant="destructive">
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}
              <Button type="submit" className="w-full" disabled={submitting}>
                {t('auth_register_btn')}
              </Button>
            </form>
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  )
}
