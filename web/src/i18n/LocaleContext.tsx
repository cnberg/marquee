import { createContext, useContext, useEffect, useState, type ReactNode } from 'react'
import en from './en.json'
import zh from './zh.json'
import { api } from '../api/client'

type Locale = 'en' | 'zh'
type Translations = Record<string, string>

const translations: Record<Locale, Translations> = { en, zh }

interface LocaleContextValue {
  locale: Locale
  setLocale: (locale: Locale) => Promise<void>
  t: (key: string, params?: Record<string, string | number>) => string
}

const LocaleContext = createContext<LocaleContextValue | null>(null)

export function LocaleProvider({ children }: { children: ReactNode }) {
  const [locale, setLocaleState] = useState<Locale>('en')
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    api
      .adminGetSettings()
      .then((res) => {
        if (res.locale === 'en' || res.locale === 'zh') {
          setLocaleState(res.locale)
        }
      })
      .catch(() => {
        // default to 'en' on error
      })
      .finally(() => setLoaded(true))
  }, [])

  const setLocale = async (newLocale: Locale) => {
    await api.adminUpdateSettings({ locale: newLocale })
    setLocaleState(newLocale)
  }

  const t = (key: string, params?: Record<string, string | number>): string => {
    let text = translations[locale][key] ?? translations['en'][key] ?? key
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        text = text.replace(`{${k}}`, String(v))
      }
    }
    return text
  }

  if (!loaded) return null

  return <LocaleContext.Provider value={{ locale, setLocale, t }}>{children}</LocaleContext.Provider>
}

export function useLocale() {
  const ctx = useContext(LocaleContext)
  if (!ctx) throw new Error('useLocale must be used within LocaleProvider')
  return ctx
}
