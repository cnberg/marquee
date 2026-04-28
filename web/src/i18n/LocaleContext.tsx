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

const LOCALE_STORAGE_KEY = 'marquee.locale'

function readStoredLocale(): Locale | null {
  try {
    const v = localStorage.getItem(LOCALE_STORAGE_KEY)
    return v === 'en' || v === 'zh' ? v : null
  } catch {
    return null
  }
}

export function LocaleProvider({ children }: { children: ReactNode }) {
  // Initial state: localStorage first (instant, no server roundtrip).
  // Server settings layered on top for logged-in users in useEffect below.
  const [locale, setLocaleState] = useState<Locale>(() => readStoredLocale() ?? 'en')

  useEffect(() => {
    // Best-effort sync from server settings. Failures (401 for anon, network)
    // don't unwind state — localStorage is the source of truth for client UI.
    api
      .adminGetSettings()
      .then((res) => {
        if (res.locale === 'en' || res.locale === 'zh') {
          setLocaleState(res.locale)
          try { localStorage.setItem(LOCALE_STORAGE_KEY, res.locale) } catch { /* ignore */ }
        }
      })
      .catch(() => {
        // Anon user (401) or offline. Local state already initialized from
        // localStorage; nothing to do.
      })
  }, [])

  const setLocale = async (newLocale: Locale) => {
    // Update UI synchronously — never block on the server. PUT /api/admin/settings
    // requires login; for anon users it returns 401 and the await would throw,
    // which previously left state unchanged and made the toggle look "无法点击".
    setLocaleState(newLocale)
    try { localStorage.setItem(LOCALE_STORAGE_KEY, newLocale) } catch { /* ignore */ }
    // Fire-and-forget server persist for cross-device sync; swallow auth errors.
    api.adminUpdateSettings({ locale: newLocale }).catch(() => { /* anon 401, ignore */ })
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

  return <LocaleContext.Provider value={{ locale, setLocale, t }}>{children}</LocaleContext.Provider>
}

export function useLocale() {
  const ctx = useContext(LocaleContext)
  if (!ctx) throw new Error('useLocale must be used within LocaleProvider')
  return ctx
}
