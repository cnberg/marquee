import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// `crypto.randomUUID` is only exposed in secure contexts (HTTPS or localhost).
// On plain HTTP LAN deploys (e.g. `http://192.168.1.100:8080`) it's undefined and
// blanks the page. Callers only need a unique React key, not crypto strength,
// so a module-level monotonic counter is enough.
let __rowIdCounter = 0
export function nextRowId(): string {
  __rowIdCounter += 1
  return `row-${__rowIdCounter}`
}

// Copy text to clipboard with broad browser support.
// Handles non-secure contexts (HTTP) where navigator.clipboard is unavailable,
// and focus-trapping dialogs (Radix) where focusing a hidden textarea would be
// stolen back before execCommand can read the selection.
export async function copyToClipboard(text: string): Promise<boolean> {
  try {
    if (window.isSecureContext && navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text)
      return true
    }
  } catch {
    // fall through to execCommand fallback
  }

  // Legacy fallback: span + Range/Selection + execCommand('copy').
  // Why a span instead of a hidden textarea? textarea.select() pulls focus,
  // and Radix Dialog's focus trap immediately yanks focus back inside the
  // dialog, which clears the document selection before execCommand runs —
  // so the textarea fallback silently copies nothing on Firefox + HTTP.
  // A span never gets focused; the Selection on it survives focus moves.
  const span = document.createElement('span')
  span.textContent = text
  span.style.position = 'fixed'
  span.style.left = '-9999px'
  span.style.top = '0'
  span.style.whiteSpace = 'pre'
  document.body.appendChild(span)

  const selection = window.getSelection()
  if (!selection) {
    document.body.removeChild(span)
    return false
  }
  const previousRange = selection.rangeCount > 0 ? selection.getRangeAt(0) : null
  selection.removeAllRanges()

  const range = document.createRange()
  range.selectNodeContents(span)
  selection.addRange(range)

  let ok = false
  try {
    ok = document.execCommand('copy')
  } catch {
    ok = false
  }

  selection.removeAllRanges()
  if (previousRange) selection.addRange(previousRange)
  document.body.removeChild(span)
  return ok
}

// Pick the right localized value for the current UI locale. Always falls back
// through the other language, then a default, so missing en data still shows
// zh instead of going blank (typical for pre-bilingual-migration rows).
export function pickLocalized<T>(
  locale: 'en' | 'zh',
  enValue: T | null | undefined,
  zhValue: T | null | undefined,
  fallback?: T | null,
): T | null | undefined {
  if (locale === 'en') return enValue ?? zhValue ?? fallback
  return zhValue ?? enValue ?? fallback
}
