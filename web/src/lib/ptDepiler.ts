const STORAGE_KEY = 'marquee.pt_depiler.extension_id'

export function getExtensionId(): string {
  try {
    return localStorage.getItem(STORAGE_KEY)?.trim() ?? ''
  } catch {
    return ''
  }
}

export function setExtensionId(id: string): void {
  const trimmed = id.trim()
  try {
    if (trimmed) localStorage.setItem(STORAGE_KEY, trimmed)
    else localStorage.removeItem(STORAGE_KEY)
  } catch {
    // localStorage unavailable (private mode etc.) — silently ignore
  }
}

function extensionProtocol(): string {
  if (typeof navigator !== 'undefined' && /Firefox\//.test(navigator.userAgent)) {
    return 'moz-extension'
  }
  return 'chrome-extension'
}

export function buildSearchUrl(extensionId: string, imdbId: string): string {
  // PT-depiler's search param supports typed keywords: `imdb|tt1234567` binds the
  // query to IMDB id space so PT sites can lookup via their IMDB index instead of
  // treating "tt1234567" as a title. Pipe is kept literal for human readability;
  // IMDB ids are tt+digits so no URL-encoding needed.
  const keyword = `imdb|${imdbId}`
  return `${extensionProtocol()}://${extensionId}/src/entries/options/index.html#/search-entity?search=${keyword}&plan=default&flush=1`
}

export { copyToClipboard } from './utils'
