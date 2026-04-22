import type {
  FilterOptions,
  Movie,
  Person,
  SearchHistoryDetail,
  SearchHistoryItem,
} from '../types'

const BASE = '/api'

function getAuthHeaders(): Record<string, string> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  const token = localStorage.getItem('auth_token')
  if (token) {
    headers['Authorization'] = `Bearer ${token}`
  }
  return headers
}

async function readErrorMessage(resp: Response): Promise<string> {
  // Try to surface the server-provided error body (Axum returns plain text for
  // (StatusCode, String) tuples, JSON for structured errors). Fall back to the
  // status code if the body is empty or unreadable.
  try {
    const text = (await resp.text()).trim()
    if (!text) return `HTTP ${resp.status}`
    // If it's a JSON object with a "message" field, prefer that.
    if (text.startsWith('{')) {
      try {
        const obj = JSON.parse(text)
        if (typeof obj.message === 'string' && obj.message) return obj.message
      } catch { /* fall through to raw text */ }
    }
    return text
  } catch {
    return `HTTP ${resp.status}`
  }
}

export async function fetchJSON<T>(path: string, options?: RequestInit): Promise<T> {
  const resp = await fetch(`${BASE}${path}`, {
    headers: getAuthHeaders(),
    ...options,
  })
  if (!resp.ok) throw new Error(await readErrorMessage(resp))
  return resp.json()
}

async function fetchText(path: string, options?: RequestInit): Promise<string> {
  const resp = await fetch(`${BASE}${path}`, {
    headers: getAuthHeaders(),
    ...options,
  })
  if (!resp.ok) throw new Error(await readErrorMessage(resp))
  return resp.text()
}

export const api = {
  listMovies: (params?: Record<string, string>) => {
    const qs = params ? '?' + new URLSearchParams(params).toString() : ''
    return fetchJSON<any>(`/movies${qs}`)
  },
  getMovie: (id: number) => fetchJSON<any>(`/movies/${id}?include=credits,images,videos,reviews,similar,recommendations,watch_providers,release_dates,external_ids,alternative_titles,translations,lists`),
  listPending: (params?: Record<string, string>) => {
    const qs = params ? '?' + new URLSearchParams(params).toString() : ''
    return fetchJSON<any>(`/dirs/pending${qs}`)
  },
  getCandidates: (dirId: number) => fetchJSON<any>(`/dirs/${dirId}/candidates`),
  bind: (dirId: number, tmdbId: number) =>
    fetchJSON<any>(`/dirs/${dirId}/bind`, {
      method: 'POST',
      body: JSON.stringify({ tmdb_id: tmdbId }),
    }),
  unbind: (dirId: number) =>
    fetchJSON<any>(`/dirs/${dirId}/unbind`, { method: 'POST' }),
  tmdbSearch: (q: string) => fetchJSON<any>(`/tmdb/search?q=${encodeURIComponent(q)}`),
  triggerScan: () => fetchJSON<any>('/admin/scan', { method: 'POST' }),
  getStatus: () => fetchJSON<any>('/admin/status'),
  getFilters: () => fetchJSON<FilterOptions>('/movies/filters'),
  inspire: () => fetchJSON<{ ideas: Array<{ display: string; query: string }> }>('/inspire', { method: 'POST' }),
  dailyPicks: () => fetchJSON<{ sections: Array<{ inspiration: string; movies: Array<{ movie: any; reason: string }> }> }>('/daily-picks'),
  // Search history
  listHistory: (limit = 20, offset = 0) =>
    fetchJSON<SearchHistoryItem[]>(`/history?limit=${limit}&offset=${offset}`),
  getHistory: (id: number) => fetchJSON<SearchHistoryDetail>(`/history/${id}`),
  deleteHistory: (id: number) =>
    fetch(`${BASE}/history/${id}`, {
      method: 'DELETE',
      headers: getAuthHeaders(),
    }).then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
    }),
  clearHistory: () =>
    fetch(`${BASE}/history`, {
      method: 'DELETE',
      headers: getAuthHeaders(),
    }).then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
    }),
  // Admin APIs
  adminOverview: () => fetchJSON<any>('/admin/overview'),
  adminGetSettings: () => fetchJSON<{ locale: string }>('/admin/settings'),
  adminUpdateSettings: (settings: { locale?: string }) =>
    fetchJSON<any>('/admin/settings', {
      method: 'PUT',
      body: JSON.stringify(settings),
    }),
  adminFailedTasks: (params?: Record<string, string>) => {
    const qs = params ? '?' + new URLSearchParams(params).toString() : ''
    return fetchJSON<any>(`/admin/failed-tasks${qs}`)
  },
  adminLlmLogs: () => fetchJSON<any>('/admin/llm-logs'),
  adminLlmLog: (filename: string) => fetchText(`/admin/llm-logs/${encodeURIComponent(filename)}`),
  adminPrompts: () => fetchJSON<any[]>('/admin/prompts'),
  adminUpdatePrompt: (name: string, content: string) =>
    fetchJSON<any>(`/admin/prompts/${encodeURIComponent(name)}`, {
      method: 'PUT',
      body: JSON.stringify({ content }),
    }),
  adminResetPrompt: (name: string) =>
    fetchJSON<any>(`/admin/prompts/${encodeURIComponent(name)}`, { method: 'DELETE' }),
  adminRegenerateDailyPicks: () =>
    fetchJSON<any>('/admin/regenerate-daily-picks', { method: 'POST' }),
  getPerson: (tmdbPersonId: number) => fetchJSON<Person>(`/persons/${tmdbPersonId}`),
  getPersonMovies: (tmdbPersonId: number, params?: Record<string, string>) => {
    const qs = params ? '?' + new URLSearchParams(params).toString() : ''
    return fetchJSON<any>(`/persons/${tmdbPersonId}/movies${qs}`)
  },
  recommend: async (
    prompt: string,
    onStatus: (stage: string, message: string) => void,
    onResult: (data: any) => void,
    onError: (message: string) => void,
    onThinking?: (stage: string, label: string, detail: any) => void,
  ) => {
    const resp = await fetch(`${BASE}/recommend`, {
      method: 'POST',
      headers: getAuthHeaders(),
      body: JSON.stringify({ prompt }),
    })
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
    const reader = resp.body!.getReader()
    const decoder = new TextDecoder()
    let buffer = ''

    let currentEvent = ''
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop()!
      for (const line of lines) {
        if (line.startsWith('event: ')) {
          currentEvent = line.slice(7).trim()
        } else if (line.startsWith('data: ')) {
          const data = line.slice(6)
          try {
            const parsed = JSON.parse(data)
            if (currentEvent === 'status') {
              onStatus(parsed.stage, parsed.message)
            } else if (currentEvent === 'result') {
              onResult(parsed)
            } else if (currentEvent === 'error') {
              onError(parsed.message)
            } else if (currentEvent === 'thinking' && onThinking) {
              onThinking(parsed.stage, parsed.label, parsed.detail)
            }
          } catch {
            // ignore parse errors
          }
          currentEvent = ''
        }
      }
    }
  },
  authRegister: (username: string, password: string) =>
    fetchJSON<{ token: string; user: { id: number; username: string } }>('/auth/register', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    }),
  authLogin: (username: string, password: string) =>
    fetchJSON<{ token: string; user: { id: number; username: string } }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    }),
  authMe: () => fetchJSON<{ id: number; username: string }>('/auth/me'),
  getMarks: (movieId: number) =>
    fetchJSON<{ want: boolean; watched: boolean; favorite: boolean }>(`/movies/${movieId}/marks`),
  setMark: (movieId: number, markType: string) =>
    fetchJSON<{ want: boolean; watched: boolean; favorite: boolean }>(`/movies/${movieId}/marks/${markType}`, {
      method: 'PUT',
    }),
  removeMark: (movieId: number, markType: string) =>
    fetchJSON<{ want: boolean; watched: boolean; favorite: boolean }>(`/movies/${movieId}/marks/${markType}`, {
      method: 'DELETE',
    }),
  batchMarks: (movieIds: number[]) =>
    fetchJSON<Record<number, { want: boolean; watched: boolean; favorite: boolean }>>('/marks/batch', {
      method: 'POST',
      body: JSON.stringify({ movie_ids: movieIds }),
    }),
  listMarkedMovies: (markType: 'want' | 'watched' | 'favorite') =>
    fetchJSON<Movie[]>(`/marks/movies?type=${markType}`),
}
