import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react'
import { api } from '../api/client'

interface User {
  id: number
  username: string
}

interface AuthContextValue {
  user: User | null
  token: string | null
  login: (username: string, password: string) => Promise<void>
  register: (username: string, password: string) => Promise<void>
  logout: () => void
  showAuthModal: () => void
  authModalOpen: boolean
  closeAuthModal: () => void
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [token, setToken] = useState<string | null>(localStorage.getItem('auth_token'))
  const [authModalOpen, setAuthModalOpen] = useState(false)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    if (token) {
      api
        .authMe()
        .then((u) => setUser(u))
        .catch(() => {
          localStorage.removeItem('auth_token')
          setToken(null)
        })
        .finally(() => setLoaded(true))
    } else {
      setLoaded(true)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const login = useCallback(async (username: string, password: string) => {
    const res = await api.authLogin(username, password)
    localStorage.setItem('auth_token', res.token)
    setToken(res.token)
    setUser(res.user)
  }, [])

  const register = useCallback(async (username: string, password: string) => {
    const res = await api.authRegister(username, password)
    localStorage.setItem('auth_token', res.token)
    setToken(res.token)
    setUser(res.user)
  }, [])

  const logout = useCallback(() => {
    localStorage.removeItem('auth_token')
    setToken(null)
    setUser(null)
  }, [])

  const showAuthModal = useCallback(() => setAuthModalOpen(true), [])
  const closeAuthModal = useCallback(() => setAuthModalOpen(false), [])

  if (!loaded) return null

  return (
    <AuthContext.Provider
      value={{ user, token, login, register, logout, showAuthModal, authModalOpen, closeAuthModal }}
    >
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}
