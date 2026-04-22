import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import { LocaleProvider, useLocale } from './i18n/LocaleContext'
import Library from './pages/Library'
import MovieDetail from './pages/MovieDetail'
import AdminLayout from './pages/AdminLayout'
import Browse from './pages/Browse'
import SearchHistory from './pages/SearchHistory'
import SearchHistoryDetail from './pages/SearchHistoryDetail'
import MyMarks from './pages/MyMarks'
import { AuthProvider, useAuth } from './auth/AuthContext'
import { AuthModal } from './components/AuthModal'
import { Button } from './components/ui/button'
import { cn } from './lib/utils'

function AppShell() {
  const { t } = useLocale()
  const { user, showAuthModal, logout } = useAuth()
  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="sticky top-0 z-10 border-b bg-background/90 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-6 py-4">
          <NavLink
            to="/"
            className="text-lg font-semibold tracking-wide text-foreground"
            aria-label={t('app_brand_aria')}
          >
            Marquee
          </NavLink>
          <nav className="flex items-center gap-2">
            <NavLink
              to="/"
              end
              className={({ isActive }) =>
                cn(
                  'rounded-md px-3 py-2 text-sm font-medium transition-colors',
                  isActive ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:text-foreground',
                )
              }
            >
              {t('app_home')}
            </NavLink>
            {user && (
              <>
                <NavLink
                  to="/marks"
                  className={({ isActive }) =>
                    cn(
                      'rounded-md px-3 py-2 text-sm font-medium transition-colors',
                      isActive ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:text-foreground',
                    )
                  }
                >
                  {t('app_my_marks')}
                </NavLink>
                <NavLink
                  to="/history"
                  className={({ isActive }) =>
                    cn(
                      'rounded-md px-3 py-2 text-sm font-medium transition-colors',
                      isActive ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:text-foreground',
                    )
                  }
                >
                  {t('app_search_history')}
                </NavLink>
              </>
            )}
            <NavLink
              to="/admin"
              className={({ isActive }) =>
                cn(
                  'rounded-md px-3 py-2 text-sm font-medium transition-colors',
                  isActive ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:text-foreground',
                )
              }
            >
              {t('app_admin')}
            </NavLink>
          </nav>
          <div className="flex items-center gap-3">
            {user ? (
              <>
                <span className="text-sm text-muted-foreground">{t('nav_greeting', { username: user.username })}</span>
                <Button variant="outline" size="sm" onClick={logout}>
                  {t('nav_logout')}
                </Button>
              </>
            ) : (
              <Button variant="outline" size="sm" onClick={showAuthModal}>
                {t('nav_login')}
              </Button>
            )}
          </div>
        </div>
      </header>

      <main className="mx-auto w-full max-w-7xl px-6 py-8">
        <Routes>
          <Route path="/" element={<Library />} />
          <Route path="/movies/:id" element={<MovieDetail />} />
          <Route path="/browse" element={<Browse />} />
          <Route path="/marks" element={<MyMarks />} />
          <Route path="/history" element={<SearchHistory />} />
          <Route path="/history/:id" element={<SearchHistoryDetail />} />
          <Route path="/admin/*" element={<AdminLayout />} />
          <Route path="*" element={<Library />} />
        </Routes>
      </main>
      <AuthModal />
    </div>
  )
}

function App() {
  return (
    <BrowserRouter>
      <LocaleProvider>
        <AuthProvider>
          <AppShell />
        </AuthProvider>
      </LocaleProvider>
    </BrowserRouter>
  )
}

export default App
