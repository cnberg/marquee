import { useState } from 'react'
import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import { Menu } from 'lucide-react'
import { LocaleProvider, useLocale } from './i18n/LocaleContext'
import Library from './pages/Library'
import MovieDetail from './pages/MovieDetail'
import AdminLayout from './pages/AdminLayout'
import Browse from './pages/Browse'
import SearchHistory from './pages/SearchHistory'
import SearchHistoryDetail from './pages/SearchHistoryDetail'
import SharedHistory from './pages/SharedHistory'
import MyMarks from './pages/MyMarks'
import { AuthProvider, useAuth } from './auth/AuthContext'
import { AuthModal } from './components/AuthModal'
import Footer from './components/Footer'
import { Button } from './components/ui/button'
import { Sheet, SheetContent, SheetTitle, SheetTrigger } from './components/ui/sheet'
import { Separator } from './components/ui/separator'
import { cn } from './lib/utils'

function navLinkClass({ isActive }: { isActive: boolean }) {
  return cn(
    'rounded-md px-3 py-2 text-sm font-medium transition-colors',
    isActive ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:text-foreground',
  )
}

function drawerLinkClass({ isActive }: { isActive: boolean }) {
  return cn(
    'block rounded-md px-3 py-2 text-base font-medium transition-colors',
    isActive ? 'bg-primary text-primary-foreground' : 'text-foreground hover:bg-accent/40',
  )
}

function AppShell() {
  const { t, locale, setLocale } = useLocale()
  const { user, showAuthModal, logout } = useAuth()
  const [drawerOpen, setDrawerOpen] = useState(false)
  const closeDrawer = () => setDrawerOpen(false)

  return (
    <div className="flex min-h-screen flex-col bg-background text-foreground">
      <header className="sticky top-0 z-10 border-b bg-background/90 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-x-4 px-3 py-3 md:flex-wrap md:gap-y-2 md:px-6 md:py-4">
          {/* Mobile: hamburger + brand */}
          <div className="flex items-center gap-2 md:hidden">
            <Sheet open={drawerOpen} onOpenChange={setDrawerOpen}>
              <SheetTrigger asChild>
                <Button variant="ghost" size="sm" className="h-9 w-9 p-0" aria-label={t('app_menu_aria')}>
                  <Menu className="h-5 w-5" />
                </Button>
              </SheetTrigger>
              <SheetContent side="left" className="flex flex-col gap-0 p-0">
                <div className="border-b p-6">
                  <SheetTitle>Marquee</SheetTitle>
                </div>
                <nav className="flex flex-col gap-1 p-4">
                  <NavLink to="/" end onClick={closeDrawer} className={drawerLinkClass}>
                    {t('app_home')}
                  </NavLink>
                  {user && (
                    <>
                      <NavLink to="/marks" onClick={closeDrawer} className={drawerLinkClass}>
                        {t('app_my_marks')}
                      </NavLink>
                      <NavLink to="/history" onClick={closeDrawer} className={drawerLinkClass}>
                        {t('app_search_history')}
                      </NavLink>
                      <NavLink to="/admin" onClick={closeDrawer} className={drawerLinkClass}>
                        {t('app_admin')}
                      </NavLink>
                    </>
                  )}
                </nav>
                <Separator />
                <div className="flex flex-col gap-3 p-4">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => { setLocale(locale === 'en' ? 'zh' : 'en'); closeDrawer() }}
                    aria-label={t('settings_language')}
                  >
                    {locale === 'en' ? t('settings_language_zh') : t('settings_language_en')}
                  </Button>
                  {user ? (
                    <>
                      <span className="px-1 text-sm text-muted-foreground">
                        {t('nav_greeting', { username: user.username })}
                      </span>
                      <Button variant="outline" size="sm" onClick={() => { logout(); closeDrawer() }}>
                        {t('nav_logout')}
                      </Button>
                    </>
                  ) : (
                    <Button variant="outline" size="sm" onClick={() => { showAuthModal(); closeDrawer() }}>
                      {t('nav_login')}
                    </Button>
                  )}
                </div>
              </SheetContent>
            </Sheet>
            <NavLink to="/" className="text-lg font-semibold tracking-wide" aria-label={t('app_brand_aria')}>
              Marquee
            </NavLink>
          </div>

          {/* Desktop: brand */}
          <NavLink
            to="/"
            className="hidden text-lg font-semibold tracking-wide text-foreground md:inline"
            aria-label={t('app_brand_aria')}
          >
            Marquee
          </NavLink>

          {/* Desktop: nav links */}
          <nav className="hidden flex-wrap items-center gap-2 md:flex">
            <NavLink to="/" end className={navLinkClass}>
              {t('app_home')}
            </NavLink>
            {user && (
              <>
                <NavLink to="/marks" className={navLinkClass}>
                  {t('app_my_marks')}
                </NavLink>
                <NavLink to="/history" className={navLinkClass}>
                  {t('app_search_history')}
                </NavLink>
                <NavLink to="/admin" className={navLinkClass}>
                  {t('app_admin')}
                </NavLink>
              </>
            )}
          </nav>

          {/* Desktop: actions */}
          <div className="hidden items-center gap-3 md:flex">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setLocale(locale === 'en' ? 'zh' : 'en')}
              aria-label={t('settings_language')}
            >
              {locale === 'en' ? t('settings_language_zh') : t('settings_language_en')}
            </Button>
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

      <main className="mx-auto w-full max-w-7xl flex-1 px-3 py-8 md:px-6">
        <Routes>
          <Route path="/" element={<Library />} />
          <Route path="/movies/:id" element={<MovieDetail />} />
          <Route path="/browse" element={<Browse />} />
          <Route path="/marks" element={<MyMarks />} />
          <Route path="/history" element={<SearchHistory />} />
          <Route path="/history/:id" element={<SearchHistoryDetail />} />
          <Route path="/shared/:token" element={<SharedHistory />} />
          <Route path="/admin/*" element={<AdminLayout />} />
          <Route path="*" element={<Library />} />
        </Routes>
      </main>
      <Footer />
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
