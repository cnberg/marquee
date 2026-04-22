import { NavLink, Route, Routes } from 'react-router-dom'
import { useEffect } from 'react'
import { useLocale } from '../i18n/LocaleContext'
import AdminOverview from './AdminOverview'
import AdminPending from './AdminPending'
import AdminErrors from './AdminErrors'
import AdminPrompts from './AdminPrompts'
import { Card } from '../components/ui/card'
import { cn } from '../lib/utils'

export default function AdminLayout() {
  const { t } = useLocale()
  useEffect(() => {
    document.title = t('admin_page_title')
  }, [t])

  return (
    <div className="flex flex-col gap-6 lg:flex-row">
      <Card className="w-full max-w-xs self-start border bg-card/80 p-4 backdrop-blur">
        <div className="space-y-4">
          <h2 className="text-xl font-semibold">{t('admin_title')}</h2>
          <nav className="space-y-2">
            <AdminNavLink to="/admin" end label={t('admin_nav_overview')} />
            <AdminNavLink to="/admin/pending" label={t('admin_nav_pending')} />
            <AdminNavLink to="/admin/errors" label={t('admin_nav_errors')} />
            <AdminNavLink to="/admin/prompts" label={t('admin_nav_prompts')} />
          </nav>
        </div>
      </Card>
      <div className="flex-1">
        <Routes>
          <Route index element={<AdminOverview />} />
          <Route path="pending" element={<AdminPending />} />
          <Route path="errors" element={<AdminErrors />} />
          <Route path="prompts" element={<AdminPrompts />} />
        </Routes>
      </div>
    </div>
  )
}

function AdminNavLink({ to, label, end }: { to: string; label: string; end?: boolean }) {
  return (
    <NavLink
      to={to}
      end={end}
      className={({ isActive }) =>
        cn(
          'flex items-center rounded-md px-3 py-2 text-sm font-medium transition hover:bg-accent hover:text-accent-foreground',
          isActive ? 'bg-accent text-accent-foreground shadow-sm' : 'text-muted-foreground',
        )
      }
    >
      {label}
    </NavLink>
  )
}
