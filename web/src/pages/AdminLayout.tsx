import { NavLink, Route, Routes } from 'react-router-dom'
import { useEffect } from 'react'
import { useLocale } from '../i18n/LocaleContext'
import AdminOverview from './AdminOverview'
import AdminPending from './AdminPending'
import AdminErrors from './AdminErrors'
import AdminPrompts from './AdminPrompts'
import AdminBenchmark from './AdminBenchmark'
import AdminBenchmarkDetail from './AdminBenchmarkDetail'
import AdminConfig from './AdminConfig'
import AdminImportDouban from './AdminImportDouban'
import AdminMultiVersion from './AdminMultiVersion'
import { Card } from '../components/ui/card'
import { cn } from '../lib/utils'

export default function AdminLayout() {
  const { t } = useLocale()
  useEffect(() => {
    document.title = t('admin_page_title')
  }, [t])

  return (
    <div className="flex flex-col gap-6 lg:flex-row">
      <Card className="w-full max-w-[180px] self-start border bg-card/80 p-3 backdrop-blur">
        <div className="space-y-4">
          <h2 className="text-xl font-semibold">{t('admin_title')}</h2>
          <nav className="space-y-2">
            <AdminNavLink to="/admin" end label={t('admin_nav_overview')} />
            <AdminNavLink to="/admin/pending" label={t('admin_nav_pending')} />
            <AdminNavLink to="/admin/errors" label={t('admin_nav_errors')} />
            <AdminNavLink to="/admin/prompts" label={t('admin_nav_prompts')} />
            <AdminNavLink to="/admin/benchmark" label={t('admin_nav_benchmark')} />
            <AdminNavLink to="/admin/config" label={t('admin_nav_config')} />
            <AdminNavLink to="/admin/import-douban" label={t('admin_nav_import_douban')} />
            <AdminNavLink to="/admin/multi-version" label={t('admin_nav_multiver')} />
          </nav>
        </div>
      </Card>
      <div className="flex-1">
        <Routes>
          <Route index element={<AdminOverview />} />
          <Route path="pending" element={<AdminPending />} />
          <Route path="errors" element={<AdminErrors />} />
          <Route path="prompts" element={<AdminPrompts />} />
          <Route path="benchmark" element={<AdminBenchmark />} />
          <Route path="benchmark/queries/:id" element={<AdminBenchmarkDetail />} />
          <Route path="config" element={<AdminConfig />} />
          <Route path="import-douban" element={<AdminImportDouban />} />
          <Route path="multi-version" element={<AdminMultiVersion />} />
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
          'flex items-center rounded-md px-3 py-1.5 text-sm font-medium transition hover:bg-secondary/20 hover:text-foreground',
          isActive ? 'bg-secondary text-secondary-foreground shadow-sm' : 'text-muted-foreground',
        )
      }
    >
      {label}
    </NavLink>
  )
}
