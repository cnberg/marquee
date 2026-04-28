import { useEffect, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { api } from '../api/client'
import type { AdminOverviewData } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { Card } from '../components/ui/card'
import { Button } from '../components/ui/button'
import { Badge } from '../components/ui/badge'
import { Alert, AlertDescription } from '../components/ui/alert'

export default function AdminOverview() {
  const { t } = useLocale()
  const [data, setData] = useState<AdminOverviewData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [scanning, setScanning] = useState(false)
  const [regenerating, setRegenerating] = useState(false)
  const [regenMessage, setRegenMessage] = useState<string | null>(null)

  const load = async () => {
    setLoading(true)
    try {
      const res = await api.adminOverview()
      setData(res)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : t('overview_load_error'))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
  }, [])

  const handleScan = async () => {
    setScanning(true)
    try {
      await api.triggerScan()
      setTimeout(load, 2000)
    } catch (err) {
      setError(err instanceof Error ? err.message : t('overview_load_error'))
    } finally {
      setScanning(false)
    }
  }

  const handleRegenerateDailyPicks = async () => {
    setRegenerating(true)
    setRegenMessage(null)
    try {
      await api.adminRegenerateDailyPicks()
      setRegenMessage(t('overview_regen_success'))
    } catch (err) {
      setRegenMessage(err instanceof Error ? err.message : t('overview_regen_error'))
    } finally {
      setRegenerating(false)
    }
  }

  if (loading) return <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>
  if (error)
    return (
      <Alert variant="destructive">
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    )
  if (!data) return null

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <h2 className="text-2xl font-semibold">{t('overview_title')}</h2>
        <div className="flex flex-wrap gap-2">
          <Button type="button" onClick={handleScan} disabled={scanning}>
            {scanning ? t('overview_scanning') : t('overview_scan_btn')}
          </Button>
          <Button type="button" variant="secondary" onClick={handleRegenerateDailyPicks} disabled={regenerating}>
            {regenerating ? t('overview_regenerating') : t('overview_regen_btn')}
          </Button>
        </div>
      </div>

      {regenMessage && (
        <Alert>
          <AlertDescription>{regenMessage}</AlertDescription>
        </Alert>
      )}

      <KpiStrip data={data} />

      <ChartGrid data={data} />

      <TasksTable tasks={data.tasks} />
    </div>
  )
}

// ============================================================================
// KPI 指标条
// ============================================================================

function KpiStrip({ data }: { data: AdminOverviewData }) {
  const { t } = useLocale()

  const statusCount = (s: string) => data.dir_status.find(([k]) => k === s)?.[1] ?? 0
  const sourceCount = (s: string) => data.movies_by_source.find(([k]) => k === s)?.[1] ?? 0

  const matched = statusCount('matched')
  const parsed = statusCount('parsed')
  const failed = statusCount('failed')
  const newCount = statusCount('new')
  const total = data.dir_total
  const processed = matched + parsed + failed
  const progressPct = total > 0 ? Math.round((processed / total) * 100) : 0

  const libraryCount = sourceCount('library')
  const relatedCount = sourceCount('related')

  const wantCount = data.mark_counts.want ?? 0
  const watchedCount = data.mark_counts.watched ?? 0
  const favoriteCount = data.mark_counts.favorite ?? 0

  let runningTasks = 0
  for (const statuses of Object.values(data.tasks)) {
    runningTasks += statuses.running ?? 0
  }

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-7">
      <KpiCard label={t('overview_kpi_library')} value={libraryCount} />
      <KpiCard label={t('overview_kpi_related')} value={relatedCount} />
      <KpiCard
        label={t('overview_kpi_progress')}
        value={`${processed}/${total}`}
        sub={`${progressPct}%${newCount > 0 ? ` · ${newCount} ${t('overview_pending_program')}` : ''}`}
      />
      <KpiCard label={t('overview_kpi_pending_review')} value={parsed} tone={parsed > 0 ? 'warn' : undefined} />
      <KpiCard label={t('overview_kpi_match_failed')} value={failed} tone={failed > 0 ? 'danger' : undefined} />
      <KpiCard
        label={t('overview_kpi_marks')}
        value={wantCount + watchedCount + favoriteCount}
        sub={`${t('mark_want')} ${wantCount} · ${t('mark_watched')} ${watchedCount} · ${t('mark_favorite')} ${favoriteCount}`}
      />
      <KpiCard label={t('overview_kpi_running_tasks')} value={runningTasks} tone={runningTasks > 0 ? 'info' : undefined} />
    </div>
  )
}

function KpiCard({
  label,
  value,
  sub,
  tone,
}: {
  label: string
  value: number | string
  sub?: string
  tone?: 'warn' | 'danger' | 'info'
}) {
  const toneClass =
    tone === 'danger'
      ? 'text-destructive'
      : tone === 'warn'
        ? 'text-amber-600 dark:text-amber-400'
        : tone === 'info'
          ? 'text-primary'
          : 'text-foreground'

  return (
    <Card className="p-3">
      <div className="text-xs font-medium text-muted-foreground">{label}</div>
      <div className={`mt-1 text-2xl font-semibold tabular-nums ${toneClass}`}>{value}</div>
      {sub && <div className="mt-1 text-xs text-muted-foreground">{sub}</div>}
    </Card>
  )
}

// ============================================================================
// 图表网格
// ============================================================================

// 主色 + 柔和辅色一套，和 shadcn 的 muted/primary 对齐
const CHART_COLORS = [
  'hsl(217 91% 60%)', // blue
  'hsl(142 71% 45%)', // green
  'hsl(38 92% 50%)', // amber
  'hsl(346 77% 50%)', // rose
  'hsl(262 83% 58%)', // violet
  'hsl(180 62% 45%)', // teal
  'hsl(24 75% 50%)', // orange
  'hsl(291 64% 42%)', // magenta
  'hsl(45 93% 47%)', // yellow
  'hsl(200 70% 50%)', // cyan
]

const PRIMARY_COLOR = 'hsl(217 91% 60%)'
const MUTED_COLOR = 'hsl(215 16% 60%)'

function ChartGrid({ data }: { data: AdminOverviewData }) {
  const { t } = useLocale()

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <ChartCard title={t('overview_dir_status')}>
        <StatusBarChart rows={data.dir_status} labelFor={(s) => t(`overview_dir_${s}`)} />
      </ChartCard>

      <ChartCard title={t('overview_match_status')}>
        <StatusBarChart rows={data.match_status} />
      </ChartCard>

      <ChartCard title={t('overview_movies_by_source')}>
        <SourcePieChart rows={data.movies_by_source} labelFor={(s) => t(`overview_movies_${s}`) ?? s} />
      </ChartCard>

      <ChartCard title={t('overview_chart_year')}>
        <TopBarChart
          rows={data.year_buckets}
          labelFor={(s) => (s === 'unknown' ? t('overview_chart_unknown') : s)}
        />
      </ChartCard>

      <ChartCard title={t('overview_chart_country')}>
        <TopBarChart rows={data.country_top} />
      </ChartCard>

      <ChartCard title={t('overview_chart_genre')}>
        <TopBarChart rows={data.genre_top} />
      </ChartCard>

      <ChartCard title={t('overview_chart_rating')} className="lg:col-span-2">
        <RatingHistogramChart rows={data.rating_histogram} unratedLabel={t('overview_chart_unrated')} />
      </ChartCard>
    </div>
  )
}

function ChartCard({
  title,
  children,
  className,
}: {
  title: string
  children: React.ReactNode
  className?: string
}) {
  return (
    <Card className={`p-4 ${className ?? ''}`}>
      <h3 className="mb-3 text-sm font-semibold text-muted-foreground">{title}</h3>
      {children}
    </Card>
  )
}

/** 目录状态 / 匹配状态：水平条形图 */
function StatusBarChart({
  rows,
  labelFor,
}: {
  rows: [string, number][]
  labelFor?: (key: string) => string
}) {
  if (rows.length === 0) {
    return <EmptyHint />
  }
  const data = rows.map(([k, v]) => ({ name: labelFor ? labelFor(k) : k, value: v }))
  return (
    <ResponsiveContainer width="100%" height={220}>
      <BarChart data={data} layout="vertical" margin={{ top: 4, right: 16, left: 4, bottom: 4 }}>
        <CartesianGrid horizontal={false} stroke="var(--color-border, #e5e7eb)" />
        <XAxis type="number" allowDecimals={false} tick={{ fontSize: 12 }} />
        <YAxis type="category" dataKey="name" width={96} tick={{ fontSize: 12 }} />
        <Tooltip cursor={{ fill: 'rgba(0,0,0,0.04)' }} />
        <Bar dataKey="value" fill={PRIMARY_COLOR} radius={[0, 4, 4, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}

/** 影片库构成：饼图 */
function SourcePieChart({
  rows,
  labelFor,
}: {
  rows: [string, number][]
  labelFor: (key: string) => string
}) {
  if (rows.length === 0 || rows.every(([, v]) => v === 0)) {
    return <EmptyHint />
  }
  const data = rows.map(([k, v]) => ({ name: labelFor(k), value: v, raw: k }))
  const total = data.reduce((s, d) => s + d.value, 0)
  return (
    <ResponsiveContainer width="100%" height={220}>
      <PieChart>
        <Pie
          data={data}
          cx="50%"
          cy="50%"
          innerRadius={50}
          outerRadius={80}
          paddingAngle={2}
          dataKey="value"
        >
          {data.map((entry) => (
            <Cell
              key={entry.raw}
              fill={entry.raw === 'library' ? PRIMARY_COLOR : MUTED_COLOR}
              stroke="var(--color-card, #ffffff)"
              strokeWidth={2}
            />
          ))}
        </Pie>
        <Tooltip
          formatter={(value, _name, item) => {
            const num = Number(value ?? 0)
            const pct = total > 0 ? ((num / total) * 100).toFixed(1) : '0'
            return [`${num} (${pct}%)`, item?.payload?.name ?? '']
          }}
        />
        <Legend iconType="circle" wrapperStyle={{ fontSize: 12 }} />
      </PieChart>
    </ResponsiveContainer>
  )
}

/** Top N / 年代分布：水平条形图，按 value 降序 */
function TopBarChart({
  rows,
  labelFor,
}: {
  rows: [string, number][]
  labelFor?: (key: string) => string
}) {
  if (rows.length === 0) {
    return <EmptyHint />
  }
  const data = rows.map(([k, v]) => ({ name: labelFor ? labelFor(k) : k, value: v }))
  const height = Math.max(200, Math.min(360, data.length * 28 + 40))
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} layout="vertical" margin={{ top: 4, right: 16, left: 4, bottom: 4 }}>
        <CartesianGrid horizontal={false} stroke="var(--color-border, #e5e7eb)" />
        <XAxis type="number" allowDecimals={false} tick={{ fontSize: 12 }} />
        <YAxis type="category" dataKey="name" width={96} tick={{ fontSize: 12 }} />
        <Tooltip cursor={{ fill: 'rgba(0,0,0,0.04)' }} />
        <Bar dataKey="value" radius={[0, 4, 4, 0]}>
          {data.map((_, i) => (
            <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

/** TMDB 评分直方图：垂直柱状图，0-1..9-10 顺序 */
function RatingHistogramChart({
  rows,
  unratedLabel,
}: {
  rows: [string, number][]
  unratedLabel: string
}) {
  if (rows.length === 0) {
    return <EmptyHint />
  }
  // 按 bucket 顺序强制排列
  const order = ['0-1', '1-2', '2-3', '3-4', '4-5', '5-6', '6-7', '7-8', '8-9', '9-10', 'unrated']
  const map = new Map(rows)
  const data = order
    .filter((k) => map.has(k))
    .map((k) => ({ name: k === 'unrated' ? unratedLabel : k, value: map.get(k) ?? 0 }))

  return (
    <ResponsiveContainer width="100%" height={240}>
      <BarChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
        <CartesianGrid vertical={false} stroke="var(--color-border, #e5e7eb)" />
        <XAxis dataKey="name" tick={{ fontSize: 12 }} />
        <YAxis allowDecimals={false} tick={{ fontSize: 12 }} />
        <Tooltip cursor={{ fill: 'rgba(0,0,0,0.04)' }} />
        <Bar dataKey="value" fill={PRIMARY_COLOR} radius={[4, 4, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}

function EmptyHint() {
  const { t } = useLocale()
  return (
    <div className="flex h-[200px] items-center justify-center text-sm text-muted-foreground">
      {t('overview_chart_empty')}
    </div>
  )
}

// ============================================================================
// 任务队列表格
// ============================================================================

function TasksTable({ tasks }: { tasks: Record<string, Record<string, number>> }) {
  const { t } = useLocale()
  return (
    <Card className="space-y-3 p-4">
      <h3 className="text-sm font-semibold text-muted-foreground">{t('overview_tasks')}</h3>
      {Object.keys(tasks).length === 0 ? (
        <div className="text-sm text-muted-foreground">{t('overview_no_tasks')}</div>
      ) : (
        <div className="overflow-hidden rounded-lg border">
          <table className="w-full text-sm">
            <thead className="bg-muted/50">
              <tr>
                <th className="px-3 py-2 text-left font-medium">{t('overview_task_type')}</th>
                <th className="px-3 py-2 text-left font-medium">pending</th>
                <th className="px-3 py-2 text-left font-medium">running</th>
                <th className="px-3 py-2 text-left font-medium">done</th>
                <th className="px-3 py-2 text-left font-medium">failed</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(tasks).map(([type, statuses]) => (
                <tr key={type} className="border-t border-border">
                  <td className="px-3 py-2">{type}</td>
                  <td className="px-3 py-2 tabular-nums">{statuses.pending ?? 0}</td>
                  <td className="px-3 py-2 tabular-nums">{statuses.running ?? 0}</td>
                  <td className="px-3 py-2 tabular-nums">{statuses.done ?? 0}</td>
                  <td className="px-3 py-2">
                    <Badge variant={statuses.failed ? 'destructive' : 'secondary'}>
                      {statuses.failed ?? 0}
                    </Badge>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  )
}
