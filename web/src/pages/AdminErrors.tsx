import { useEffect, useState } from 'react'
import { api } from '../api/client'
import { useLocale } from '../i18n/LocaleContext'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs'
import { Card } from '../components/ui/card'
import { Badge } from '../components/ui/badge'

interface LlmLogEntry {
  filename: string
  size: number
  modified: string
}

interface FailedTask {
  id: number
  task_type: string
  payload: string | null
  status: string
  retries: number
  max_retries: number
  error_msg: string | null
  created_at: string
  updated_at: string
}

export default function AdminErrors() {
  const { t } = useLocale()
  const [tab, setTab] = useState<'tasks' | 'llm'>('tasks')

  const [tasks, setTasks] = useState<FailedTask[]>([])
  const [tasksTotal, setTasksTotal] = useState(0)
  const [tasksLoading, setTasksLoading] = useState(false)

  const [logs, setLogs] = useState<LlmLogEntry[]>([])
  const [logsLoading, setLogsLoading] = useState(false)
  const [selectedLog, setSelectedLog] = useState<string | null>(null)
  const [logContent, setLogContent] = useState<string>('')
  const [logContentLoading, setLogContentLoading] = useState(false)

  const loadTasks = async () => {
    setTasksLoading(true)
    try {
      const res = await api.adminFailedTasks()
      setTasks(res.data ?? [])
      setTasksTotal(res.total ?? 0)
    } catch {
      // ignore
    } finally {
      setTasksLoading(false)
    }
  }

  const loadLogs = async () => {
    setLogsLoading(true)
    try {
      const res = await api.adminLlmLogs()
      setLogs(res)
    } catch {
      // ignore
    } finally {
      setLogsLoading(false)
    }
  }

  useEffect(() => {
    if (tab === 'tasks') {
      loadTasks()
    } else {
      loadLogs()
    }
  }, [tab])

  const handleLogClick = async (filename: string) => {
    if (selectedLog === filename) {
      setSelectedLog(null)
      return
    }
    setSelectedLog(filename)
    setLogContentLoading(true)
    try {
      const content = await api.adminLlmLog(filename)
      setLogContent(typeof content === 'string' ? content : JSON.stringify(content, null, 2))
    } catch {
      setLogContent(t('errors_log_load_fail'))
    } finally {
      setLogContentLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">{t('errors_title')}</h2>
      <Tabs value={tab} onValueChange={(v) => setTab(v as typeof tab)}>
        <TabsList>
          <TabsTrigger value="tasks">
            {t('errors_failed_tasks')} <Badge variant="secondary" className="ml-2">{tasksTotal}</Badge>
          </TabsTrigger>
          <TabsTrigger value="llm">{t('errors_llm_logs')}</TabsTrigger>
        </TabsList>

        <TabsContent value="tasks">
          <Card className="p-4">
            {tasksLoading ? (
              <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>
            ) : tasks.length === 0 ? (
              <div className="text-sm text-muted-foreground">{t('errors_no_tasks')}</div>
            ) : (
              <div className="overflow-hidden rounded-lg border">
                <table className="w-full text-sm">
                  <thead className="bg-muted/50">
                    <tr>
                      <th className="px-3 py-2 text-left font-medium">{t('errors_col_id')}</th>
                      <th className="px-3 py-2 text-left font-medium">{t('errors_col_type')}</th>
                      <th className="px-3 py-2 text-left font-medium">{t('errors_col_error')}</th>
                      <th className="px-3 py-2 text-left font-medium">{t('errors_col_retries')}</th>
                      <th className="px-3 py-2 text-left font-medium">{t('errors_col_time')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tasks.map((t) => (
                      <tr key={t.id} className="border-t border-border">
                        <td className="px-3 py-2">{t.id}</td>
                        <td className="px-3 py-2">{t.task_type}</td>
                        <td className="px-3 py-2 text-muted-foreground" title={t.error_msg ?? ''}>{t.error_msg ?? '-'}</td>
                        <td className="px-3 py-2">{t.retries}/{t.max_retries}</td>
                        <td className="px-3 py-2">{t.updated_at}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
        </TabsContent>

        <TabsContent value="llm">
          <Card className="space-y-3 p-4">
            {logsLoading ? (
              <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>
            ) : logs.length === 0 ? (
              <div className="text-sm text-muted-foreground">{t('errors_no_logs')}</div>
            ) : (
              <div className="space-y-2">
                {logs.map((log) => (
                  <div key={log.filename} className="rounded-lg border bg-card/70">
                    <button
                      type="button"
                      className="flex w-full items-center justify-between px-3 py-2 text-left text-sm font-medium"
                      onClick={() => handleLogClick(log.filename)}
                    >
                      <span>{log.filename}</span>
                      <span className="text-xs text-muted-foreground">{log.modified} · {(log.size / 1024).toFixed(1)} KB</span>
                    </button>
                    {selectedLog === log.filename && (
                      <div className="border-t px-3 py-2">
                        {logContentLoading ? (
                          <div className="text-sm text-muted-foreground">{t('errors_loading')}</div>
                        ) : (
                          <pre className="max-h-80 overflow-auto rounded-md bg-muted/40 p-2 text-xs leading-relaxed whitespace-pre-wrap">
                            {logContent}
                          </pre>
                        )}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  )
}
