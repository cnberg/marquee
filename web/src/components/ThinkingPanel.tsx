import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from './ui/card'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { useLocale } from '../i18n/LocaleContext'

interface ThinkingEntry {
  stage: string
  label_key?: string
  label: string
  detail: any
}

// Translate a field name like "query_type" via i18n key `thinking_field_query_type`.
// Falls back to the raw name if no translation is registered.
function translateFieldName(t: (key: string) => string, name: string): string {
  const key = `thinking_field_${name}`
  const translated = t(key)
  return translated === key ? name : translated
}

interface Props {
  entries: ThinkingEntry[]
}

// Stages whose detail we render as a bullet list of non-null fields instead of JSON.
const FIELD_LIST_STAGES = new Set(['understanding', 'recall', 'selecting', 'classification', 'matching', 'expanding', 'ranking'])

function isEmpty(v: any): boolean {
  if (v == null || v === '') return true
  if (Array.isArray(v)) return v.length === 0
  if (typeof v === 'object') return Object.values(v).every(isEmpty)
  return false
}

function isPlainObject(v: any): boolean {
  return v !== null && typeof v === 'object' && !Array.isArray(v)
}

function FieldNode({ value, name }: { value: any; name?: string }) {
  const { t } = useLocale()
  const displayName = name ? translateFieldName(t, name) : undefined
  // Object: render as labeled group with indented children
  if (isPlainObject(value)) {
    const entries = Object.entries(value).filter(([, v]) => !isEmpty(v))
    if (entries.length === 0) return null
    return (
      <div>
        {displayName && <div className="font-medium">{displayName}</div>}
        <ul className={displayName ? 'ml-4 space-y-0.5' : 'space-y-0.5'}>
          {entries.map(([k, v]) => (
            <li key={k}>
              <FieldNode value={v} name={k} />
            </li>
          ))}
        </ul>
      </div>
    )
  }
  // Array of objects: render each item as a sub-FieldNode (avoids "[object Object]")
  if (Array.isArray(value) && value.some((item) => isPlainObject(item))) {
    return (
      <div>
        {displayName && <div className="font-medium">{displayName}</div>}
        <ul className={displayName ? 'ml-4 space-y-1' : 'space-y-1'}>
          {value.map((item, i) => (
            <li key={i}>
              <FieldNode value={item} />
            </li>
          ))}
        </ul>
      </div>
    )
  }
  // Array of scalars: join with comma
  // Scalar: just stringify
  const display = Array.isArray(value) ? value.join(', ') : String(value)
  return (
    <span>
      {displayName && <span className="font-medium">{displayName}: </span>}
      <span>{display}</span>
    </span>
  )
}

function FieldList({ detail }: { detail: any }) {
  if (isEmpty(detail)) {
    return <div className="text-xs text-muted-foreground">（空）</div>
  }
  return (
    <div className="text-xs text-muted-foreground">
      <FieldNode value={detail} />
    </div>
  )
}

function ThinkingEntryRow({ entry }: { entry: ThinkingEntry }) {
  const { t } = useLocale()
  // Inner entries default expanded — once the user opens the outer panel,
  // they're already saying "show me everything".
  const [open, setOpen] = useState(true)
  const useFieldList = FIELD_LIST_STAGES.has(entry.stage)
  // Prefer i18n key (new SSE format). Fall back to `label` for historic records
  // written before label_key existed.
  const displayLabel = entry.label_key ? t(entry.label_key) : entry.label
  return (
    <div>
      <button
        type="button"
        className="flex w-full items-center justify-between py-2 text-left"
        onClick={() => setOpen((v) => !v)}
      >
        <span className="text-xs font-medium text-muted-foreground">{displayLabel}</span>
        {open ? (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
        )}
      </button>
      {open && (
        <div className="pb-2">
          {useFieldList ? (
            <FieldList detail={entry.detail} />
          ) : (
            <pre className="whitespace-pre-wrap text-xs font-mono text-muted-foreground">
              {JSON.stringify(entry.detail, null, 2)}
            </pre>
          )}
        </div>
      )}
    </div>
  )
}

export default function ThinkingPanel({ entries }: Props) {
  if (entries.length === 0) return null
  // Outer panel default collapsed; user opens it explicitly to see the inside.
  const [open, setOpen] = useState(false)
  return (
    <Card className="mt-6">
      <CardHeader
        className="flex cursor-pointer flex-row items-center justify-between"
        onClick={() => setOpen((v) => !v)}
      >
        <CardTitle className="text-sm text-foreground">Thinking</CardTitle>
        {open ? (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
        )}
      </CardHeader>
      {open && (
        <CardContent className="divide-y">
          {entries.map((entry, i) => (
            <ThinkingEntryRow key={i} entry={entry} />
          ))}
        </CardContent>
      )}
    </Card>
  )
}
