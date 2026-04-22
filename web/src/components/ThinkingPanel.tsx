import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from './ui/card'
import { ChevronDown, ChevronRight } from 'lucide-react'

interface ThinkingEntry {
  stage: string
  label: string
  detail: any
}

interface Props {
  entries: ThinkingEntry[]
}

export default function ThinkingPanel({ entries }: Props) {
  if (entries.length === 0) return null
  const [open, setOpen] = useState(true)
  return (
    <Card className="mt-6">
      <CardHeader
        className="flex cursor-pointer flex-row items-center justify-between"
        onClick={() => setOpen((v) => !v)}
      >
        <CardTitle className="text-sm text-foreground">Thinking</CardTitle>
        {open ? <ChevronDown className="h-4 w-4 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 text-muted-foreground" />}
      </CardHeader>
      {open && (
        <CardContent className="space-y-3">
          {entries.map((entry, i) => (
            <div key={i} className="rounded-lg border bg-muted/40 p-3">
              <div className="text-xs font-medium text-muted-foreground">{entry.label}</div>
              <pre className="mt-2 whitespace-pre-wrap text-xs font-mono text-muted-foreground">
                {JSON.stringify(entry.detail, null, 2)}
              </pre>
            </div>
          ))}
        </CardContent>
      )}
    </Card>
  )
}
