import type { FilterOptions } from '../types'
import { useLocale } from '../i18n/LocaleContext'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select'
import { Button } from './ui/button'

const dimensionOrder = [
  'decades',
  'genres',
  'countries',
  'languages',
  'ratings',
  'runtimes',
] as const

type DimensionKey = keyof FilterOptions

const dimensionToParam: Record<DimensionKey, string> = {
  decades: 'decade',
  genres: 'genre',
  countries: 'country',
  languages: 'language',
  ratings: 'rating',
  runtimes: 'runtime',
}

interface Props {
  filters: FilterOptions | null
  selected: Record<string, string>
  onSelect: (paramName: string, value: string | null) => void
}

export default function FilterBar({ filters, selected, onSelect }: Props) {
  const { t } = useLocale()
  if (!filters) return null

  const dimensionLabels: Record<string, string> = {
    decades: t('filter_decades'),
    genres: t('filter_genres'),
    countries: t('filter_countries'),
    languages: t('filter_languages'),
    ratings: t('filter_ratings'),
    runtimes: t('filter_runtimes'),
  }

  return (
    <div className="space-y-4 rounded-xl border bg-card p-4 shadow-sm">
      {dimensionOrder.map((key) => {
        const options = filters[key]
        if (!options || options.length === 0) return null
        const param = dimensionToParam[key]
        const active = selected[param]
        return (
          <div key={key} className="space-y-2">
            <div className="flex items-center justify-between gap-2">
              <span className="text-sm font-medium text-foreground">{dimensionLabels[key]}</span>
              {active && (
                <Button variant="ghost" size="sm" onClick={() => onSelect(param, null)}>
                  <span className="sr-only">{dimensionLabels[key]}</span>
                  <span aria-hidden>×</span>
                </Button>
              )}
            </div>
            <Select value={active ?? undefined} onValueChange={(v) => onSelect(param, v)}>
              <SelectTrigger className="w-full sm:w-64">
                <SelectValue placeholder={dimensionLabels[key]} />
              </SelectTrigger>
              <SelectContent>
                {options.map(([value, count]) => {
                  const translated = t(value)
                  const label = translated === value ? value : translated
                  return (
                    <SelectItem key={value} value={value}>
                      {label} ({count})
                    </SelectItem>
                  )
                })}
              </SelectContent>
            </Select>
          </div>
        )
      })}
    </div>
  )
}
