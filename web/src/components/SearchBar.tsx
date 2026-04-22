import { useLocale } from '../i18n/LocaleContext'
import { Input } from './ui/input'
import { Button } from './ui/button'
import { Search as SearchIcon } from 'lucide-react'

interface SearchBarProps {
  value: string
  onSearch: (value: string) => void
  placeholder?: string
}

export function SearchBar({ value, onSearch, placeholder }: SearchBarProps) {
  const { t } = useLocale()
  return (
    <div className="flex w-full items-center gap-2">
      <Input
        type="search"
        value={value}
        onChange={(e) => onSearch(e.target.value)}
        placeholder={placeholder ?? t('search_placeholder')}
        aria-label={t('search_aria')}
      />
      <Button type="button" onClick={() => onSearch(value)}>
        <SearchIcon className="mr-2 h-4 w-4" />
        {t('search_aria')}
      </Button>
    </div>
  )
}

export default SearchBar
