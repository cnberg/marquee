import type { MouseEvent, ReactElement } from 'react'
import { useLocale } from '../i18n/LocaleContext'
import { Button } from './ui/button'
import { Bookmark, Check, Heart } from 'lucide-react'

type MarkType = 'want' | 'watched' | 'favorite'

interface MovieMarkButtonsProps {
  movieId: number
  marks: { want: boolean; watched: boolean; favorite: boolean }
  onToggle: (markType: MarkType) => void
  size?: 'sm' | 'lg'
}

export function MovieMarkButtons({ movieId, marks, onToggle, size = 'sm' }: MovieMarkButtonsProps) {
  const { t } = useLocale()

  const handleClick = (markType: MarkType) => (e: MouseEvent<HTMLButtonElement>) => {
    e.preventDefault()
    e.stopPropagation()
    onToggle(markType)
  }

  const buttons: Array<{ key: MarkType; label: string; icon: ReactElement }> = [
    {
      key: 'want',
      label: t('mark_want'),
      icon: <Bookmark className="h-4 w-4" aria-hidden />,
    },
    {
      key: 'watched',
      label: t('mark_watched'),
      icon: <Check className="h-4 w-4" aria-hidden />,
    },
    {
      key: 'favorite',
      label: t('mark_favorite'),
      icon: <Heart className="h-4 w-4" aria-hidden />,
    },
  ]

  return (
    <div className="flex items-center gap-2" data-movie={movieId}>
      {buttons.map(({ key, label, icon }) => (
        <Button
          key={key}
          variant={marks[key] ? 'default' : 'outline'}
          size={size === 'lg' ? 'default' : 'sm'}
          onClick={handleClick(key)}
          aria-pressed={marks[key]}
          title={label}
          type="button"
          className={marks[key] ? '' : 'bg-background'}
        >
          {icon}
          {size === 'lg' && <span>{label}</span>}
        </Button>
      ))}
    </div>
  )
}
