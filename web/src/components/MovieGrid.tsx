import type { ReactNode } from 'react'
import { MovieCard } from './MovieCard'
import type { Movie } from '../types'

export interface MovieGridItem {
  movie: Movie
  reason?: ReactNode
  in_library?: boolean
  downloading?: boolean
}

interface MovieGridProps {
  items: MovieGridItem[]
  marks?: Record<number, { want: boolean; watched: boolean; favorite: boolean }>
  onToggleMark?: (movieId: number, markType: 'want' | 'watched' | 'favorite') => void
  /**
   * 装饰位用：当本组项数为奇数时，移动端隐藏最末一张卡，避免末行只剩 1 个。
   * 仅适用于服务端固定项数、用户不能翻页加载的"短列展示位"。
   * 用户的完整数据（翻页结果 / 标记 / 智能推荐）一律不开启。
   */
  mobileTrim?: boolean
}

export function MovieGrid({ items, marks, onToggleMark, mobileTrim = false }: MovieGridProps) {
  return (
    <div
      className={
        'grid grid-cols-2 gap-4 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5' +
        (mobileTrim ? ' mobile-trim-odd-tail' : '')
      }
    >
      {items.map((item) => (
        <MovieCard
          key={item.movie.id}
          movie={item.movie}
          marks={marks?.[item.movie.id]}
          onToggleMark={onToggleMark ? (mt) => onToggleMark(item.movie.id, mt) : undefined}
          outOfLibrary={item.in_library === false}
          downloading={item.downloading === true}
          reason={item.reason}
        />
      ))}
    </div>
  )
}

export default MovieGrid
