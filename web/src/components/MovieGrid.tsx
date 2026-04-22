import MovieCard from './MovieCard'
import type { Movie } from '../types'

interface MovieGridProps {
  movies: Movie[]
  marks?: Record<number, { want: boolean; watched: boolean; favorite: boolean }>
  onToggleMark?: (movieId: number, markType: 'want' | 'watched' | 'favorite') => void
}

export function MovieGrid({ movies, marks, onToggleMark }: MovieGridProps) {
  return (
    <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6">
      {movies.map((movie) => (
        <MovieCard
          key={movie.id}
          movie={movie}
          marks={marks?.[movie.id]}
          onToggleMark={onToggleMark ? (mt) => onToggleMark(movie.id, mt) : undefined}
        />
      ))}
    </div>
  )
}

export default MovieGrid
