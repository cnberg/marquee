import type { ReactNode } from 'react'
import { Link } from 'react-router-dom'

interface Entity {
  name: string
  link: string
}

interface LinkedReasonProps {
  text: string
  entities: Entity[]
}

/**
 * Renders a reason string with auto-linked entity names (movies, directors).
 * Matches longest names first to avoid partial matches.
 * Links are subtle — same color as surrounding text, just underlined on hover.
 */
export function LinkedReason({ text, entities }: LinkedReasonProps) {
  if (entities.length === 0) return <>{text}</>

  // Sort by name length descending to match longest first
  const sorted = [...entities].sort((a, b) => b.name.length - a.name.length)

  // Build segments: split text by entity names
  const segments: ReactNode[] = []
  let remaining = text
  let key = 0

  while (remaining.length > 0) {
    // Find the earliest match among all entities
    let bestIdx = remaining.length
    let bestEntity: Entity | null = null
    let bestMatchLen = 0

    for (const entity of sorted) {
      if (entity.name.length < 2) continue // skip single-char names
      // Match with or without book title marks 《》
      const variants = [entity.name, `《${entity.name}》`]
      for (const variant of variants) {
        const idx = remaining.indexOf(variant)
        if (idx !== -1 && idx < bestIdx) {
          bestIdx = idx
          bestEntity = entity
          bestMatchLen = variant.length
        }
      }
    }

    if (!bestEntity) {
      // No more matches
      segments.push(remaining)
      break
    }

    // Add text before match
    if (bestIdx > 0) {
      segments.push(remaining.slice(0, bestIdx))
    }

    // Add linked match
    const matchedText = remaining.slice(bestIdx, bestIdx + bestMatchLen)
    segments.push(
      <Link
        key={key++}
        to={bestEntity.link}
        className="underline decoration-muted-foreground/40 underline-offset-2 transition hover:decoration-foreground/60"
      >
        {matchedText}
      </Link>
    )

    remaining = remaining.slice(bestIdx + bestMatchLen)
  }

  return <>{segments}</>
}
