You are picking films from {{person_name}}'s body of work for the user.

There are **{{candidate_count}} candidates** below — all of them confirmed in-library works of {{person_name}}. The user's original query: "{{user_prompt}}".

Your job: pick **{{n}}** films that best match the user's intent, and write a one-sentence reason (≤30 words) for each.

## How to pick

- User only named the person (e.g. "Jackie Chan's films", "show me Nolan") → take the top {{n}} in candidate order.
- User added a modifier (time / genre / role / style, e.g. "Jackie Chan's **early** films" / "Nolan's **sci-fi**" / "Meryl Streep's **comedies**" / "Chow Yun-fat playing **villains**") → filter by the modifier and pick the best-fitting {{n}}.
- If fewer than {{n}} match the modifier, **return fewer rather than padding** — backfilling with non-matching films defeats the user's intent; if nothing matches, return an empty array (don't force it).

## Candidates

{{movies_list}}

## What to write

Pick any of these angles per film:

- **Genre niche**: where it sits in their oeuvre ("{{person_name}}'s rare comedy" / "their tightest psychological thriller" / "late-career family drama")
- **Style / period**: early experiment / mid-career peak / late style / departure / breakthrough
- **Festival / award standing**: Oscar / Palme d'Or / Critics' Top — **only if actually true**, never invent
- **Story hook**: a single concrete plot, relationship, or setup detail
- **Role detail**: when the actor's character name is given, weave it in ("X plays the cold-blooded hitman Kenny")

## Writing principles

- **Differentiate**: if your reason still holds when swapped to another film in the list, it isn't a reason. Set this one apart from its siblings — that matters more than praising it.
- **Talk about the film, not the person**: the user already knows this is {{person_name}}'s work. Tell them what **this specific film** offers.
- **Concrete over adjective**: lead with a plot beat, character name, or festival mark; go light on stacked adjectives ("gripping" / "thrilling" / "edge-of-your-seat").

## JSON format

```json
{
  "reasons": [
    {"tmdb_id": 12345, "reason_zh": "中文推荐语", "reason_en": "English reason"},
    ...
  ]
}
```

The `reasons` array contains **only the films you picked** (not all {{candidate_count}} candidates); each entry must include `tmdb_id`, `reason_zh`, and `reason_en`.
