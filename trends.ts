import { db } from "./db";
import { velocityScore } from "./score";

export type Trend = {
  source: string; country: string; title: string; url: string | null;
  volume: number; isNew: boolean; ageMinutes: number; score: number;
};

export async function getTrends(country: string | null): Promise<Trend[]> {
  const sql = db();
  const rows = await sql`
    with t as (
      select source, country, title, volume, url, fetched_at,
             lag(volume) over w as prev_volume,
             row_number() over (partition by source, country, title order by fetched_at desc) as rn,
             min(fetched_at) over (partition by source, country, title) as first_seen
      from snapshots
      where fetched_at > now() - interval '24 hours'
      window w as (partition by source, country, title order by fetched_at)
    )
    select source, country, title, volume, url, prev_volume,
           extract(epoch from (now() - first_seen)) / 60 as age_minutes
    from t
    where rn = 1
      and fetched_at > now() - interval '30 minutes'
      and (${country}::text is null or country = ${country})
  `;
  return rows
    .map((r) => {
      const prev = r.prev_volume === null ? null : Number(r.prev_volume);
      const age = Number(r.age_minutes);
      return {
        source: r.source, country: r.country, title: r.title, url: r.url,
        volume: Number(r.volume), isNew: prev === null, ageMinutes: Math.round(age),
        score: velocityScore(Number(r.volume), prev, age),
      } as Trend;
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, 50);
}
