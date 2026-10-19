import { XMLParser } from "fast-xml-parser";

export type Item = { source: string; country: string; title: string; volume: number; url: string | null };

// Edit this list to change which countries you track.
export const COUNTRIES = ["US", "GB", "IN", "NG", "BR", "JP"];

function toNumber(s: unknown): number {
  const m = String(s ?? "").replace(/[+,\s]/g, "").match(/^(\d+(?:\.\d+)?)([KkMm]?)$/);
  if (!m) return 0;
  const unit = m[2].toLowerCase();
  return Math.round(parseFloat(m[1]) * (unit === "m" ? 1e6 : unit === "k" ? 1e3 : 1));
}

async function googleTrends(geo: string): Promise<Item[]> {
  const res = await fetch(`https://trends.google.com/trending/rss?geo=${geo}`, {
    headers: { "User-Agent": "Mozilla/5.0 TrendRadar" },
  });
  if (!res.ok) return [];
  const xml = new XMLParser().parse(await res.text());
  const items = ([] as Record<string, unknown>[]).concat(xml?.rss?.channel?.item ?? []);
  return items.map((i) => ({
    source: "google",
    country: geo,
    title: String(i.title),
    volume: toNumber(i["ht:approx_traffic"]),
    url: i.link ? String(i.link) : null,
  }));
}

async function hackerNews(): Promise<Item[]> {
  const res = await fetch("https://hn.algolia.com/api/v1/search?tags=front_page&hitsPerPage=30");
  if (!res.ok) return [];
  const data = await res.json();
  return data.hits.map((h: { title: string; points: number; url?: string; objectID: string }) => ({
    source: "hackernews",
    country: "GLOBAL",
    title: h.title,
    volume: h.points ?? 0,
    url: h.url ?? `https://news.ycombinator.com/item?id=${h.objectID}`,
  }));
}

export async function fetchAll(): Promise<Item[]> {
  const results = await Promise.allSettled([...COUNTRIES.map(googleTrends), hackerNews()]);
  return results.flatMap((r) => (r.status === "fulfilled" ? r.value : []));
}
