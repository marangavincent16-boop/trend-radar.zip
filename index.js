// Trend Radar: the whole app in one file.
const express = require("express");
const { neon } = require("@neondatabase/serverless");
const { XMLParser } = require("fast-xml-parser");

const app = express();

// Edit this list to change which countries you track.
const COUNTRIES = ["US", "GB", "IN", "NG", "BR", "JP", "KE"];

const db = () => neon(process.env.DATABASE_URL);

// ---------- Fetching trends ----------
function toNumber(s) {
  const m = String(s ?? "").replace(/[+,\s]/g, "").match(/^(\d+(?:\.\d+)?)([KkMm]?)$/);
  if (!m) return 0;
  const unit = m[2].toLowerCase();
  return Math.round(parseFloat(m[1]) * (unit === "m" ? 1e6 : unit === "k" ? 1e3 : 1));
}

async function googleTrends(geo) {
  const res = await fetch(`https://trends.google.com/trending/rss?geo=${geo}`, {
    headers: { "User-Agent": "Mozilla/5.0 TrendRadar" },
  });
  if (!res.ok) return [];
  const xml = new XMLParser().parse(await res.text());
  const items = [].concat(xml?.rss?.channel?.item ?? []);
  return items.map((i) => ({
    source: "google", country: geo, title: String(i.title),
    volume: toNumber(i["ht:approx_traffic"]), url: i.link ? String(i.link) : null,
  }));
}

async function hackerNews() {
  const res = await fetch("https://hn.algolia.com/api/v1/search?tags=front_page&hitsPerPage=30");
  if (!res.ok) return [];
  const data = await res.json();
  return data.hits.map((h) => ({
    source: "hackernews", country: "GLOBAL", title: h.title, volume: h.points ?? 0,
    url: h.url ?? `https://news.ycombinator.com/item?id=${h.objectID}`,
  }));
}

async function fetchAll() {
  const results = await Promise.allSettled([...COUNTRIES.map(googleTrends), hackerNews()]);
  return results.flatMap((r) => (r.status === "fulfilled" ? r.value : []));
}

// ---------- Scoring ----------
function velocityScore(current, previous, ageMinutes) {
  const growth = previous === null ? 1 : (current - previous) / Math.max(previous, 1);
  const size = Math.log10(current + 1);
  const freshness = 1 / (1 + ageMinutes / 60);
  return Math.round((Math.max(growth, 0) * 50 + size * 10) * freshness * 100) / 100;
}

async function getTrends(country) {
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
    where rn = 1 and fetched_at > now() - interval '30 minutes'
      and (${country}::text is null or country = ${country})`;
  return rows
    .map((r) => {
      const prev = r.prev_volume === null ? null : Number(r.prev_volume);
      const age = Number(r.age_minutes);
      return {
        source: r.source, country: r.country, title: r.title, url: r.url,
        volume: Number(r.volume), isNew: prev === null, ageMinutes: Math.round(age),
        score: velocityScore(Number(r.volume), prev, age),
      };
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, 50);
}

// ---------- Fetch endpoint (called every 10 minutes by a scheduler) ----------
app.get("/api/cron/fetch", async (req, res) => {
  const secret = process.env.CRON_SECRET;
  const given = (req.get("authorization") || "").replace("Bearer ", "") || req.query.key;
  if (!secret || given !== secret) return res.status(401).send("Unauthorized");
  try {
    const items = await fetchAll();
    if (!items.length) return res.json({ inserted: 0 });
    const sql = db();
    await sql`
      insert into snapshots (source, country, title, volume, url)
      select * from unnest(
        ${items.map((i) => i.source)}::text[], ${items.map((i) => i.country)}::text[],
        ${items.map((i) => i.title)}::text[], ${items.map((i) => i.volume)}::int[],
        ${items.map((i) => i.url)}::text[])`;
    await sql`delete from snapshots where fetched_at < now() - interval '3 days'`;
    res.json({ inserted: items.length });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

// ---------- Dashboard ----------
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const CSS = `:root{--bg:#f6f7f4;--ink:#14201c;--mute:#5d6b65;--line:#d9dfd8;--accent:#0a7d5a}
@media(prefers-color-scheme:dark){:root{--bg:#0f1512;--ink:#e8efea;--mute:#93a39a;--line:#27332d;--accent:#3ddba0}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.5 system-ui,sans-serif}
main{max-width:720px;margin:0 auto;padding:32px 16px 64px}h1{margin:0;font-size:1.75rem;letter-spacing:-.02em}
.sub{margin:4px 0 20px;color:var(--mute)}nav{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px}
nav a{padding:4px 12px;border:1px solid var(--line);border-radius:999px;color:var(--mute);text-decoration:none;font-size:.875rem}
nav a.on{background:var(--accent);border-color:var(--accent);color:var(--bg)}
ol{list-style:none;margin:0;padding:0}li{display:grid;grid-template-columns:32px 1fr auto;gap:12px;padding:12px 0;border-top:1px solid var(--line)}
.rank{color:var(--mute)}.title{color:var(--ink);font-weight:600;text-decoration:none;overflow-wrap:anywhere}
.title:hover{color:var(--accent)}.meta{color:var(--mute);font-size:.8125rem}.score{font-weight:700;color:var(--accent)}
.empty{color:var(--mute);padding:24px 0}a:focus-visible{outline:2px solid var(--accent);outline-offset:2px}`;

app.get("/", async (req, res) => {
  const all = [...COUNTRIES, "GLOBAL"];
  const selected = all.includes(req.query.country) ? req.query.country : null;
  let body;
  try {
    const trends = await getTrends(selected);
    body = trends.length === 0
      ? `<p class="empty">No trends yet. Run the fetch link once, then reload.</p>`
      : `<ol>${trends.map((t, i) => `<li><span class="rank">${i + 1}</span><div>
<a class="title" href="${esc(t.url || "#")}" target="_blank" rel="noreferrer">${esc(t.title)}</a>
<div class="meta">${esc(t.country)} · ${esc(t.source)} · ${t.volume.toLocaleString("en-US")} ${t.source === "hackernews" ? "points" : "searches"}${t.isNew ? " · new" : ""} · seen ${t.ageMinutes} min ago</div></div>
<span class="score">${t.score}</span></li>`).join("")}</ol>`;
  } catch (e) {
    body = `<p class="empty">Could not load trends: ${esc(e.message || e)}. Check DATABASE_URL.</p>`;
  }
  res.set("Cache-Control", "no-store").type("html").send(`<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Trend Radar</title><style>${CSS}</style></head><body><main>
<h1>Trend Radar</h1><p class="sub">Ranked by how fast each topic is rising. Refreshes about every 10 minutes.</p>
<nav><a href="/" class="${selected ? "" : "on"}">All</a>${all.map((c) => `<a href="/?country=${c}" class="${selected === c ? "on" : ""}">${c}</a>`).join("")}</nav>
${body}</main></body></html>`);
});

module.exports = app;
