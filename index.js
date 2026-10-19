// Trend Radar: the whole app in one file (login, tabs, trend fetcher).
const crypto = require("crypto");
const express = require("express");
const { neon } = require("@neondatabase/serverless");
const { XMLParser } = require("fast-xml-parser");

const app = express();
app.use(express.urlencoded({ extended: false }));

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
  const res = await fetch(`https://trends.google.com/trending/rss?geo=${geo}`, { headers: { "User-Agent": "Mozilla/5.0 TrendRadar" } });
  if (!res.ok) return [];
  const xml = new XMLParser().parse(await res.text());
  return [].concat(xml?.rss?.channel?.item ?? []).map((i) => ({
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

// ---------- Scoring and queries ----------
function velocityScore(current, previous, ageMinutes) {
  const growth = previous === null ? 1 : (current - previous) / Math.max(previous, 1);
  const size = Math.log10(current + 1);
  const freshness = 1 / (1 + ageMinutes / 60);
  return Math.round((Math.max(growth, 0) * 50 + size * 10) * freshness * 100) / 100;
}
async function getAll() {
  const rows = await db()`
    with t as (
      select source, country, title, volume, url, fetched_at,
             lag(volume) over w as prev_volume,
             row_number() over (partition by source, country, title order by fetched_at desc) as rn,
             min(fetched_at) over (partition by source, country, title) as first_seen
      from snapshots where fetched_at > now() - interval '24 hours'
      window w as (partition by source, country, title order by fetched_at)
    )
    select source, country, title, volume, url, prev_volume,
           extract(epoch from (now() - first_seen)) / 60 as age_minutes
    from t where rn = 1 and fetched_at > now() - interval '30 minutes'`;
  return rows.map((r) => {
    const prev = r.prev_volume === null ? null : Number(r.prev_volume);
    const age = Number(r.age_minutes);
    return {
      source: r.source, country: r.country, title: r.title, url: r.url, volume: Number(r.volume),
      isNew: prev === null, ageMinutes: Math.round(age), score: velocityScore(Number(r.volume), prev, age),
    };
  }).sort((a, b) => b.score - a.score);
}
async function minutesSinceUpdate() {
  const rows = await db()`select extract(epoch from (now() - max(fetched_at))) / 60 as m from snapshots`;
  return rows[0] && rows[0].m !== null ? Math.round(Number(rows[0].m)) : null;
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

// ---------- Login ----------
const PASS = () => process.env.APP_PASSWORD || "";
const sessionToken = () =>
  crypto.createHmac("sha256", process.env.SESSION_SECRET || process.env.CRON_SECRET || "trend-radar").update("session:" + PASS()).digest("hex");
const safeEq = (a, b) => {
  const A = Buffer.from(String(a)), B = Buffer.from(String(b));
  return A.length === B.length && crypto.timingSafeEqual(A, B);
};
const readCookie = (req, name) => {
  const hit = (req.headers.cookie || "").split(/;\s*/).find((c) => c.startsWith(name + "="));
  return hit ? decodeURIComponent(hit.slice(name.length + 1)) : "";
};
const authed = (req) => PASS() !== "" && safeEq(readCookie(req, "tr_session"), sessionToken());

// ---------- Look and feel (matches the Zack portal) ----------
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const CSS = `:root{--bg:#fff;--surface:#f5f7fa;--border:#d1d9e0;--accent:#0d7a55;--accent2:#e63559;--text:#1a202c;--muted:#6b7a8d;--success:#16a34a;--warning:#d97706;--danger:#dc2626}
@media(prefers-color-scheme:dark){:root{--bg:#0f1512;--surface:#16201b;--border:#27332d;--accent:#2fb386;--text:#e8efea;--muted:#93a39a}}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Space Mono',monospace;background:var(--bg);color:var(--text);min-height:100vh;overflow-x:hidden}
.bg{font-family:'Bricolage Grotesque',sans-serif}
header{display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--bg);z-index:5;flex-wrap:wrap;gap:10px}
.brand{display:flex;align-items:center;gap:10px}.brand-icon{width:30px;height:30px;background:var(--accent);border-radius:3px;display:flex;align-items:center;justify-content:center;color:#fff}
.brand-text{font-family:'Bricolage Grotesque',sans-serif;font-weight:800;font-size:16px}.brand-text span{color:var(--accent)}
.brand-sub{font-size:9px;letter-spacing:.15em;color:var(--muted);text-transform:uppercase}
.hr{display:flex;align-items:center;gap:12px}.sync{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--muted)}
.dot{width:7px;height:7px;border-radius:50%;background:var(--success)}.dot.warn{background:var(--warning)}.dot.err{background:var(--danger)}
.btn-out{padding:7px 12px;border:1px solid var(--border);color:var(--muted);font:11px 'Space Mono',monospace;border-radius:4px;text-decoration:none}
.btn-out:hover{border-color:var(--danger);color:var(--danger)}
.tabs{display:flex;border-bottom:2px solid var(--border);padding:0 8px;overflow-x:auto}
.nav-tab{padding:11px 16px;background:none;border:none;border-bottom:3px solid transparent;margin-bottom:-2px;font:12px 'Space Mono',monospace;color:var(--muted);cursor:pointer;white-space:nowrap}
.nav-tab.active{color:var(--accent);border-bottom-color:var(--accent);font-weight:700}
main{padding:20px 16px 64px;max-width:900px;margin:0 auto}.pane{display:none}.pane.active{display:block}
.stats{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin-bottom:22px}
@media(min-width:700px){.stats{grid-template-columns:repeat(4,1fr)}}
.stat{padding:14px 16px;background:var(--surface);border:1px solid var(--border);border-radius:6px;position:relative;overflow:hidden;min-width:0}
.stat::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:var(--accent)}
.stat-l{font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:var(--muted)}
.stat-v{font-family:'Bricolage Grotesque',sans-serif;font-size:22px;font-weight:800;margin-top:4px;overflow-wrap:anywhere}.stat-v.sm{font-size:13px;line-height:1.3}
.chips{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:14px}
.chips a{padding:4px 12px;border:1px solid var(--border);border-radius:999px;color:var(--muted);text-decoration:none;font-size:12px}
.chips a.on{background:var(--accent);border-color:var(--accent);color:#fff}
ol{list-style:none}li.row{display:grid;grid-template-columns:28px 1fr auto;gap:10px;padding:12px 0;border-top:1px solid var(--border)}
.rank{color:var(--muted)}.title{color:var(--text);font-weight:700;text-decoration:none;overflow-wrap:anywhere}.title:hover{color:var(--accent)}
.meta{color:var(--muted);font-size:11px}.score{font-weight:700;color:var(--accent)}.empty{color:var(--muted);padding:24px 0;font-size:13px}
.grid{display:grid;gap:12px}@media(min-width:700px){.grid{grid-template-columns:repeat(2,1fr)}}
.card{padding:14px 16px;border:1px solid var(--border);border-radius:6px;background:var(--surface)}
.card h3{font-family:'Bricolage Grotesque',sans-serif;font-size:16px;margin-bottom:8px;display:flex;justify-content:space-between}.card h3 small{font:11px 'Space Mono',monospace;color:var(--muted)}
.card a{display:block;color:var(--text);text-decoration:none;font-size:12px;padding:4px 0;overflow-wrap:anywhere}.card a:hover{color:var(--accent)}
h2{font-family:'Bricolage Grotesque',sans-serif;font-size:18px;margin:18px 0 8px}h2:first-child{margin-top:0}
label.chk{display:flex;gap:10px;align-items:flex-start;padding:10px 0;border-top:1px solid var(--border);font-size:13px;cursor:pointer}
label.chk input{margin-top:3px;accent-color:var(--accent);width:18px;height:18px;flex-shrink:0}
.prog{font-size:12px;color:var(--accent);font-weight:700;margin-bottom:6px}
a:focus-visible,button:focus-visible,input:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.login{min-height:100vh;display:flex;align-items:center;justify-content:center;padding:16px}
.login-box{position:relative;width:100%;max-width:420px;padding:44px 32px;border:1px solid var(--border);background:var(--surface);box-shadow:0 4px 24px rgba(0,0,0,.08)}
.login-box::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,var(--accent),var(--accent2))}
.login-box h1{font-family:'Bricolage Grotesque',sans-serif;font-size:28px;font-weight:800;margin:24px 0 6px}.login-box p{font-size:11px;color:var(--muted);margin-bottom:28px}
.fl{display:block;font-size:10px;letter-spacing:.15em;text-transform:uppercase;color:var(--muted);margin-bottom:8px}
.fi{width:100%;padding:14px 16px;background:var(--bg);border:1px solid var(--border);color:var(--text);font:14px 'Space Mono',monospace;border-radius:4px;margin-bottom:18px}
.btn{width:100%;padding:16px;background:var(--accent);color:#fff;border:none;font:700 13px 'Bricolage Grotesque',sans-serif;letter-spacing:.15em;text-transform:uppercase;cursor:pointer;border-radius:4px}
.err{margin-top:14px;font-size:12px;color:var(--danger);text-align:center;min-height:18px}`;

const shell = (inner) => `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="theme-color" content="#0d7a55"><title>Trend Radar</title>
<link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:wght@400;700;800&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
<style>${CSS}</style></head><body>${inner}</body></html>`;

const loginPage = (msg) => shell(`<div class="login"><form class="login-box" method="post" action="/login">
<div class="brand"><div class="brand-icon">⚡</div><div><div class="brand-text">Trend<span>Radar</span></div><div class="brand-sub">Worldwide trend desk</div></div></div>
<h1>Sign in</h1><p>Enter your password to open the dashboard.</p>
<label class="fl" for="pw">Password</label><input class="fi" id="pw" name="password" type="password" autocomplete="current-password" required autofocus>
<button class="btn" type="submit">Enter</button><div class="err">${esc(msg || "")}</div></form></div>`);

app.post("/login", async (req, res) => {
  if (PASS() && safeEq(req.body.password || "", PASS())) {
    res.cookie("tr_session", sessionToken(), { httpOnly: true, secure: !!process.env.VERCEL, sameSite: "lax", maxAge: 30 * 864e5 });
    return res.redirect("/");
  }
  await new Promise((r) => setTimeout(r, 600));
  res.status(401).type("html").send(loginPage(PASS() ? "Wrong password." : "APP_PASSWORD is not set in Vercel yet."));
});
app.get("/logout", (req, res) => { res.clearCookie("tr_session"); res.redirect("/"); });

// ---------- Dashboard ----------
const row = (t, i) => `<li class="row"><span class="rank">${i + 1}</span><div>
<a class="title" href="${esc(t.url || "#")}" target="_blank" rel="noreferrer">${esc(t.title)}</a>
<div class="meta">${esc(t.country)} · ${esc(t.source)} · ${t.volume.toLocaleString("en-US")} ${t.source === "hackernews" ? "points" : "searches"}${t.isNew ? " · new" : ""} · seen ${t.ageMinutes} min ago</div></div>
<span class="score">${t.score}</span></li>`;

const PLAN = [
  ["Morning", "Check the top 5 trends in your lane and post 1 to 2 takes with your own angle"],
  ["Morning", "Reply to 3 fresh posts (under 60 minutes old) from accounts with 10K to 500K followers"],
  ["Midday", "Reply with a real fact or angle to 5 more fresh posts, never just “great post”"],
  ["Midday", "Reply to at least 2 mid-size accounts (5K to 50K followers) in your lane"],
  ["Evening", "Post once more on a rising trend"],
  ["Evening", "Answer every reply you received today"],
];

app.get("/", async (req, res) => {
  res.set("Cache-Control", "no-store");
  if (!authed(req)) return res.type("html").send(loginPage(PASS() ? "" : "APP_PASSWORD is not set in Vercel yet."));
  const names = [...COUNTRIES, "GLOBAL"];
  const selected = names.includes(req.query.country) ? req.query.country : null;
  let all = [], mins = null, error = "";
  try { [all, mins] = await Promise.all([getAll(), minutesSinceUpdate()]); }
  catch (e) { error = String(e.message || e); }

  const list = (selected ? all.filter((t) => t.country === selected) : all).slice(0, 50);
  const dot = mins === null ? "err" : mins <= 25 ? "" : mins <= 60 ? "warn" : "err";
  const top = all[0];
  const trendsPane = error
    ? `<p class="empty">Could not load trends: ${esc(error)}. Check DATABASE_URL.</p>`
    : list.length === 0 ? `<p class="empty">No trends yet. Run the fetch link once, then reload.</p>` : `<ol>${list.map(row).join("")}</ol>`;
  const countryPane = names.map((c) => {
    const items = all.filter((t) => t.country === c);
    return `<div class="card"><h3>${esc(c)} <small>${items.length} trending</small></h3>${
      items.slice(0, 3).map((t) => `<a href="${esc(t.url || "#")}" target="_blank" rel="noreferrer">${esc(t.title)}</a>`).join("") || '<span class="meta">No data yet</span>'}</div>`;
  }).join("");

  res.type("html").send(shell(`<header>
<div class="brand"><div class="brand-icon">⚡</div><div><div class="brand-text">Trend<span>Radar</span></div><div class="brand-sub">Worldwide trend desk</div></div></div>
<div class="hr"><div class="sync"><span class="dot ${dot}"></span>${mins === null ? "No data" : mins <= 1 ? "Just now" : mins + " min ago"}</div><a class="btn-out" href="/logout">LOG OUT</a></div></header>
<div class="tabs"><button class="nav-tab" data-tab="trends">Trends</button><button class="nav-tab" data-tab="countries">By Country</button><button class="nav-tab" data-tab="plan">Daily Plan</button></div>
<main>
<div class="stats">
<div class="stat"><div class="stat-l">Trends now</div><div class="stat-v">${all.length}</div></div>
<div class="stat"><div class="stat-l">Countries live</div><div class="stat-v">${new Set(all.map((t) => t.country)).size}</div></div>
<div class="stat"><div class="stat-l">Fastest riser</div><div class="stat-v sm">${top ? esc(top.title) : "None yet"}</div></div>
<div class="stat"><div class="stat-l">Last update</div><div class="stat-v">${mins === null ? "-" : mins + "m"}</div></div></div>
<section class="pane" id="p-trends"><div class="chips"><a href="/" class="${selected ? "" : "on"}">All</a>${names.map((c) => `<a href="/?country=${c}" class="${selected === c ? "on" : ""}">${c}</a>`).join("")}</div>${trendsPane}</section>
<section class="pane" id="p-countries"><div class="grid">${countryPane}</div></section>
<section class="pane" id="p-plan"><h2>Today’s routine</h2><div class="prog" id="prog"></div>
${PLAN.map((p, i) => `<label class="chk"><input type="checkbox" data-i="${i}"><span><b>${p[0]}:</b> ${esc(p[1])}</span></label>`).join("")}
<h2>Rules</h2><p class="meta" style="line-height:1.7">Reply first, post second. Answer every reply within the first hour. Never buy followers or use follow/unfollow. Never automate replies or likes: you approve every post yourself.</p></section>
</main>
<script>
var T=["trends","countries","plan"];
function show(id){if(T.indexOf(id)<0)id="trends";T.forEach(function(t){document.getElementById("p-"+t).classList.toggle("active",t===id);document.querySelector('[data-tab="'+t+'"]').classList.toggle("active",t===id)});try{history.replaceState(null,"","#"+id)}catch(e){}}
document.querySelectorAll(".nav-tab").forEach(function(b){b.onclick=function(){show(b.dataset.tab)}});
show(location.hash.slice(1));
var key="tr-plan-"+new Date().toISOString().slice(0,10),state={};
try{state=JSON.parse(localStorage.getItem(key)||"{}")}catch(e){}
var boxes=document.querySelectorAll('input[data-i]');
function prog(){var n=0;boxes.forEach(function(b){if(b.checked)n++});document.getElementById("prog").textContent=n+" of "+boxes.length+" done today"}
boxes.forEach(function(b){b.checked=!!state[b.dataset.i];b.onchange=function(){state[b.dataset.i]=b.checked;try{localStorage.setItem(key,JSON.stringify(state))}catch(e){}prog()}});prog();
</script>`));
});

module.exports = app;
