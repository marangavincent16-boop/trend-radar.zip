import { getTrends } from "@/lib/trends";
import { COUNTRIES } from "@/lib/fetchers";

export const dynamic = "force-dynamic";

export default async function Page({ searchParams }: { searchParams: Promise<{ country?: string }> }) {
  const { country } = await searchParams;
  const selected = country && [...COUNTRIES, "GLOBAL"].includes(country) ? country : null;
  const trends = await getTrends(selected);

  return (
    <main>
      <h1>Trend Radar</h1>
      <p className="sub">Ranked by how fast each topic is rising. Refreshes about every 10 minutes.</p>
      <nav>
        <a href="/" className={!selected ? "on" : ""}>All</a>
        {[...COUNTRIES, "GLOBAL"].map((c) => (
          <a key={c} href={`/?country=${c}`} className={selected === c ? "on" : ""}>{c}</a>
        ))}
      </nav>
      {trends.length === 0 ? (
        <p className="empty">No trends yet. Run the fetch job once (see README), then reload.</p>
      ) : (
        <ol>
          {trends.map((t, i) => (
            <li key={`${t.source}-${t.country}-${t.title}`}>
              <span className="rank">{i + 1}</span>
              <div>
                <a href={t.url ?? "#"} target="_blank" rel="noreferrer" className="title">{t.title}</a>
                <div className="meta">
                  {t.country} · {t.source} · {t.volume.toLocaleString()} {t.source === "hackernews" ? "points" : "searches"}
                  {t.isNew ? " · new" : ""} · seen {t.ageMinutes} min ago
                </div>
              </div>
              <span className="score">{t.score}</span>
            </li>
          ))}
        </ol>
      )}
    </main>
  );
}
