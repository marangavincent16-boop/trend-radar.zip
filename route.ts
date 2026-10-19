import { db } from "@/lib/db";
import { fetchAll } from "@/lib/fetchers";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  if (req.headers.get("authorization") !== `Bearer ${process.env.CRON_SECRET}`) {
    return new Response("Unauthorized", { status: 401 });
  }
  const items = await fetchAll();
  if (!items.length) return Response.json({ inserted: 0 });
  const sql = db();
  await sql`
    insert into snapshots (source, country, title, volume, url)
    select * from unnest(
      ${items.map((i) => i.source)}::text[],
      ${items.map((i) => i.country)}::text[],
      ${items.map((i) => i.title)}::text[],
      ${items.map((i) => i.volume)}::int[],
      ${items.map((i) => i.url)}::text[]
    )`;
  await sql`delete from snapshots where fetched_at < now() - interval '3 days'`;
  return Response.json({ inserted: items.length });
}
