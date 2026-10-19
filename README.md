# Trend Radar (Week 1)

Free stack: Next.js on Vercel, Neon Postgres, GitHub Actions for the 10-minute fetch.

## Setup
1. Create a free project at neon.tech. Open its SQL editor and run `schema.sql`.
2. Copy `.env.example` to `.env.local`. Paste your Neon connection string as `DATABASE_URL`. Set `CRON_SECRET` to a long random string.
3. `npm install` then `npm run dev`. Open http://localhost:3000.
4. Load first data: `curl -H "Authorization: Bearer YOUR_CRON_SECRET" http://localhost:3000/api/cron/fetch`. Run it twice, a few minutes apart, so growth can be measured. Reload the page.

## Deploy
1. Push this folder to a GitHub repo.
2. Import the repo at vercel.com. Add `DATABASE_URL` and `CRON_SECRET` as environment variables. Deploy.
3. In the GitHub repo, add secrets `CRON_SECRET` (same value) and `APP_URL` (your Vercel URL, no trailing slash).
4. Open the Actions tab, run "Fetch trends" once by hand to confirm it works.
