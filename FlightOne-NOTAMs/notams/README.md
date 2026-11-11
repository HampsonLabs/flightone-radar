# FlightOne NOTAMs (Hampson Labs)

Keeps a copy of active NOTAMs from the FAA NOTAM Management Service (NMS-API) in R2 and serves them to the app.
The app never calls the FAA. FAA limits (NMS-API FAQ): one change pull every 3 minutes, one full load a day.

| Part | Where | When |
|---|---|---|
| `worker.js` | Cloudflare Worker `flightone-notams` | every 3 minutes: changes since the last pull; serves `/v1/notams` and `/health` |
| `notam_full.py` | GitHub Actions (repo HampsonLabs/flightone-radar, folder `notams/`) | once a day 04:17 UTC: full load |

Files in R2 bucket `flightone-charts`: `notams/loc/<ID>.json`, `notams/state-full.json`, `notams/state-delta.json`.
No credential or token is ever stored in R2.

## Set up (pre-production)
1. GitHub → HampsonLabs/flightone-radar → upload this folder as `notams/`, and `notams-full.yml` into `.github/workflows/`.
2. GitHub → Settings → Secrets and variables → Actions → New repository secret:
   `NMS_CLIENT_ID` (the KEY) and `NMS_CLIENT_SECRET` (the SECRET). The R2 secrets are already there.
3. Terminal, in this folder:
   ```
   npx wrangler deploy
   npx wrangler secret put NMS_CLIENT_ID
   npx wrangler secret put NMS_CLIENT_SECRET
   ```
4. GitHub → Actions → "NOTAMs full load" → Run workflow. When green, open
   `https://flightone-notams.tomwhampson.workers.dev/health` (should say `"ok": true` within 6 minutes) and
   `https://flightone-notams.tomwhampson.workers.dev/v1/notams?locations=KJFK,KATL`.

## Going to production
After the FAA issues production credentials (reply on ticket INC0036013):
- `wrangler.toml`: `NMS_HOST = "https://api-nms.aim.faa.gov"`, then `npx wrangler deploy` and put the two new secrets.
- GitHub → Settings → Secrets and variables → Actions → Variables: `NMS_HOST` = `https://api-nms.aim.faa.gov`,
  and replace the two secrets.

## Terms (FAA Terms of Service, NMS-API, 7 Oct 2026)
Redistribution is allowed. It must not be described as "FAA data"; the API says
"NOTAM Management Service (NMS) data, processed by Hampson Labs" and "Not an official briefing".
The FAA doesn't receive every State's NOTAMs (for example UK N-series): coverage is stated in every response.

## Tests
`python3 tests/test_records.py` and `node tests/test_worker.mjs` (offline, fake FAA and fake R2).
