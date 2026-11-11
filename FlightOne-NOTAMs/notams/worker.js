// FlightOne NOTAM server (Hampson Labs).
//
// Keeps a copy of active NOTAMs from the FAA NOTAM Management Service (NMS-API) in R2, one small JSON file per
// location, and serves them to the app. The FAA is never called by the app: their limits are one change pull every
// 3 minutes and one full load a day (NMS-API FAQ), so this server does exactly that and nothing more.
//
//   Every 3 minutes (cron)  changes since the last pull (new, replaced, cancelled) are merged into the files.
//   Once a day              the full load is rebuilt by notam_full.py on GitHub Actions (same files).
//
//   GET /v1/notams?locations=KJFK,KZNY,EGLL   active and upcoming NOTAMs for up to 25 locations (ICAO or FAA ids)
//   GET /health                               age of the last change pull and of the last full load
//
// R2 (bucket flightone-charts, prefix notams/):
//   notams/loc/<ID>.json        {"location":"KJFK","notams":[…]}  written by both jobs
//   notams/state-full.json      written by notam_full.py          {running, startedAt, fullLoadAt, count, …}
//   notams/state-delta.json     written by this worker            {lastDeltaAt, lastOK, lastCount, lastError, …}
//
// Secrets (wrangler secret put): NMS_CLIENT_ID, NMS_CLIENT_SECRET.  Var: NMS_HOST (staging now, production later).
// Data terms: redistribution is allowed; it must not be described as "FAA data" (FAA Terms of Service, NMS-API).

const PREFIX = "notams/";
const MAX_LOCATIONS = 25;
const MAX_SHARDS_PER_RUN = 300;      // more than this in one 3-minute window is left to the daily full load
const OVERLAP_SECONDS = 60;          // FAA advice: overlap delta windows by about a minute
const PRUNE_AFTER_HOURS = 24;        // ended or cancelled NOTAMs are dropped from the files after this
const LOCATION_ID = /^[A-Z0-9]{2,6}$/;

let tokenCache = null;               // {token, expires} — kept in memory only, never written to storage

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: cors() });
    if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);

    if (url.pathname === "/" || url.pathname === "/health") return health(env);

    if (url.pathname === "/v1/notams") {
      const ids = [...new Set((url.searchParams.get("locations") || "")
        .toUpperCase().split(/[\s,;]+/).filter((s) => LOCATION_ID.test(s)))].slice(0, MAX_LOCATIONS);
      if (!ids.length) return json({ error: "Give locations=ICAO,ICAO (up to 25)" }, 400);

      const cache = caches.default;
      const key = new Request(`${url.origin}/v1/notams?locations=${ids.slice().sort().join(",")}`);
      const hit = await cache.match(key);
      if (hit) return hit;

      const now = Date.now();
      const [full, delta] = await Promise.all([readJSON(env, "state-full.json"), readJSON(env, "state-delta.json")]);
      const locations = {};
      await Promise.all(ids.map(async (id) => {
        const shard = await readJSON(env, `loc/${id}.json`);
        locations[id] = (shard?.notams || []).filter((n) => isCurrent(n, now)).map((n) => ({
          ...n, upcoming: n.start ? Date.parse(n.start) > now : false,
        })).sort(byStart);
      }));
      const response = json({
        status: "ok",
        generatedAt: new Date(now).toISOString(),
        fullLoadAt: full?.fullLoadAt ?? null,
        lastUpdateAt: delta?.lastOKAt ?? null,
        source: "NOTAM Management Service (NMS) data, processed by Hampson Labs",
        notice: "Not an official briefing. Check NOTAMs with the official source for your flight before departure.",
        coverage: "Not every State's NOTAMs are received; UK N-series NOTAMs are not included.",
        locations,
      }, 200, { "cache-control": "public, max-age=60" });
      ctx.waitUntil(cache.put(key, response.clone()));
      return response;
    }
    return json({ error: "Not found" }, 404);
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(pullChanges(env));
  },
};

// MARK: - Change pull (every 3 minutes)

async function pullChanges(env) {
  const startedAt = new Date();
  const delta = (await readJSON(env, "state-delta.json")) || {};
  const full = (await readJSON(env, "state-full.json")) || {};
  const record = { ...delta, lastRunAt: startedAt.toISOString() };

  // The daily full load is rewriting the files: don't write over it. Its own start time becomes our next window.
  if (full.running && full.startedAt && Date.now() - Date.parse(full.startedAt) < 45 * 60 * 1000) {
    record.lastSkipped = "full load running";
    await writeJSON(env, "state-delta.json", record);
    return;
  }

  const floor = Date.now() - 23 * 3600 * 1000;               // the API only allows a 24-hour window
  const candidates = [delta.lastDeltaAt, full.running ? null : full.startedAt].filter(Boolean).map(Date.parse);
  let since = candidates.length ? Math.max(...candidates) - OVERLAP_SECONDS * 1000 : floor;
  since = Math.max(since, floor);

  try {
    const token = await accessToken(env);
    const query = `lastUpdatedDate=${encodeURIComponent(nmsTime(new Date(since)))}`;
    const response = await fetch(`${env.NMS_HOST}/nmsapi/v1/notams?${query}`, {
      headers: { Authorization: `Bearer ${token}`, nmsResponseFormat: "GEOJSON", Accept: "application/json" },
    });
    if (response.status === 401) tokenCache = null;
    if (!response.ok) throw new Error(`NMS ${response.status}: ${(await response.text()).slice(0, 200)}`);
    const body = await response.json();
    const features = body?.data?.geojson || [];

    const byLocation = new Map();
    for (const feature of features) {
      const n = toRecord(feature);
      if (!n) continue;
      if (!byLocation.has(n.key)) byLocation.set(n.key, []);
      byLocation.get(n.key).push(n);
    }

    let written = 0;
    const keys = [...byLocation.keys()];
    for (const id of keys.slice(0, MAX_SHARDS_PER_RUN)) {
      const shard = (await readJSON(env, `loc/${id}.json`)) || { location: id, notams: [] };
      const merged = merge(shard.notams, byLocation.get(id), Date.now());
      await writeJSON(env, `loc/${id}.json`, { location: id, notams: merged });
      written++;
    }

    Object.assign(record, {
      lastDeltaAt: startedAt.toISOString(), lastOKAt: startedAt.toISOString(), lastCount: features.length,
      lastLocations: written, lastOverflow: Math.max(0, keys.length - MAX_SHARDS_PER_RUN), lastError: null,
      lastSkipped: null, host: env.NMS_HOST,
    });
  } catch (error) {
    record.lastError = String(error?.message || error).slice(0, 300);
    record.lastErrorAt = startedAt.toISOString();
  }
  await writeJSON(env, "state-delta.json", record);
}

async function accessToken(env) {
  if (tokenCache && tokenCache.expires > Date.now() + 60_000) return tokenCache.token;
  if (!env.NMS_CLIENT_ID || !env.NMS_CLIENT_SECRET) throw new Error("NMS credentials are not set (wrangler secret put)");
  const response = await fetch(`${env.NMS_HOST}/v1/auth/token`, {   // the token URL has no /nmsapi (NMS-API FAQ)
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization: "Basic " + btoa(`${env.NMS_CLIENT_ID}:${env.NMS_CLIENT_SECRET}`),
    },
    body: "grant_type=client_credentials",
  });
  if (!response.ok) throw new Error(`NMS token ${response.status}: ${(await response.text()).slice(0, 200)}`);
  const body = await response.json();
  if (!body.access_token) throw new Error("NMS token response had no access_token");
  tokenCache = { token: body.access_token, expires: Date.now() + (Number(body.expires_in) || 1799) * 1000 };
  return tokenCache.token;
}

// MARK: - Records (the same shape as notam_full.py writes)

export function toRecord(feature) {
  const core = feature?.properties?.coreNOTAMData;
  const n = core?.notam;
  if (!n || !n.id) return null;
  if ((n.type || "").toUpperCase() === "C") return null;        // a cancellation message; the original carries the date
  const translations = core.notamTranslation || [];
  const icaoText = translations.find((t) => t.type === "ICAO")?.formattedText || null;
  const localText = translations.find((t) => t.type === "LOCAL_FORMAT")?.simpleText || null;
  const key = String(n.icaoLocation || n.location || "").toUpperCase();
  if (!LOCATION_ID.test(key)) return null;
  const point = firstPoint(feature.geometry);
  return compact({
    key,
    id: String(n.id),
    series: n.series, number: n.number, year: n.year, type: n.type,
    issued: n.issued, updated: n.lastUpdated,
    location: n.location, icao: n.icaoLocation, fir: n.affectedFir, cls: n.classification,
    start: n.effectiveStart, end: n.effectiveEnd, est: n.estimated === "true" || n.estimated === true || undefined,
    cancel: n.cancelationDate, schedule: n.schedule,
    qcode: n.selectionCode, traffic: n.traffic, purpose: n.purpose, scope: n.scope,
    lower: n.lowerLimit, upper: n.upperLimit, minFl: n.minimumFl, maxFl: n.maximumFl,
    coords: n.coordinates, radius: n.radius, lat: point?.[1], lon: point?.[0],
    text: n.text, icaoText, localText,
  });
}

export function merge(existing, incoming, now) {
  const map = new Map((existing || []).map((n) => [n.id, n]));
  for (const n of incoming) {
    const old = map.get(n.id);
    if (!old || !old.updated || !n.updated || Date.parse(n.updated) >= Date.parse(old.updated)) map.set(n.id, n);
  }
  const cutoff = now - PRUNE_AFTER_HOURS * 3600 * 1000;
  return [...map.values()].filter((n) => !endedBefore(n, cutoff)).sort((a, b) => (a.id < b.id ? -1 : 1));
}

export function isCurrent(n, now) { return !endedBefore(n, now); }

function endedBefore(n, time) {
  const cancel = n.cancel ? Date.parse(n.cancel) : NaN;
  if (!Number.isNaN(cancel) && cancel <= time) return true;
  const end = n.end && !/PERM/i.test(n.end) ? Date.parse(n.end) : NaN;
  return !Number.isNaN(end) && end <= time;
}

function firstPoint(geometry) {
  if (!geometry) return null;
  if (geometry.type === "Point") return geometry.coordinates;
  for (const g of geometry.geometries || []) { const p = firstPoint(g); if (p) return p; }
  return null;
}

function compact(object) {
  for (const k of Object.keys(object)) if (object[k] === undefined || object[k] === null || object[k] === "") delete object[k];
  return object;
}

function byStart(a, b) { return (Date.parse(a.start) || 0) - (Date.parse(b.start) || 0); }
function nmsTime(date) { return date.toISOString().replace(/\.\d{3}Z$/, "Z"); }

// MARK: - Storage and responses

async function health(env) {
  const [full, delta] = await Promise.all([readJSON(env, "state-full.json"), readJSON(env, "state-delta.json")]);
  const minutes = (iso) => (iso ? Math.round((Date.now() - Date.parse(iso)) / 60000) : null);
  const updateAge = minutes(delta?.lastOKAt);
  const fullAge = minutes(full?.fullLoadAt);
  const ok = updateAge !== null && updateAge <= 15 && (fullAge === null || fullAge <= 30 * 60);
  return json({
    service: "FlightOne NOTAMs", ok, host: delta?.host ?? env.NMS_HOST,
    lastUpdateMinutesAgo: updateAge, lastChanges: delta?.lastCount ?? null, lastError: delta?.lastError ?? null,
    fullLoadHoursAgo: fullAge === null ? null : Math.round(fullAge / 6) / 10, fullLoadCount: full?.count ?? null,
    fullLoadLocations: full?.locations ?? null, fullLoadRunning: !!full?.running,
  }, ok ? 200 : 503);
}

async function readJSON(env, name) {
  const object = await env.CHARTS.get(PREFIX + name);
  if (!object) return null;
  try { return await object.json(); } catch { return null; }
}

async function writeJSON(env, name, value) {
  await env.CHARTS.put(PREFIX + name, JSON.stringify(value), { httpMetadata: { contentType: "application/json" } });
}

function json(value, status = 200, extra = {}) {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json", ...cors(), ...extra } });
}
function cors() { return { "access-control-allow-origin": "*", "access-control-allow-methods": "GET, OPTIONS" }; }
