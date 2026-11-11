// Offline checks of the worker: change pull merged into R2, API filtering, health. Fake R2 and fake FAA.
import worker, { toRecord, merge } from "../worker.js";

const store = new Map();
const R2 = {
  async get(k) { return store.has(k) ? { json: async () => JSON.parse(store.get(k)) } : null; },
  async put(k, v) { store.set(k, v); },
};
const feature = (id, icao, extra = {}) => ({ type: "Feature", properties: { coreNOTAMData: { notam: {
  id, number: "01/1", type: "N", icaoLocation: icao, location: icao.slice(1), text: `T${id}`,
  effectiveStart: "2026-01-01T00:00:00Z", effectiveEnd: "2099-01-01T00:00:00Z", lastUpdated: "2026-11-11T10:00:00Z", ...extra } } } });

let calls = [];
globalThis.fetch = async (url, init) => {
  calls.push(String(url));
  if (String(url).endsWith("/v1/auth/token")) {
    if (!init.headers.Authorization.startsWith("Basic ")) throw new Error("no basic auth");
    return new Response(JSON.stringify({ access_token: "T", expires_in: "1799" }));
  }
  if (init.headers.nmsResponseFormat !== "GEOJSON") throw new Error("missing format header");
  return new Response(JSON.stringify({ status: "Success", data: { geojson: [
    feature("1", "KJFK"), feature("2", "KJFK", { cancelationDate: "2020-01-01T00:00:00Z" }),
    feature("3", "EGLL", { effectiveStart: "2099-01-01T00:00:00Z", effectiveEnd: "2099-02-01T00:00:00Z" }),
    feature("4", "KJFK", { type: "C" }) ] } }));
};
globalThis.caches = { default: { match: async () => null, put: async () => {} } };
const env = { CHARTS: R2, NMS_HOST: "https://h", NMS_CLIENT_ID: "a", NMS_CLIENT_SECRET: "b" };
const waits = []; const ctx = { waitUntil: (p) => waits.push(p) };

await worker.scheduled({}, env, ctx); await Promise.all(waits);
const delta = JSON.parse(store.get("notams/state-delta.json"));
if (delta.lastError) throw new Error(delta.lastError);
if (delta.lastCount !== 4) throw new Error("count");
if (!calls[1].includes("/nmsapi/v1/notams?lastUpdatedDate=")) throw new Error("query " + calls[1]);
const jfk = JSON.parse(store.get("notams/loc/KJFK.json"));
if (jfk.notams.length !== 1 || jfk.notams[0].id !== "1") throw new Error("merge/prune " + JSON.stringify(jfk));

const res = await worker.fetch(new Request("https://w/v1/notams?locations=kjfk,EGLL,bad!,ZZZZ"), env, ctx);
const body = await res.json();
if (body.locations.KJFK.length !== 1 || body.locations.EGLL[0].upcoming !== true || body.locations.ZZZZ.length !== 0)
  throw new Error("api " + JSON.stringify(body));
if ("bad!" in body.locations) throw new Error("validation");

// Full load running: the pull is skipped and nothing is written over it.
store.set("notams/state-full.json", JSON.stringify({ running: true, startedAt: new Date().toISOString() }));
calls = []; await worker.scheduled({}, env, ctx); await Promise.all(waits);
if (calls.length) throw new Error("should skip while full load runs");

// Newer version replaces older; older never overwrites newer.
const a = { id: "9", updated: "2026-11-11T10:00:00Z", text: "new" }, b = { id: "9", updated: "2026-11-10T10:00:00Z", text: "old" };
if (merge([a], [b], Date.now())[0].text !== "new" || merge([b], [a], Date.now())[0].text !== "new") throw new Error("merge order");

const h = await worker.fetch(new Request("https://w/health"), env, ctx);
if (h.status !== 200) throw new Error("health " + (await h.text()));
console.log("worker checks: all pass");
