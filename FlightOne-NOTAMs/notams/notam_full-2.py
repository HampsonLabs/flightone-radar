"""FlightOne NOTAMs — daily full load (Hampson Labs). Runs on GitHub Actions once a day.

Downloads every active NOTAM from the FAA NOTAM Management Service (NMS-API), one file per classification in GeoJSON,
and rewrites the per-location files in R2 that the FlightOne NOTAM worker serves (notams/loc/<ID>.json). Only files
whose content changed are uploaded; locations with no active NOTAMs any more are deleted. The worker merges changes
into the same files every 3 minutes.

FAA limits (NMS-API FAQ): one full load in 24 hours, where one pull of each classification counts as one full load.

Environment (GitHub secrets): NMS_CLIENT_ID, NMS_CLIENT_SECRET, R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY.
Optional: NMS_HOST (default staging), R2_BUCKET (default flightone-charts).
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import sys
import time
import zipfile
from datetime import datetime, timezone

PREFIX = "notams/"
CLASSIFICATIONS = ["DOMESTIC", "INTERNATIONAL", "FDC", "MILITARY", "LOCAL_MILITARY"]
STAGING = "https://api-staging.cgifederal-aim.com"


def log(message: str) -> None:
    print(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {message}", flush=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_location(value: str) -> bool:
    return 2 <= len(value) <= 6 and value.isascii() and value.isalnum()


# MARK: - Records (the same shape as worker.js toRecord)

def to_record(feature: dict) -> dict | None:
    core = ((feature or {}).get("properties") or {}).get("coreNOTAMData") or {}
    n = core.get("notam") or {}
    if not n.get("id"):
        return None
    if str(n.get("type", "")).upper() == "C":
        return None
    key = str(n.get("icaoLocation") or n.get("location") or "").upper()
    if not is_location(key):
        return None
    translations = core.get("notamTranslation") or []
    icao_text = next((t.get("formattedText") for t in translations if t.get("type") == "ICAO"), None)
    local_text = next((t.get("simpleText") for t in translations if t.get("type") == "LOCAL_FORMAT"), None)
    point = first_point(feature.get("geometry"))
    estimated = n.get("estimated")
    record = {
        "key": key, "id": str(n["id"]),
        "series": n.get("series"), "number": n.get("number"), "year": n.get("year"), "type": n.get("type"),
        "issued": n.get("issued"), "updated": n.get("lastUpdated"),
        "location": n.get("location"), "icao": n.get("icaoLocation"), "fir": n.get("affectedFir"),
        "cls": n.get("classification"),
        "start": n.get("effectiveStart"), "end": n.get("effectiveEnd"),
        "est": True if estimated in (True, "true") else None,
        "cancel": n.get("cancelationDate"), "schedule": n.get("schedule"),
        "qcode": n.get("selectionCode"), "traffic": n.get("traffic"), "purpose": n.get("purpose"),
        "scope": n.get("scope"), "lower": n.get("lowerLimit"), "upper": n.get("upperLimit"),
        "minFl": n.get("minimumFl"), "maxFl": n.get("maximumFl"),
        "coords": n.get("coordinates"), "radius": n.get("radius"),
        "lat": point[1] if point else None, "lon": point[0] if point else None,
        "text": n.get("text"), "icaoText": icao_text, "localText": local_text,
    }
    return {k: v for k, v in record.items() if v not in (None, "")}


def first_point(geometry):
    if not geometry:
        return None
    if geometry.get("type") == "Point":
        return geometry.get("coordinates")
    for g in geometry.get("geometries") or []:
        p = first_point(g)
        if p:
            return p
    return None


def ended(record: dict, now: float) -> bool:
    for field in ("cancel", "end"):
        value = record.get(field)
        if not value or "PERM" in str(value).upper():
            continue
        try:
            when = datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except ValueError:
            continue
        if when <= now:
            return True
    return False


def features_from_bytes(raw: bytes) -> list[dict]:
    """The classification file's exact wrapping isn't documented: accept gzip or zip, a FeatureCollection, the API
    response shape, a plain array, or one JSON feature per line."""
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    elif raw[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            raw = b"".join(z.read(name) for name in z.namelist())
    text = raw.decode("utf-8", errors="replace").strip()
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        out = []
        for line in text.splitlines():
            line = line.strip().rstrip(",")
            if line.startswith("{"):
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        return out
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        if isinstance(value.get("features"), list):
            return value["features"]
        data = value.get("data") or {}
        if isinstance(data.get("geojson"), list):
            return data["geojson"]
        if value.get("type") == "Feature":
            return [value]
    return []


def build_shards(features: list[dict], now: float) -> dict[str, list[dict]]:
    shards: dict[str, dict[str, dict]] = {}
    for feature in features:
        record = to_record(feature)
        if not record or ended(record, now):
            continue
        shards.setdefault(record["key"], {})[record["id"]] = record
    return {key: sorted(by_id.values(), key=lambda r: r["id"]) for key, by_id in shards.items()}


def shard_bytes(key: str, notams: list[dict]) -> bytes:
    return json.dumps({"location": key, "notams": notams}, separators=(",", ":"), ensure_ascii=False).encode()


# MARK: - NMS-API

class NMS:
    def __init__(self, host: str, client_id: str, client_secret: str):
        import requests
        self.requests = requests
        self.host = host.rstrip("/")
        self.auth = (client_id, client_secret)
        self.token = None
        self.expires = 0.0
        self.last_call = 0.0

    def _pace(self):
        wait = 1.2 - (time.time() - self.last_call)       # pre-production allows one request per second
        if wait > 0:
            time.sleep(wait)
        self.last_call = time.time()

    def bearer(self) -> str:
        if self.token and self.expires > time.time() + 60:
            return self.token
        self._pace()
        log("signing in")
        r = self.requests.post(f"{self.host}/v1/auth/token", data={"grant_type": "client_credentials"},
                               auth=self.auth, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"sign-in failed: HTTP {r.status_code} {r.text[:200]}")
        body = r.json()
        log("signed in")
        self.token = body["access_token"]
        self.expires = time.time() + float(body.get("expires_in") or 1799)
        return self.token

    def classification(self, name: str) -> bytes:
        """Full set for one classification. The classification must be the only query parameter: with any other
        parameter the API returns the data in the response body instead of a download link (nms-api.yaml)."""
        self._pace()
        headers = {"Authorization": f"Bearer {self.bearer()}", "nmsResponseFormat": "GEOJSON"}
        log(f"{name}: requesting")
        r = self.requests.get(f"{self.host}/nmsapi/v1/notams", params={"classification": name},
                              headers=headers, timeout=(30, 120), allow_redirects=False)
        log(f"{name}: HTTP {r.status_code}")
        if r.status_code not in (200, 307):
            raise RuntimeError(f"{name}: HTTP {r.status_code} {r.text[:300]}")
        path = r.headers.get("Location")
        if not path:
            try:
                body = r.json()
            except ValueError:
                raise RuntimeError(f"{name}: unexpected response {r.text[:300]}")
            data = body.get("data") or {}
            if isinstance(data.get("geojson"), list):          # returned directly in the body
                return r.content
            path = data.get("url")
        if not path:
            raise RuntimeError(f"{name}: no download link in the response: {r.text[:300]}")
        if path.startswith("http"):
            url = path
        elif path.startswith("/nmsapi/"):
            url = self.host + path
        else:
            url = f"{self.host}/nmsapi{path}"
        self._pace()
        log(f"{name}: downloading")
        started = time.time()
        chunks, size, next_report = [], 0, 10 << 20
        with self.requests.get(url, headers={"Authorization": f"Bearer {self.bearer()}"},
                               timeout=(30, 120), stream=True) as content:
            if content.status_code != 200:
                raise RuntimeError(f"{name}: download HTTP {content.status_code} {content.text[:300]}")
            for chunk in content.iter_content(1 << 20):
                chunks.append(chunk)
                size += len(chunk)
                if size >= next_report:
                    log(f"{name}: {size >> 20} MB")
                    next_report += 10 << 20
                if time.time() - started > 8 * 60:
                    raise RuntimeError(f"{name}: download still running after 8 minutes ({size >> 20} MB); stopped")
        log(f"{name}: {size / 1e6:.1f} MB in {time.time() - started:.0f} s")
        return b"".join(chunks)


# MARK: - R2

def r2():
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        config=Config(max_pool_connections=40, retries={"max_attempts": 3}),
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def existing_etags(s3, bucket: str) -> dict[str, str]:
    out, token = {}, None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": PREFIX + "loc/"}
        if token:
            kwargs["ContinuationToken"] = token
        page = s3.list_objects_v2(**kwargs)
        for item in page.get("Contents", []):
            key = item["Key"][len(PREFIX + "loc/"):-len(".json")]
            out[key] = item["ETag"].strip('"')
        if not page.get("IsTruncated"):
            return out
        token = page["NextContinuationToken"]


def put_json(s3, bucket: str, name: str, value) -> None:
    s3.put_object(Bucket=bucket, Key=PREFIX + name, Body=json.dumps(value).encode(), ContentType="application/json")


def get_json(s3, bucket: str, name: str):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=PREFIX + name)["Body"].read())
    except Exception:
        return None


def main() -> int:
    host = os.environ.get("NMS_HOST") or STAGING
    bucket = os.environ.get("R2_BUCKET", "flightone-charts")
    s3 = r2()
    previous = get_json(s3, bucket, "state-full.json") or {}
    started = now_iso()
    put_json(s3, bucket, "state-full.json", {**previous, "running": True, "startedAt": started, "host": host})

    try:
        nms = NMS(host, os.environ["NMS_CLIENT_ID"], os.environ["NMS_CLIENT_SECRET"])
        features, counts = [], {}
        for name in CLASSIFICATIONS:
            try:
                batch = features_from_bytes(nms.classification(name))
            except Exception as error:
                if name == "LOCAL_MILITARY":       # the FAA says there are none; never fail the load over it
                    log(f"{name}: skipped ({error})")
                    continue
                raise
            counts[name] = len(batch)
            features.extend(batch)
            log(f"{name}: {len(batch)} NOTAMs")

        shards = build_shards(features, time.time())
        total = sum(len(v) for v in shards.values())
        log(f"{total} active NOTAMs in {len(shards)} locations")
        if total == 0:
            raise RuntimeError("the full load came back empty; nothing was changed")
        if previous.get("count") and total < 0.5 * previous["count"]:
            raise RuntimeError(f"only {total} NOTAMs against {previous['count']} last time; nothing was changed")

        etags = existing_etags(s3, bucket)
        log(f"{len(etags)} location files already in storage")
        changed = []
        for key, notams in shards.items():
            body = shard_bytes(key, notams)
            if etags.get(key) != hashlib.md5(body).hexdigest():
                changed.append((key, body))
        log(f"{len(changed)} location files to upload")

        def upload(item):
            key, body = item
            for attempt in range(3):
                try:
                    s3.put_object(Bucket=bucket, Key=f"{PREFIX}loc/{key}.json", Body=body,
                                  ContentType="application/json")
                    return
                except Exception:
                    if attempt == 2:
                        raise
                    time.sleep(1 + attempt)

        from concurrent.futures import ThreadPoolExecutor
        uploaded = 0
        with ThreadPoolExecutor(max_workers=32) as pool:
            for _ in pool.map(upload, changed):
                uploaded += 1
                if uploaded % 1000 == 0:
                    log(f"uploaded {uploaded} of {len(changed)}")
        stale = [k for k in etags if k not in shards]
        for i in range(0, len(stale), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": f"{PREFIX}loc/{k}.json"} for k in stale[i:i + 1000]]})
        log(f"uploaded {uploaded}, unchanged {len(shards) - uploaded}, removed {len(stale)}")

        put_json(s3, bucket, "state-full.json", {
            "running": False, "startedAt": started, "fullLoadAt": started, "finishedAt": now_iso(), "host": host,
            "count": total, "locations": len(shards), "byClassification": counts,
            "uploaded": uploaded, "removed": len(stale), "lastError": None,
        })
        return 0
    except Exception as error:
        put_json(s3, bucket, "state-full.json", {**previous, "running": False, "host": host,
                                                  "lastError": str(error)[:300], "lastErrorAt": now_iso()})
        print(f"Full load failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
