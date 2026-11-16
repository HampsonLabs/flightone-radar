"""FlightOne winds aloft: DWD ICON-EU (UK and nearby Europe) and NOAA GFS (wider area and later hours).

Runs on GitHub every hour. When a new model run is complete on the publisher's server, it takes the wind
(u and v) at each pressure level the apps use, plus the 10 m wind, cuts out our area and writes one small
file per level to R2 (flightone-charts, under wind/). The flightone-charts Worker's /wind endpoint reads a
single point's forecast from those files, so the apps never call a weather API directly.

Sources and licences:
  ICON-EU  https://opendata.dwd.de/weather/nwp/icon-eu/grib/  CC BY 4.0, credit "Source: Deutscher Wetterdienst"
           New run every 3 hours; we keep hours 0-48 (0-30 for the 03/09/15/21 runs). ~7 km grid, we keep every
           second point (0.125 degrees).
  GFS      https://nomads.ncep.noaa.gov (grib filter)  NOAA open data, CC0, no restrictions.
           New run every 6 hours; we keep hours 0-120 every 3 hours on a 0.25 degree grid.

File layout (little-endian): for each grid point, south to north then west to east, the forecast hours in order,
each as two int16 values: u and v in tenths of a metre per second. -32768 means no value (level below ground).
wind/index.json says where each model's grid starts, its spacing, size, hours and levels.
"""

import bz2
import datetime as dt
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import numpy as np

BUCKET = "flightone-charts"
INDEX_KEY = "wind/index.json"
USER_AGENT = "FlightOne-Wind/1.0 (support@hampsonlabs.co.uk)"
MISSING = -32768

# Pressure levels the apps ask for (FlightOne WIND_LEVELS). 975 hPa is not published by ICON-EU, so the
# job makes it from the average of 1000 and 950 hPa.
LEVELS = [1000, 975, 950, 925, 900, 850, 800, 700, 600, 500]

ICON = {
    "name": "icon-eu",
    "base": "https://opendata.dwd.de/weather/nwp/icon-eu/grib",
    "south": 46.0, "north": 64.0, "west": -14.0, "east": 14.0, "step": 0.125,
    "levels": [l for l in LEVELS if l != 975],
    "every": 3,
    "credit": "Source: Deutscher Wetterdienst (ICON-EU), CC BY 4.0",
}

GFS = {
    "name": "gfs",
    "filter": "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl",
    "check": "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod",
    "south": 30.0, "north": 72.0, "west": -30.0, "east": 40.0, "step": 0.25,
    "levels": LEVELS,
    "hours": list(range(0, 121, 3)),
    "every": 6,
    "credit": "NOAA GFS",
}


# MARK: Small helpers

def log(*parts):
    print(*parts, flush=True)


def http_get(url, tries=3, timeout=60):
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as res:
                return res.read()
        except urllib.error.HTTPError as err:
            if err.code == 404:
                raise
            last = err
        except Exception as err:  # network hiccup: try again
            last = err
        time.sleep(2 + attempt * 3)
    raise last


def exists(url):
    try:
        req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=30) as res:
            return 200 <= res.status < 300
    except Exception:
        return False


def latest_runs(every, delay_hours, count=4, now=None):
    """Newest model run times first, allowing for how long the publisher takes."""
    now = now or dt.datetime.now(dt.timezone.utc)
    t = now - dt.timedelta(hours=delay_hours)
    t = t.replace(minute=0, second=0, microsecond=0, hour=(t.hour // every) * every)
    return [t - dt.timedelta(hours=every * i) for i in range(count)]


def target_grid(model):
    d = model["step"]
    ny = int(round((model["north"] - model["south"]) / d)) + 1
    nx = int(round((model["east"] - model["west"]) / d)) + 1
    return ny, nx


# MARK: GRIB reading (ecCodes)

def grib_fields(blob):
    """Every message in a GRIB2 file as (shortName, typeOfLevel, level, lats, lons, values south-to-north)."""
    import eccodes

    out = []
    with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp:
        tmp.write(blob)
        tmp.flush()
        with open(tmp.name, "rb") as f:
            while True:
                gid = eccodes.codes_grib_new_from_file(f)
                if gid is None:
                    break
                try:
                    eccodes.codes_set(gid, "missingValue", 1.0e20)
                    ni = eccodes.codes_get(gid, "Ni")
                    nj = eccodes.codes_get(gid, "Nj")
                    lat1 = eccodes.codes_get(gid, "latitudeOfFirstGridPointInDegrees")
                    lon1 = eccodes.codes_get(gid, "longitudeOfFirstGridPointInDegrees")
                    di = eccodes.codes_get(gid, "iDirectionIncrementInDegrees")
                    dj = eccodes.codes_get(gid, "jDirectionIncrementInDegrees")
                    jpos = eccodes.codes_get(gid, "jScansPositively")
                    vals = eccodes.codes_get_values(gid).astype(np.float32).reshape(nj, ni)
                    vals[vals > 1.0e19] = np.nan
                    if lon1 > 180:
                        lon1 -= 360
                    lats = lat1 + (np.arange(nj) * dj if jpos else -np.arange(nj) * dj)
                    if not jpos:  # stored north to south: flip
                        vals = vals[::-1]
                        lats = lats[::-1]
                    lons = lon1 + np.arange(ni) * di
                    out.append((
                        eccodes.codes_get(gid, "shortName"),
                        eccodes.codes_get(gid, "typeOfLevel"),
                        eccodes.codes_get(gid, "level"),
                        lats, lons, vals,
                    ))
                finally:
                    eccodes.codes_release(gid)
    return out


def resample(lats, lons, vals, model):
    """Nearest source point for every point of our grid (the grids line up, so this is a plain cut-out)."""
    ny, nx = target_grid(model)
    d = model["step"]
    want_lat = model["south"] + np.arange(ny) * d
    want_lon = model["west"] + np.arange(nx) * d
    dlat = (lats[-1] - lats[0]) / max(1, len(lats) - 1)
    dlon = (lons[-1] - lons[0]) / max(1, len(lons) - 1)
    iy = np.clip(np.rint((want_lat - lats[0]) / dlat).astype(int), 0, len(lats) - 1)
    ix = np.clip(np.rint((want_lon - lons[0]) / dlon).astype(int), 0, len(lons) - 1)
    if abs(lats[iy[0]] - want_lat[0]) > d or abs(lats[iy[-1]] - want_lat[-1]) > d:
        raise ValueError("model grid does not cover our latitude range")
    if abs(lons[ix[0]] - want_lon[0]) > d or abs(lons[ix[-1]] - want_lon[-1]) > d:
        raise ValueError("model grid does not cover our longitude range")
    return vals[np.ix_(iy, ix)]


# MARK: Packing

def pack(u, v):
    """u, v: (hours, ny, nx) float m/s -> bytes laid out point by point, hours inside."""
    both = np.stack([u, v], axis=-1)                # hours, ny, nx, 2
    both = np.transpose(both, (1, 2, 0, 3))         # ny, nx, hours, 2
    scaled = np.rint(both * 10.0)
    scaled = np.where(np.isfinite(scaled), np.clip(scaled, -32767, 32767), MISSING)
    return scaled.astype("<i2").tobytes()


def level_key(level):
    return "10m" if level == "10m" else str(level)


# MARK: ICON-EU

def icon_url(run, hour, param, level=None):
    stamp = run.strftime("%Y%m%d%H")
    folder = f"{ICON['base']}/{run:%H}/{param.lower()}"
    if level is None:
        return f"{folder}/icon-eu_europe_regular-lat-lon_single-level_{stamp}_{hour:03d}_{param.upper()}.grib2.bz2"
    return f"{folder}/icon-eu_europe_regular-lat-lon_pressure-level_{stamp}_{hour:03d}_{level}_{param.upper()}.grib2.bz2"


def icon_hours(run):
    return list(range(0, (48 if run.hour % 6 == 0 else 30) + 1))


def icon_find_run():
    for run in latest_runs(ICON["every"], delay_hours=3):
        last = icon_hours(run)[-1]
        if exists(icon_url(run, last, "v", 500)) and exists(icon_url(run, last, "v_10m")):
            return run
    return None


def icon_build(run):
    hours = icon_hours(run)
    ny, nx = target_grid(ICON)
    jobs = []
    for level in ICON["levels"]:
        for param in ("u", "v"):
            for h in hours:
                jobs.append((level, param, h, icon_url(run, h, param, level)))
    for param in ("u_10m", "v_10m"):
        for h in hours:
            jobs.append(("10m", param[0], h, icon_url(run, h, param)))

    grids = {}
    for level in ICON["levels"] + ["10m"]:
        for p in ("u", "v"):
            grids[(level, p)] = np.full((len(hours), ny, nx), np.nan, dtype=np.float32)
    hour_index = {h: i for i, h in enumerate(hours)}

    def fetch(job):
        level, param, h, url = job
        fields = grib_fields(bz2.decompress(http_get(url)))
        if not fields:
            raise ValueError(f"no GRIB message in {url}")
        _, _, _, lats, lons, vals = fields[0]
        return level, param, h, resample(lats, lons, vals, ICON)

    log(f"ICON-EU {run:%Y-%m-%d %H}Z: {len(jobs)} files")
    started = time.time()
    with ThreadPoolExecutor(max_workers=16) as pool:
        for n, (level, param, h, grid) in enumerate(pool.map(fetch, jobs), 1):
            grids[(level, param)][hour_index[h]] = grid
            if n % 200 == 0:
                log(f"  {n}/{len(jobs)} in {time.time() - started:.0f} s")

    files = {}
    for level in ICON["levels"] + ["10m"]:
        files[level_key(level)] = pack(grids[(level, "u")], grids[(level, "v")])
    # 975 hPa from 1000 and 950 hPa
    u975 = (grids[(1000, "u")] + grids[(950, "u")]) / 2
    v975 = (grids[(1000, "v")] + grids[(950, "v")]) / 2
    files["975"] = pack(u975, v975)
    return hours, files


# MARK: GFS

def gfs_url(run, hour):
    levels = "".join(f"&lev_{l}_mb=on" for l in GFS["levels"])
    return (f"{GFS['filter']}?dir=%2Fgfs.{run:%Y%m%d}%2F{run:%H}%2Fatmos&file=gfs.t{run:%H}z.pgrb2.0p25.f{hour:03d}"
            f"&var_UGRD=on&var_VGRD=on{levels}&lev_10_m_above_ground=on"
            f"&subregion=&toplat={GFS['north']:g}&leftlon={GFS['west']:g}&rightlon={GFS['east']:g}&bottomlat={GFS['south']:g}")


def gfs_find_run():
    last = GFS["hours"][-1]
    for run in latest_runs(GFS["every"], delay_hours=4):
        if exists(f"{GFS['check']}/gfs.{run:%Y%m%d}/{run:%H}/atmos/gfs.t{run:%H}z.pgrb2.0p25.f{last:03d}.idx"):
            return run
    return None


def gfs_build(run):
    hours = GFS["hours"]
    ny, nx = target_grid(GFS)
    names = [l for l in GFS["levels"]] + ["10m"]
    grids = {(l, p): np.full((len(hours), ny, nx), np.nan, dtype=np.float32) for l in names for p in ("u", "v")}
    log(f"GFS {run:%Y-%m-%d %H}Z: {len(hours)} files")
    for i, h in enumerate(hours):
        blob = http_get(gfs_url(run, h), timeout=120)
        for short, typ, level, lats, lons, vals in grib_fields(blob):
            if short in ("u", "v") and typ == "isobaricInhPa" and level in GFS["levels"]:
                key = (level, short)
            elif short in ("10u", "10v") and typ == "heightAboveGround" and level == 10:
                key = ("10m", short[-1])
            else:
                continue
            grids[key][i] = resample(lats, lons, vals, GFS)
        time.sleep(0.7)  # NOAA asks for no more than 120 requests a minute
    files = {level_key(l): pack(grids[(l, "u")], grids[(l, "v")]) for l in names}
    return hours, files


# MARK: R2

def r2_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def load_index(client):
    try:
        return json.loads(client.get_object(Bucket=BUCKET, Key=INDEX_KEY)["Body"].read())
    except Exception:
        return {"models": {}}


def model_entry(model, run, hours, prefix):
    ny, nx = target_grid(model)
    return {
        "run": run.strftime("%Y%m%d%H"),
        "base": run.strftime("%Y-%m-%dT%H:00Z"),
        "hours": hours,
        "south": model["south"], "west": model["west"], "step": model["step"],
        "ny": ny, "nx": nx,
        "levels": [str(l) for l in LEVELS] + ["10m"],
        "prefix": prefix,
        "credit": model["credit"],
    }


def publish(client, index, model, run, hours, files):
    prefix = f"wind/{model['name']}/{run:%Y%m%d%H}/"
    for key, data in files.items():
        client.put_object(Bucket=BUCKET, Key=prefix + key + ".bin", Body=data,
                          ContentType="application/octet-stream", CacheControl="public, max-age=86400")
    previous = index["models"].get(model["name"], {}).get("prefix")
    index["models"][model["name"]] = model_entry(model, run, hours, prefix)
    index["updated"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    client.put_object(Bucket=BUCKET, Key=INDEX_KEY, Body=json.dumps(index).encode(),
                      ContentType="application/json", CacheControl="no-store")
    # Keep this run and the one before (a request may still be reading it); delete anything older.
    keep = {prefix, previous}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"wind/{model['name']}/"):
        for obj in page.get("Contents", []):
            run_prefix = obj["Key"].rsplit("/", 1)[0] + "/"
            if run_prefix not in keep:
                client.delete_object(Bucket=BUCKET, Key=obj["Key"])
    log(f"  published {prefix} ({sum(len(d) for d in files.values()) / 1e6:.1f} MB)")


def main():
    client = r2_client()
    index = load_index(client)
    index.setdefault("models", {})
    did = False
    for model, find, build in ((ICON, icon_find_run, icon_build), (GFS, gfs_find_run, gfs_build)):
        try:
            run = find()
            if run is None:
                log(f"{model['name']}: no complete run found yet")
                continue
            if index["models"].get(model["name"], {}).get("run") == run.strftime("%Y%m%d%H"):
                log(f"{model['name']}: {run:%Y-%m-%d %H}Z already published")
                continue
            hours, files = build(run)
            publish(client, index, model, run, hours, files)
            did = True
        except Exception as err:  # one model failing must not stop the other
            log(f"{model['name']}: FAILED: {err!r}")
            index = load_index(client)
            index.setdefault("models", {})
    if not did:
        log("Nothing new.")


if __name__ == "__main__":
    sys.exit(main())
