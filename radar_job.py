#!/usr/bin/env python3
"""
FlightOne radar job (Hampson Labs). Replaces RainViewer with Met Office UK radar.

Every few minutes (GitHub Actions, see .github/workflows/radar.yml):
  1. finds the newest files in the Met Office's public radar bucket on AWS
     (s3://met-office-radar-obs-data, "UK Composite, Surface Rain Rate Estimate", 1 km, every 15 min);
  2. turns each new one into Web Mercator map tiles (zoom 1–7), packed into ONE file per frame;
  3. uploads the pack to the R2 bucket "flightone-charts" (radar/frames/<id>.pack) and rewrites radar/index.json;
  4. deletes frames older than the newest KEEP_FRAMES.
The flightone-radar Worker serves the index and the tiles to the apps.

Licence: Met Office radar on AWS is © Crown copyright, CC BY-SA 4.0. The tiles are an adaptation, so they are
also CC BY-SA 4.0 with credit; the apps show "Radar: Met Office © Crown copyright, CC BY-SA 4.0".

Environment (GitHub secrets): R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY. Optional R2_BUCKET.
Run locally:  python3 radar_job.py --inspect FILE.h5   (prints the HDF5 layout)
              python3 radar_job.py --dry-run           (renders into ./radar-out, uploads nothing)
"""
import argparse
import datetime as dt
import io
import json
import math
import os
import re
import struct
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

import numpy as np

SOURCE = "https://met-office-radar-obs-data.s3.eu-west-2.amazonaws.com"
KEEP_FRAMES = 12            # 3 hours at 15-minute frames
MIN_ZOOM, MAX_ZOOM = 1, 7   # the app enlarges zoom 7 for closer views (1 km data)
TILE = 256
ATTRIBUTION = "Radar: Met Office © Crown copyright, CC BY-SA 4.0 (adapted by Hampson Labs)"
TIME_IN_KEY = re.compile(r"(20\d{2})(\d{2})(\d{2})[T_-]?(\d{2})(\d{2})")

# Rain rate (mm/h) colour scale: lower bound, RGBA. Below the first bound is transparent.
SCALE = [
    (0.1, (160, 210, 255, 200)),   # very light
    (0.5, (74, 163, 255, 215)),    # light
    (1.0, (30, 111, 224, 230)),    # light–moderate
    (2.0, (41, 179, 74, 235)),     # moderate
    (4.0, (242, 210, 27, 240)),    # moderate–heavy
    (8.0, (242, 140, 27, 245)),    # heavy
    (16.0, (224, 48, 30, 250)),    # very heavy
    (32.0, (194, 27, 194, 255)),   # intense
]
NO_COVERAGE = (128, 128, 128, 46)  # inside the composite grid but beyond radar range: faint grey, so "no data" ≠ "no rain"


# ---------------------------------------------------------------------------------------------------------------
# Source bucket

def s3_list(prefix="", delimiter="/", start_after=None):
    """All (common prefixes, [(key, last_modified)]) under prefix, following continuation tokens."""
    prefixes, keys, token = [], [], None
    while True:
        query = {"list-type": "2", "prefix": prefix, "delimiter": delimiter, "max-keys": "1000"}
        if token:
            query["continuation-token"] = token
        elif start_after:
            query["start-after"] = start_after
        with urllib.request.urlopen(f"{SOURCE}/?{urllib.parse.urlencode(query)}", timeout=60) as r:
            root = ET.fromstring(r.read())
        ns = {"s": root.tag.split("}")[0].strip("{")} if root.tag.startswith("{") else {"s": ""}
        q = (lambda t: f"s:{t}") if ns["s"] else (lambda t: t)
        for p in root.findall(q("CommonPrefixes"), ns):
            prefixes.append(p.find(q("Prefix"), ns).text)
        for c in root.findall(q("Contents"), ns):
            keys.append((c.find(q("Key"), ns).text, c.find(q("LastModified"), ns).text))
        if (root.findtext(q("IsTruncated"), "false", ns) or "false").lower() != "true":
            return prefixes, keys
        token = root.findtext(q("NextContinuationToken"), None, ns)


def newest_files(count):
    """Newest radar files, walking the bucket's folders from the newest end (works for any date-ordered layout)."""
    found = []

    def walk(prefix):
        prefixes, keys = s3_list(prefix)
        files = sorted(k for k, _ in keys if k.lower().endswith((".h5", ".hdf5", ".hdf")))
        found.extend(reversed(files))
        for p in sorted(prefixes, reverse=True):
            if len(found) >= count:
                return
            walk(p)

    walk("")
    found = sorted(set(found), key=key_sort)[-count:]
    return found


def key_sort(key):
    m = TIME_IN_KEY.search(key.rsplit("/", 1)[-1]) or TIME_IN_KEY.search(key)
    return (m.groups() if m else ("",)), key


def download(key):
    url = f"{SOURCE}/{urllib.parse.quote(key)}"
    with urllib.request.urlopen(url, timeout=120) as r:
        return r.read()


# ---------------------------------------------------------------------------------------------------------------
# ODIM HDF5

def read_odim(blob):
    """Returns (rate[rows, cols] float32 with NaN = no coverage, georef dict, valid time)."""
    import h5py

    with h5py.File(io.BytesIO(blob), "r") as f:
        where = attrs(f.get("where"))
        what = attrs(f.get("what"))
        ds = f.get("dataset1") or next(f[k] for k in f if k.startswith("dataset"))
        d1 = ds.get("data1") or next(ds[k] for k in ds if k.startswith("data"))
        dwhat = {**attrs(ds.get("what")), **attrs(d1.get("what"))}
        dwhere = attrs(ds.get("where"))
        raw = d1["data"][()].astype(np.float64)

    gain = float(dwhat.get("gain", 1.0))
    offset = float(dwhat.get("offset", 0.0))
    nodata = dwhat.get("nodata", -1)
    undetect = dwhat.get("undetect", 0)
    rate = np.full(raw.shape, np.nan, dtype=np.float32)
    covered = ~np.isclose(raw, float(nodata))
    value = raw * gain + offset
    rate[covered] = np.where(np.isclose(raw[covered], float(undetect)), 0.0, value[covered])
    rate[covered & ~np.isfinite(raw)] = np.nan
    rate[covered & (rate < 0)] = 0.0

    georef = {**where, **dwhere}
    georef["xsize"] = raw.shape[1]
    georef["ysize"] = raw.shape[0]
    date = str(dwhat.get("startdate") or what.get("date") or "")
    clock = str(dwhat.get("starttime") or what.get("time") or "")
    try:
        valid = dt.datetime.strptime(date + clock[:4], "%Y%m%d%H%M").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        valid = None
    return rate, georef, valid


def attrs(group):
    if group is None:
        return {}
    out = {}
    for k, v in group.attrs.items():
        if isinstance(v, bytes):
            v = v.decode(errors="replace")
        elif isinstance(v, np.ndarray) and v.size == 1:
            v = v.item()
        elif isinstance(v, np.generic):
            v = v.item()
        if isinstance(v, bytes):
            v = v.decode(errors="replace")
        out[k] = v
    return out


def inspect(path):
    import h5py

    def show(name, obj):
        kind = "dataset" if isinstance(obj, h5py.Dataset) else "group"
        extra = f" {obj.shape} {obj.dtype}" if kind == "dataset" else ""
        print(f"/{name}  [{kind}]{extra}")
        for k, v in obj.attrs.items():
            print(f"    {k} = {v!r}")

    with h5py.File(path, "r") as f:
        for k, v in f.attrs.items():
            print(f"/ {k} = {v!r}")
        f.visititems(show)


# ---------------------------------------------------------------------------------------------------------------
# Projection (Transverse Mercator, e.g. British National Grid; Ordnance Survey formulae)

class TransverseMercator:
    def __init__(self, projdef=""):
        p = dict(re.findall(r"\+(\w+)=([^\s]+)", projdef or ""))
        a, b = 6377563.396, 6356256.909          # Airy 1830 (BNG)
        if p.get("ellps", "airy").lower() in ("wgs84", "grs80"):
            a, b = 6378137.0, 6356752.314245
        a = float(p.get("a", a)); b = float(p.get("b", b))
        self.a, self.b = a, b
        self.f0 = float(p.get("k_0", p.get("k", 0.9996012717)))
        self.lat0 = math.radians(float(p.get("lat_0", 49)))
        self.lon0 = math.radians(float(p.get("lon_0", -2)))
        self.e0 = float(p.get("x_0", 400000))
        self.n0 = float(p.get("y_0", -100000))
        self.e2 = 1 - (b * b) / (a * a)
        self.n = (a - b) / (a + b)

    def _m(self, phi):
        n, b, f0, p0 = self.n, self.b, self.f0, self.lat0
        return b * f0 * (
            (1 + n + 1.25 * n**2 + 1.25 * n**3) * (phi - p0)
            - (3 * n + 3 * n**2 + 2.625 * n**3) * np.sin(phi - p0) * np.cos(phi + p0)
            + (1.875 * n**2 + 1.875 * n**3) * np.sin(2 * (phi - p0)) * np.cos(2 * (phi + p0))
            - (35 / 24) * n**3 * np.sin(3 * (phi - p0)) * np.cos(3 * (phi + p0)))

    def forward(self, lat_deg, lon_deg):
        phi = np.radians(lat_deg); lam = np.radians(lon_deg)
        a, f0, e2 = self.a, self.f0, self.e2
        s, c = np.sin(phi), np.cos(phi)
        t = np.tan(phi)
        nu = a * f0 / np.sqrt(1 - e2 * s**2)
        rho = a * f0 * (1 - e2) / (1 - e2 * s**2) ** 1.5
        eta2 = nu / rho - 1
        m = self._m(phi)
        i = m + self.n0
        ii = nu / 2 * s * c
        iii = nu / 24 * s * c**3 * (5 - t**2 + 9 * eta2)
        iiia = nu / 720 * s * c**5 * (61 - 58 * t**2 + t**4)
        iv = nu * c
        v = nu / 6 * c**3 * (nu / rho - t**2)
        vi = nu / 120 * c**5 * (5 - 18 * t**2 + t**4 + 14 * eta2 - 58 * t**2 * eta2)
        dl = lam - self.lon0
        north = i + ii * dl**2 + iii * dl**4 + iiia * dl**6
        east = self.e0 + iv * dl + v * dl**3 + vi * dl**5
        return east, north


class Grid:
    """Maps lat/lon to (row, col) in the composite."""

    def __init__(self, georef):
        self.tm = TransverseMercator(str(georef.get("projdef", "")))
        self.cols, self.rows = int(georef["xsize"]), int(georef["ysize"])
        self.dx = float(georef.get("xscale", 1000)); self.dy = float(georef.get("yscale", 1000))
        if "UL_lat" in georef and "UL_lon" in georef:
            x, y = self.tm.forward(float(georef["UL_lat"]), float(georef["UL_lon"]))
            # Corners are given in degrees to limited precision; snap to the grid spacing.
            self.x0 = round(float(x) / self.dx) * self.dx
            self.y0 = round(float(y) / self.dy) * self.dy
        else:  # Met Office 1 km UK composite, documented layout
            self.x0, self.y0 = -405000.0, 1550000.0

    def rowcol(self, lat, lon):
        x, y = self.tm.forward(lat, lon)
        return (self.y0 - y) / self.dy, (x - self.x0) / self.dx


# ---------------------------------------------------------------------------------------------------------------
# Tiles

def palette_lut():
    """Index 0 transparent, 1 no-coverage grey, 2.. the rain classes. Returns (rgba[n,4], bounds)."""
    rgba = [(0, 0, 0, 0), NO_COVERAGE] + [c for _, c in SCALE]
    return np.array(rgba, dtype=np.uint8), np.array([b for b, _ in SCALE])


def classify(rate):
    """rate (NaN = no coverage) → palette index (uint8)."""
    _, bounds = palette_lut()
    idx = np.searchsorted(bounds, np.nan_to_num(rate, nan=-1.0), side="right").astype(np.uint8)
    idx = np.where(idx > 0, idx + 1, 0).astype(np.uint8)
    idx[np.isnan(rate)] = 1
    return idx


def max_pyramid(classes, levels=7):
    """Level k = max over 2^k × 2^k blocks (so small showers survive at low zoom). Grey (1) < rain (≥2)."""
    out = [classes]
    cur = classes
    for _ in range(levels):
        r, c = cur.shape
        r2, c2 = (r + 1) // 2, (c + 1) // 2
        pad = np.zeros((r2 * 2, c2 * 2), dtype=cur.dtype)
        pad[:r, :c] = cur
        cur = pad.reshape(r2, 2, c2, 2).max(axis=(1, 3))
        out.append(cur)
    return out


def tile_bounds_lonlat(z, x, y):
    n = 2 ** z
    lon0 = x / n * 360 - 180
    lon1 = (x + 1) / n * 360 - 180
    lat0 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat1 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return lon0, lat1, lon1, lat0  # west, south, east, north


def tile_range(z, west, south, east, north):
    n = 2 ** z
    fx = lambda lon: int(math.floor((lon + 180) / 360 * n))
    fy = lambda lat: int(math.floor((1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n))
    return range(max(0, fx(west)), min(n - 1, fx(east)) + 1), range(max(0, fy(north)), min(n - 1, fy(south)) + 1)


def render_tile(grid, pyramid, z, x, y, lattice=17):
    """Palette-index image (256×256) or None if the tile has nothing to draw."""
    # Projection is smooth over one tile: project a lattice and interpolate.
    n = 2 ** z
    t = np.linspace(0, 1, lattice)
    px = (x + t) / n
    py = (y + t) / n
    lon = px * 360 - 180
    lat = np.degrees(np.arctan(np.sinh(np.pi * (1 - 2 * py))))
    lat_g, lon_g = np.meshgrid(lat, lon, indexing="ij")
    rows, cols = grid.rowcol(lat_g, lon_g)
    if rows.max() < 0 or cols.max() < 0 or rows.min() >= grid.rows or cols.min() >= grid.cols:
        return None

    centre_lat = math.radians(float(lat.mean()))
    metres_per_px = 156543.03392 * math.cos(centre_lat) / n
    # Block at least as big as a pixel, so no rain cell falls between samples.
    level = max(0, min(len(pyramid) - 1, math.ceil(math.log2(max(metres_per_px / grid.dx, 1e-9)) - 1e-6)))
    src = pyramid[level]
    scale = 2 ** level

    # bilinear interpolation of the lattice up to 256×256 pixel centres
    u = (np.arange(TILE) + 0.5) / TILE * (lattice - 1)
    i0 = np.clip(np.floor(u).astype(int), 0, lattice - 2)
    w = u - i0
    def interp(g):
        a = g[i0][:, i0] * (1 - w)[None, :] + g[i0][:, i0 + 1] * w[None, :]
        b = g[i0 + 1][:, i0] * (1 - w)[None, :] + g[i0 + 1][:, i0 + 1] * w[None, :]
        return a * (1 - w)[:, None] + b * w[:, None]
    r = np.floor(interp(rows) / scale).astype(np.int64)
    c = np.floor(interp(cols) / scale).astype(np.int64)
    inside = (r >= 0) & (c >= 0) & (r < src.shape[0]) & (c < src.shape[1])
    img = np.zeros((TILE, TILE), dtype=np.uint8)
    img[inside] = src[r[inside], c[inside]]
    if not img.any():
        return None
    return img


def png(indexed):
    from PIL import Image

    rgba, _ = palette_lut()
    im = Image.frombytes("P", (indexed.shape[1], indexed.shape[0]), np.ascontiguousarray(indexed).tobytes())
    im.putpalette(rgba[:, :3].flatten().tolist())
    buf = io.BytesIO()
    im.save(buf, format="PNG", optimize=True, transparency=bytes(rgba[:, 3].tolist()))
    return buf.getvalue()


def build_pack(rate, georef):
    """One file per frame: b'F1RP', u32 count, count × (u16 z, u16 x, u16 y, u16 0, u32 offset, u32 length), PNGs."""
    grid = Grid(georef)
    pyramid = max_pyramid(classify(rate))
    tiles = []
    for z in range(MIN_ZOOM, MAX_ZOOM + 1):
        xs, ys = tile_range(z, -26, 43, 16, 65)  # generous box around the UK composite
        for y in ys:
            for x in xs:
                img = render_tile(grid, pyramid, z, x, y)
                if img is not None:
                    tiles.append((z, x, y, png(img)))
    header = 8 + 16 * len(tiles)
    index, body, offset = [], [], header
    for z, x, y, data in tiles:
        index.append(struct.pack("<HHHHII", z, x, y, 0, offset, len(data)))
        body.append(data)
        offset += len(data)
    return b"F1RP" + struct.pack("<I", len(tiles)) + b"".join(index) + b"".join(body), len(tiles)


# ---------------------------------------------------------------------------------------------------------------
# R2

def r2_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def load_index(client, bucket):
    if client is None:
        try:
            with open(os.path.join("radar-out", "index.json")) as f:
                return json.load(f)
        except OSError:
            return {"frames": []}
    try:
        return json.loads(client.get_object(Bucket=bucket, Key="radar/index.json")["Body"].read())
    except client.exceptions.NoSuchKey:
        return {"frames": []}
    except Exception as error:  # first run, or a transient error: start fresh but say so
        print(f"index.json not read ({error}); starting a new one")
        return {"frames": []}


def put(client, bucket, key, data, content_type, cache):
    if client is None:
        path = os.path.join("radar-out", key.split("radar/", 1)[1])
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        return
    client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type, CacheControl=cache)


def delete(client, bucket, key):
    if client is None:
        try:
            os.remove(os.path.join("radar-out", key.split("radar/", 1)[1]))
        except OSError:
            pass
        return
    client.delete_object(Bucket=bucket, Key=key)


# ---------------------------------------------------------------------------------------------------------------

def frame_id(valid, key):
    if valid:
        return valid.strftime("%Y%m%dT%H%MZ")
    m = TIME_IN_KEY.search(key.rsplit("/", 1)[-1])
    return "".join(m.groups()[:3]) + "T" + "".join(m.groups()[3:]) + "Z" if m else re.sub(r"\W", "_", key)[-40:]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--inspect", metavar="FILE", help="print the HDF5 layout of a local file and stop")
    parser.add_argument("--dry-run", action="store_true", help="write into ./radar-out instead of R2")
    args = parser.parse_args()
    if args.inspect:
        inspect(args.inspect)
        return 0

    client = None if args.dry_run else r2_client()
    bucket = os.environ.get("R2_BUCKET", "flightone-charts")
    index = load_index(client, bucket)
    have = {f["source"] for f in index.get("frames", [])}

    keys = newest_files(KEEP_FRAMES)
    print(f"Newest source files: {keys[-1] if keys else 'none'} ({len(keys)} checked)")
    frames = list(index.get("frames", []))
    added = 0
    for key in keys:
        if key in have:
            continue
        blob = download(key)
        rate, georef, valid = read_odim(blob)
        if added == 0:
            g = Grid(georef)
            print(f"Grid {g.cols}×{g.rows} at {g.dx:.0f} m, top-left corner E {g.x0:.0f} N {g.y0:.0f} "
                  f"(Met Office documents −405000 / 1550000); valid {valid}")
        fid = frame_id(valid, key)
        pack, count = build_pack(rate, georef)
        put(client, bucket, f"radar/frames/{fid}.pack", pack, "application/octet-stream", "public, max-age=31536000, immutable")
        time = int((valid or dt.datetime.now(dt.timezone.utc)).timestamp())
        frames = [f for f in frames if f["id"] != fid] + [{"id": fid, "time": time, "source": key, "tiles": count}]
        added += 1
        raining = float(np.nanmean(rate > 0.1) * 100)
        print(f"  {fid}: {count} tiles, {len(pack) / 1e6:.2f} MB, rain over {raining:.1f}% of covered area")

    frames.sort(key=lambda f: f["time"])
    old, frames = frames[:-KEEP_FRAMES], frames[-KEEP_FRAMES:]
    if added or old or not index.get("frames"):
        index = {
            "generated": int(dt.datetime.now(dt.timezone.utc).timestamp()),
            "attribution": ATTRIBUTION,
            "licence": "https://creativecommons.org/licenses/by-sa/4.0/",
            "minZoom": MIN_ZOOM, "maxZoom": MAX_ZOOM,
            "scale": [{"mmh": b, "rgba": list(c)} for b, c in SCALE],
            "frames": frames,
        }
        put(client, bucket, "radar/index.json", json.dumps(index, indent=1).encode(), "application/json", "public, max-age=60")
        for f in old:
            delete(client, bucket, f"radar/frames/{f['id']}.pack")
    print(f"Added {added}, removed {len(old)}, now {len(frames)} frames"
          + (f" ({frames[0]['id']} … {frames[-1]['id']})" if frames else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
