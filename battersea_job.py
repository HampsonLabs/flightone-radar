"""FlightOne Battersea local data, fetched on GitHub once a month.

The public Overpass (OpenStreetMap) servers turn away requests from Cloudflare, so the chart server can't fetch this
itself. This job runs on GitHub instead, tries each public Overpass server in turn and uploads the answer to R2
(flightone-charts, battersea/osm.json). The chart server's /battersea hands that copy to the apps.
Data: (c) OpenStreetMap contributors, ODbL.
"""

import datetime as dt
import json
import os
import sys
import urllib.parse
import urllib.request

QUERY = """[out:json][timeout:120];
way["waterway"="river"]["name"="River Thames"](51.455,-0.275,51.515,-0.070)->.river;
(way(around.river:120)["bridge"]["name"];way(around.river:120)["man_made"="bridge"]["name"];)->.bridges;
(node["railway"="station"]["name"="Clapham Junction"](51.455,-0.185,51.475,-0.160);
way["highway"]["name"="Albert Bridge Road"](51.465,-0.175,51.490,-0.155);
way["building"]["name"~"Albion Riverside",i](51.470,-0.180,51.490,-0.155);
way["aeroway"](51.465,-0.186,51.475,-0.172););
.river out geom;.bridges out tags geom;out tags geom center;"""

ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]
BUCKET = "flightone-charts"


def fetch():
    errors = []
    body = urllib.parse.urlencode({"data": QUERY}).encode()
    for url in ENDPOINTS:
        try:
            req = urllib.request.Request(url, data=body, headers={
                "User-Agent": "FlightOne Battersea job (support@hampsonlabs.co.uk)",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=240) as res:
                text = res.read().decode("utf-8")
            data = json.loads(text)
            if not data.get("elements"):
                raise ValueError("no elements")
            print(f"{url}: {len(data['elements'])} elements", flush=True)
            return text, url
        except Exception as err:  # try the next server
            print(f"{url}: {err!r}", flush=True)
            errors.append(f"{urllib.parse.urlparse(url).netloc}: {err}")
    raise SystemExit("All Overpass servers failed: " + "; ".join(errors))


def main():
    text, source = fetch()
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    s3.put_object(Bucket=BUCKET, Key="battersea/osm.json", Body=text.encode(), ContentType="application/json")
    status = {"ok": True, "source": urllib.parse.urlparse(source).netloc, "via": "GitHub job",
              "at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "bytes": len(text)}
    s3.put_object(Bucket=BUCKET, Key="battersea/status.json", Body=json.dumps(status).encode(), ContentType="application/json")
    print("published battersea/osm.json", status, flush=True)


if __name__ == "__main__":
    sys.exit(main())
