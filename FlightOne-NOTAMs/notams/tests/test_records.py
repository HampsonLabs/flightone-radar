"""Offline checks of the record and shard logic against the FAA's own sample feature (nms-api.yaml)."""
import json, os, sys, time, gzip
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from notam_full import to_record, build_shards, features_from_bytes, ended

SAMPLE = {"type": "Feature", "properties": {"coreNOTAMData": {
    "notamEvent": {"encoding": "ANNOTATION", "scenario": "123"},
    "notam": {"id": "NMS_ID_1234567812345678", "series": "A", "number": "01/123", "year": "2025", "type": "N",
              "issued": "2025-03-17T17:08:00.000Z", "affectedFir": "ZTL", "selectionCode": "QXXXX", "traffic": "IV",
              "purpose": "BO", "scope": "A", "minimumFl": "000", "maximumFl": "999", "location": "CLT",
              "effectiveStart": "2025-03-17T17:02:00.000Z", "effectiveEnd": "2099-09-18T21:11:00.000Z",
              "estimated": "true", "text": "27 RWY END ID LGT U/S", "classification": "DOM",
              "accountId": "XYZ", "lastUpdated": "2025-03-17T17:08:00.000Z", "icaoLocation": "KCLT",
              "schedule": "DAILY 0500-1900", "lowerLimit": "SFC", "upperLimit": "280M",
              "coordinates": "3939N04302E", "radius": "5"},
    "notamTranslation": [{"type": "LOCAL_FORMAT", "simpleText": "!CLT 01/123 SAMPLE NOTAM"},
                         {"type": "ICAO", "formattedText": "01/123 NOTAMR \nQ) Q_CODE \nA) CLT"}]}},
    "geometry": {"type": "GeometryCollection", "geometries": [
        {"type": "Point", "coordinates": [-81.19, 40.0]}]}}

r = to_record(SAMPLE)
assert r["key"] == "KCLT" and r["id"] == "NMS_ID_1234567812345678" and r["est"] is True
assert r["lat"] == 40.0 and r["lon"] == -81.19 and r["icaoText"].startswith("01/123")
cancel = json.loads(json.dumps(SAMPLE)); cancel["properties"]["coreNOTAMData"]["notam"]["type"] = "C"
assert to_record(cancel) is None
old = json.loads(json.dumps(SAMPLE)); n = old["properties"]["coreNOTAMData"]["notam"]
n["id"] = "2"; n["effectiveEnd"] = "2020-01-01T00:00:00Z"
gone = json.loads(json.dumps(SAMPLE)); g = gone["properties"]["coreNOTAMData"]["notam"]
g["id"] = "3"; g["cancelationDate"] = "2021-01-01T00:00:00Z"
perm = json.loads(json.dumps(SAMPLE)); p = perm["properties"]["coreNOTAMData"]["notam"]
p["id"] = "4"; p["effectiveEnd"] = "PERM"; p["icaoLocation"] = "EGLL"
shards = build_shards([SAMPLE, old, gone, perm], time.time())
assert sorted(shards) == ["EGLL", "KCLT"] and len(shards["KCLT"]) == 1 and len(shards["EGLL"]) == 1

fc = json.dumps({"type": "FeatureCollection", "features": [SAMPLE, perm]}).encode()
assert len(features_from_bytes(gzip.compress(fc))) == 2
assert len(features_from_bytes(json.dumps([SAMPLE]).encode())) == 1
assert len(features_from_bytes((json.dumps(SAMPLE) + "\n" + json.dumps(perm)).encode())) == 2
assert len(features_from_bytes(json.dumps({"status": "Success", "data": {"geojson": [SAMPLE]}}).encode())) == 1
print("python record checks: all pass")
