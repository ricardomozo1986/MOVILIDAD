#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL de tráfico (sin Shapely) – Compatible con Streamlit Cloud
-------------------------------------------------------------
- Lee una red vial GeoJSON (LineString) y la divide en subtramos (~L metros) usando pura trigonometría y distancias WGS84.
- Para cada subtramo, llama Google Routes API (Distance Matrix v2) con tráfico y calcula speed_kmh.
- Evita dependencia de GEOS/Shapely, por lo que instala fácil en entornos limitados.

Requisitos: requests
Variables de entorno: GOOGLE_MAPS_API_KEY
Uso:
  python etl_cajica_routes_noshapely.py --input cajica_segments.geojson --output cajica_speeds.geojson --subsegment_m 300 --batch_size 40
"""
import os, sys, json, math, argparse, datetime as dt
from typing import List, Tuple, Dict, Any
import requests

GOOGLE_ENDPOINT = "https://routes.googleapis.com/distanceMatrix/v2:computeRouteMatrix"
R_EARTH = 6371008.8  # metros

def haversine_m(lon1, lat1, lon2, lat2):
    # Distancia aproximada en metros entre dos puntos WGS84
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return 2 * R_EARTH * math.asin(math.sqrt(a))

def linestring_length_m(coords: List[Tuple[float,float]]) -> float:
    return sum(haversine_m(lon1, lat1, lon2, lat2) for (lon1,lat1),(lon2,lat2) in zip(coords[:-1], coords[1:]))

def interpolate_point(lon1, lat1, lon2, lat2, t: float):
    # Interpolación lineal en lon/lat (válida para distancias cortas típicas urbanas)
    return (lon1 + (lon2 - lon1)*t, lat1 + (lat2 - lat1)*t)

def densify_linestring(coords: List[Tuple[float,float]], target_len_m: float) -> List[List[Tuple[float,float]]]:
    """Divide la línea en subtramos de ~target_len_m devolviendo listas de coords por subtramo."""
    if len(coords) < 2:
        return []
    # Precompute segment lengths
    seglens = [haversine_m(*coords[i], *coords[i+1]) for i in range(len(coords)-1)]
    total = sum(seglens)
    if total == 0:
        return []
    # Número de subtramos (al menos 1)
    n = max(1, int(math.ceil(total / target_len_m)))
    out = []
    # Recorremos la línea acumulando distancia y cortando en puntos a distancias iguales
    cut_positions = [i * total / n for i in range(n)] + [total]
    # Walk
    current_seg = 0
    dist_into_seg = 0.0
    seg_accum = 0.0
    cur_pt = coords[0]
    last_cut_abs = 0.0
    pts_accum = [cur_pt]
    for cut_abs in cut_positions[1:]:  # saltamos 0
        # avanzar hasta alcanzar cut_abs
        while last_cut_abs < cut_abs and current_seg < len(seglens):
            seg_len = seglens[current_seg]
            remaining = seg_len - dist_into_seg
            needed = cut_abs - last_cut_abs
            if needed <= remaining + 1e-6:
                # cortar dentro de este segmento
                t = (dist_into_seg + needed) / seg_len
                lon1, lat1 = coords[current_seg]
                lon2, lat2 = coords[current_seg+1]
                cut_pt = interpolate_point(lon1, lat1, lon2, lat2, t)
                pts_accum.append(cut_pt)
                out.append(pts_accum)
                # preparar siguiente subtramo
                pts_accum = [cut_pt]
                last_cut_abs = cut_abs
                dist_into_seg += needed
                if abs(dist_into_seg - seg_len) < 1e-6:
                    current_seg += 1
                    dist_into_seg = 0.0
            else:
                # saltar al siguiente segmento
                last_cut_abs += remaining
                current_seg += 1
                dist_into_seg = 0.0
                pts_accum.append(coords[current_seg])
        # continuar
    # En caso de redondeos, asegura último punto
    if pts_accum and pts_accum[-1] != coords[-1]:
        pts_accum.append(coords[-1])
    # filtra subtramos degenerados
    out = [seg for seg in out if len(seg) >= 2]
    return out

def payload_matrix(origins: List[Tuple[float,float]], destinations: List[Tuple[float,float]]):
    def pt(lon, lat):
        return {"waypoint": {"location": {"latLng": {"latitude": lat, "longitude": lon}}}}
    return {
        "origins": [pt(lon,lat) for (lon,lat) in origins],
        "destinations": [pt(lon,lat) for (lon,lat) in destinations],
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_AWARE",
        "departureTime": dt.datetime.now().isoformat()
    }

def request_matrix(session, api_key: str, origins, destinations):
    headers = {"X-Goog-Api-Key": api_key, "Content-Type": "application/json"}
    data = payload_matrix(origins, destinations)
    r = session.post(GOOGLE_ENDPOINT, headers=headers, data=json.dumps(data), timeout=30)
    r.raise_for_status()
    out = []
    for line in r.text.strip().splitlines():
        if line.strip():
            out.append(json.loads(line))
    return out

def estimate_speed_kmh(distance_m: float, duration_iso: str) -> float:
    if not duration_iso or not duration_iso.endswith("s"):
        return float('nan')
    try:
        sec = float(duration_iso[:-1])
        if sec <= 0: return float('nan')
        return (distance_m / sec) * 3.6
    except Exception:
        return float('nan')

def grade_color(v: float) -> str:
    if math.isnan(v): return "#888888"
    if v >= 45: return "#2E7D32"
    if v >= 30: return "#F9A825"
    if v >= 15: return "#EF6C00"
    return "#C62828"

def run_once(input_path: str, output_path: str, subsegment_m: float = 300.0, batch_size: int = 40) -> Dict[str, Any]:
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        print("ERROR: Debes definir GOOGLE_MAPS_API_KEY en Secrets/entorno.", file=sys.stderr)
        sys.exit(2)

    gj = json.load(open(input_path, "r", encoding="utf-8"))
    features_in = gj.get("features", [])
    subsegments = []  # (lonlat_ini, lonlat_fin, coords_subtramo, props_base)
    for feat in features_in:
        if not isinstance(feat, dict): continue
        geom = feat.get("geometry", {})
        props = feat.get("properties", {}) or {}
        if geom.get("type") != "LineString": continue
        coords = geom.get("coordinates", [])
        # espera [ [lon,lat], ... ]
        subs = densify_linestring(coords, subsegment_m)
        for seg in subs:
            subsegments.append((seg[0], seg[-1], seg, props))

    import requests
    session = requests.Session()
    features_out = []
    i = 0
    while i < len(subsegments):
        batch = subsegments[i:i+batch_size]
        origins = [b[0] for b in batch]
        dests   = [b[1] for b in batch]
        try:
            cells = request_matrix(session, api_key, origins, dests)
        except Exception as e:
            # rellena sin dato
            for _, _, seg, props in batch:
                dist_m = linestring_length_m(seg)
                features_out.append({
                    "type":"Feature",
                    "geometry":{"type":"LineString","coordinates":seg},
                    "properties":{**props, "speed_kmh": None, "distance_m": round(dist_m,1),
                                  "duration": None, "updated_at": dt.datetime.utcnow().isoformat()+"Z",
                                  "color": grade_color(float('nan'))}
                })
            i += batch_size
            continue

        # mapear resultados
        for idx, (_, _, seg, props) in enumerate(batch):
            cell = next((c for c in cells if c.get("originIndex")==idx and c.get("destinationIndex")==idx), None)
            if not cell or cell.get("status")!="OK":
                spd = float('nan'); dur=None
                dist_m = linestring_length_m(seg)
            else:
                dur = cell.get("duration")
                dist_m = float(cell.get("distanceMeters", linestring_length_m(seg)))
                spd = estimate_speed_kmh(dist_m, dur)
            features_out.append({
                "type":"Feature",
                "geometry":{"type":"LineString","coordinates":seg},
                "properties":{**props, "speed_kmh": None if math.isnan(spd) else round(spd,1),
                              "distance_m": round(dist_m,1), "duration": dur,
                              "updated_at": dt.datetime.utcnow().isoformat()+"Z",
                              "color": grade_color(spd)}
            })
        i += batch_size

    out = {"type":"FeatureCollection","features":features_out}
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    print(f"[OK] Escrito {output_path} con {len(features_out)} subtramos.")
    return out

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--subsegment_m", type=float, default=300.0)
    ap.add_argument("--batch_size", type=int, default=40)
    args = ap.parse_args()
    run_once(args.input, args.output, args.subsegment_m, args.batch_size)
