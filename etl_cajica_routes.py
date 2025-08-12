#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL de tráfico para Cajicá (Alternativa B, sin Waze)
---------------------------------------------------
- Toma una red vial (GeoJSON LineString) y la divide en subtramos (~300 m).
- Para cada subtramo, consulta Google Routes API (Distance Matrix v2: computeRouteMatrix)
  con routingPreference=TRAFFIC_AWARE y departureTime=now, para obtener la duración con tráfico.
- Calcula speed_kmh = distance_m / duration_s * 3.6 y publica un GeoJSON con estilos.
- Cumple Términos: no raspa tiles ni colores de Google; usa única y exclusivamente la API oficial.
Requisitos:
  pip install requests shapely==2.0.4 geopandas==0.14.4 (opcional para debug)
Variables de entorno:
  GOOGLE_MAPS_API_KEY=<tu_api_key>
Uso:
  python etl_cajica_routes.py --input cajica_segments.geojson --output cajica_speeds.geojson --subsegment_m 300 --interval_s 120
"""

import os
import sys
import time
import json
import math
import argparse
import datetime as dt
from typing import List, Tuple, Dict, Any

import requests
from shapely.geometry import LineString, Point, mapping
from shapely.ops import substring

GOOGLE_ENDPOINT = "https://routes.googleapis.com/distanceMatrix/v2:computeRouteMatrix"

def densify_to_subsegments(line: LineString, target_len_m: float) -> List[Tuple[Point, Point, LineString]]:
    """Divide una LineString en subtramos de longitud objetivo (aprox).
    Retorna lista de (pt_ini, pt_fin, geom_subsegmento)."""
    L = line.length
    if L == 0:
        return []
    segments = []
    # número de cortes, mínimo 1
    n = max(1, int(math.ceil(L / target_len_m)))
    for i in range(n):
        start = (i / n) * L
        end = min(((i + 1) / n) * L, L)
        sub = substring(line, start, end, normalized=False)
        if sub.length > 0:
            segments.append((Point(sub.coords[0]), Point(sub.coords[-1]), LineString(sub.coords)))
    return segments

def payload_matrix(origins: List[Point], destinations: List[Point]) -> Dict[str, Any]:
    """Construye el payload para computeRouteMatrix (orígenes/destinos paralelos)."""
    def pt(p: Point):
        return {"waypoint": {"location": {"latLng": {"latitude": p.y, "longitude": p.x}}}}
    return {
        "origins": [pt(p) for p in origins],
        "destinations": [pt(p) for p in destinations],
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_AWARE",
        "departureTime": dt.datetime.now().isoformat()
    }

def request_matrix(session: requests.Session, api_key: str, origins: List[Point], destinations: List[Point]) -> List[Dict[str, Any]]:
    """Llama a computeRouteMatrix. Devuelve lista de celdas (fila i, col j)."""
    headers = {
        "X-Goog-Api-Key": api_key,
        "Content-Type": "application/json"
    }
    data = payload_matrix(origins, destinations)
    r = session.post(GOOGLE_ENDPOINT, headers=headers, data=json.dumps(data), timeout=30)
    r.raise_for_status()
    # La API puede devolver NDJSON (line-delimited JSON). Intentemos parsear por líneas.
    out = []
    for line in r.text.strip().splitlines():
        if not line.strip():
            continue
        out.append(json.loads(line))
    return out

def estimate_speed_kmh(distance_m: float, duration_iso: str) -> float:
    """Convierte distance_m y duration (ISO8601, p.ej. '123.4s') a km/h."""
    if not duration_iso or not duration_iso.endswith("s"):
        return float('nan')
    try:
        sec = float(duration_iso[:-1])
        if sec <= 0:
            return float('nan')
        return (distance_m / sec) * 3.6
    except Exception:
        return float('nan')

def grade_color(speed_kmh: float) -> str:
    """Clasifica velocidad para simbología del front (no colores de Google)."""
    if math.isnan(speed_kmh):
        return "#888888"
    if speed_kmh >= 45:
        return "#2E7D32"  # verde
    if speed_kmh >= 30:
        return "#F9A825"  # ámbar
    if speed_kmh >= 15:
        return "#EF6C00"  # naranja
    return "#C62828"      # rojo

def run_once(input_path: str, output_path: str, subsegment_m: float = 300.0, batch_size: int = 50) -> Dict[str, Any]:
    """Ejecuta una pasada: lee red, densifica, consulta API por lotes y escribe GeoJSON con speed_kmh por subtramo."""
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        print("ERROR: Debes definir GOOGLE_MAPS_API_KEY en el entorno.", file=sys.stderr)
        sys.exit(2)

    with open(input_path, "r", encoding="utf-8") as f:
        gj = json.load(f)

    features_out = []
    session = requests.Session()

    # Recorre features (LineString). Cada LineString se parte en subtramos.
    all_pairs = []  # (origin_point, dest_point, linestring, props_base)
    for feat in gj.get("features", []):
        geom = feat.get("geometry", {})
        props = feat.get("properties", {}) or {}
        if geom.get("type") != "LineString":
            continue
        line = LineString(geom.get("coordinates"))
        subs = densify_to_subsegments(line, subsegment_m)
        for (p0, p1, subgeom) in subs:
            all_pairs.append((p0, p1, subgeom, props))

    # Procesamos en lotes para no exceder límites de matriz.
    # La API soporta hasta 100x100 en algunas ediciones, pero aquí usamos lotes chicos.
    i = 0
    while i < len(all_pairs):
        batch = all_pairs[i:i+batch_size]
        origins = [b[0] for b in batch]
        dests   = [b[1] for b in batch]
        try:
            cells = request_matrix(session, api_key, origins, dests)
        except Exception as e:
            print(f"[WARN] Falló request en lote {i}-{i+len(batch)}: {e}", file=sys.stderr)
            # Emitimos sin velocidad para que el front muestre gris
            for (_, _, subgeom, props) in batch:
                features_out.append({
                    "type": "Feature",
                    "geometry": mapping(subgeom),
                    "properties": {
                        **props,
                        "speed_kmh": None,
                        "distance_m": float(subgeom.length),
                        "updated_at": dt.datetime.utcnow().isoformat() + "Z",
                        "color": grade_color(float('nan'))
                    }
                })
            i += batch_size
            continue

        # cells es lista por fila, pero el API retorna celdas con indices de origen/destino
        speed_vals = []
        for cell in cells:
            oi = cell.get("originIndex")
            di = cell.get("destinationIndex")
            status = cell.get("status")
            dist_m = float(cell.get("distanceMeters", 0.0))
            dur    = cell.get("duration", None)  # ej "123.4s"
            if status != "OK":
                spd = float('nan')
            else:
                spd = estimate_speed_kmh(dist_m, dur)
            speed_vals.append((oi, di, dist_m, dur, spd))

        # Mapear de vuelta a cada subtramo
        for k, (p0, p1, subgeom, props) in enumerate(batch):
            # buscamos en speed_vals por origenIndex=k y destinoIndex=k
            match = next((sv for sv in speed_vals if sv[0] == k and sv[1] == k), None)
            if match is None:
                spd = float('nan'); dist_m = subgeom.length; dur = None
            else:
                _, _, dist_m, dur, spd = match
                if dist_m <= 0:
                    dist_m = subgeom.length
            features_out.append({
                "type": "Feature",
                "geometry": mapping(subgeom),
                "properties": {
                    **props,
                    "speed_kmh": None if math.isnan(spd) else round(spd, 1),
                    "distance_m": round(float(dist_m), 1),
                    "duration": dur,
                    "updated_at": dt.datetime.utcnow().isoformat() + "Z",
                    "color": grade_color(spd)
                }
            })
        i += batch_size

    out = {"type": "FeatureCollection", "features": features_out}
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    print(f"[OK] Escrito {output_path} con {len(features_out)} subtramos.")
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="GeoJSON de red vial (LineString)")
    ap.add_argument("--output", required=True, help="GeoJSON de salida con speed_kmh por subtramo")
    ap.add_argument("--subsegment_m", type=float, default=300.0, help="Longitud objetivo del subtramo (m)")
    ap.add_argument("--interval_s", type=int, default=0, help="Si >0, ejecuta en bucle cada N segundos")
    ap.add_argument("--batch_size", type=int, default=50, help="Tamaño de lote por request")
    args = ap.parse_args()

    if args.interval_s > 0:
        while True:
            try:
                run_once(args.input, args.output, args.subsegment_m, args.batch_size)
            except Exception as e:
                print(f"[ERROR] ciclo ETL: {e}", file=sys.stderr)
            time.sleep(max(5, args.interval_s))
    else:
        run_once(args.input, args.output, args.subsegment_m, args.batch_size)

if __name__ == "__main__":
    main()
