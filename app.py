# app.py
# Streamlit app: Tráfico en vivo – Cajicá (MVP sin Waze)
# - Ejecuta el ETL (Google Routes API traffic-aware) bajo demanda.
# - Muestra el resultado en un mapa (Folium) con auto-refresco.
#
# Instrucciones (Streamlit Cloud):
# 1) Sube este repo con: app.py, etl_cajica_routes.py, requirements.txt, cajica_segments.geojson
# 2) En Settings -> Secrets, agrega tu clave:
#       GOOGLE_MAPS_API_KEY = "tu_api_key"
# 3) Deploy. En la app, pulsa "Actualizar velocidades" para ejecutar el ETL.
# 4) Ajusta frecuencia de auto-refresco en la UI si lo deseas.
#
# NOTA: No se raspan tiles/colores de Google; se usa exclusivamente la API oficial de rutas.

import os
import json
import time
import streamlit as st
from io import StringIO
from typing import Dict, Any

from etl_cajica_routes import run_once

try:
    from streamlit_folium import st_folium
    import folium
except Exception as e:
    st.error("Faltan dependencias de mapas. Asegúrate de incluir 'folium' y 'streamlit-folium' en requirements.txt.")
    raise

st.set_page_config(layout="wide", page_title="Cajicá – Tráfico en vivo (MVP sin Waze)")

# Sidebar – configuración
st.sidebar.title("Configuración")
subsegment_m = st.sidebar.slider("Longitud de subtramo (m)", 100, 600, 300, step=50)
batch_size   = st.sidebar.slider("Tamaño de lote por request", 10, 100, 40, step=10)
auto_refresh_sec = st.sidebar.slider("Auto-refresco (segundos)", 0, 300, 60, step=10,
    help="Si es 0, no se auto-refresca.")

# Secrets → variable de entorno para el ETL
if "GOOGLE_MAPS_API_KEY" in st.secrets:
    os.environ["GOOGLE_MAPS_API_KEY"] = st.secrets["GOOGLE_MAPS_API_KEY"]

st.title("Cajicá – Tráfico en vivo (MVP sin Waze)")
st.write("Este tablero estima **velocidades por subtramo** usando **Google Routes API** con `routingPreference=TRAFFIC_AWARE`. No se extraen colores/tiles de Google.")

col1, col2, col3 = st.columns([1,1,2])

with col1:
    uploaded = st.file_uploader("Cargar red vial (GeoJSON LineString)", type=["geojson","json"], accept_multiple_files=False, help="Opcional. Si no cargas, se usa 'cajica_segments.geojson' del repo.")
    run_clicked = st.button("Actualizar velocidades", type="primary")
    if uploaded:
        try:
            gj = json.load(uploaded)
            with open("cajica_segments.geojson", "w", encoding="utf-8") as f:
                json.dump(gj, f, ensure_ascii=False)
            st.success("Red vial cargada y guardada como cajica_segments.geojson")
        except Exception as e:
            st.error(f"Error leyendo GeoJSON: {e}")

with col2:
    st.info("Salida del ETL: **cajica_speeds.geojson**. Se refresca al ejecutar el botón o con auto-refresco.")
    if auto_refresh_sec > 0:
        st.caption(f"Auto-refresco activado cada {auto_refresh_sec} s.")
        st.autorefresh = st.experimental_rerun  # placeholder; below we use st_autorefresh

with col3:
    st.markdown("""
**Cómo funciona**  
1) Densifica cada tramo en subtramos (~300 m).  
2) Llama a Google Routes API (matrix) con tráfico.  
3) Calcula velocidad = distancia / duración.  
4) Publica GeoJSON y lo pinta en el mapa.  
    """)

# Ejecutar ETL bajo demanda
if run_clicked:
    try:
        out = run_once("cajica_segments.geojson", "cajica_speeds.geojson", subsegment_m=subsegment_m, batch_size=batch_size)
        st.success(f"ETL ejecutado. Subtramos: {len(out.get('features', []))}.")
    except SystemExit as se:
        st.error("Falta GOOGLE_MAPS_API_KEY en Secrets. Ve a Settings → Secrets para configurarla.")
    except Exception as e:
        st.error(f"Error en ETL: {e}")

# Cargar salida (si existe) o mostrar demo
def load_or_demo() -> Dict[str, Any]:
    path = "cajica_speeds.geojson"
    if not os.path.exists(path):
        # Crear demo mínima
        demo = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name":"Segmento demo 1","speed_kmh":18.7,"distance_m":350.0,"duration":"67.2s","updated_at":"—","color":"#EF6C00"},
                    "geometry": {"type":"LineString","coordinates":[[-74.0330,4.9145],[-74.0305,4.9170]]}
                },
                {
                    "type": "Feature",
                    "properties": {"name":"Segmento demo 2","speed_kmh":46.2,"distance_m":420.0,"duration":"32.7s","updated_at":"—","color":"#2E7D32"},
                    "geometry": {"type":"LineString","coordinates":[[-74.0285,4.9190],[-74.0255,4.9225]]}
                }
            ]
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(demo, f, ensure_ascii=False, indent=2)
        return demo
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

gj = load_or_demo()

# KPIs
features = gj.get("features", [])
speeds = [f["properties"].get("speed_kmh") for f in features if isinstance(f.get("properties",{}).get("speed_kmh", None), (int,float))]
n = len(speeds)
avg = (sum(speeds)/n) if n else None
slow = sum(1 for v in speeds if v < 15)
very_slow = sum(1 for v in speeds if v is not None and v < 10)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Tramos con dato", n if n else 0)
k2.metric("Velocidad media", f"{avg:.1f} km/h" if avg else "—")
k3.metric("< 15 km/h", slow)
k4.metric("< 10 km/h", very_slow)

# Mapa
m = folium.Map(location=[4.918, -74.028], zoom_start=13, control_scale=True)
for feat in features:
    geom = feat.get("geometry", {})
    props = feat.get("properties", {})
    if geom.get("type") != "LineString":
        continue
    coords = geom.get("coordinates", [])
    latlngs = [(lat, lon) for lon, lat in coords]  # convertir a (lat, lon)
    color = props.get("color", "#888888")
    tooltip = f"{props.get('name','Tramo')} – {props.get('speed_kmh','?')} km/h"
    popup = f"""
    <b>{props.get('name','Tramo')}</b><br/>
    Velocidad: {props.get('speed_kmh','?')} km/h<br/>
    Longitud: {props.get('distance_m','?')} m<br/>
    Actualizado: {props.get('updated_at','—')}
    """
    folium.PolyLine(latlngs, color=color, weight=6, opacity=0.9, tooltip=tooltip, popup=popup).add_to(m)

st_folium(m, width=1200, height=720)

# Auto refresh
if auto_refresh_sec and auto_refresh_sec > 0:
    st.experimental_singleton.clear()  # harmless hint for clarity
    st.experimental_rerun = None
    st.experimental_set_query_params(ts=int(time.time()))
    st.experimental_show = None
    st_autorefresh = st.experimental_rerun  # compatibility no-op
    st.experimental_memo.clear()  # clear memo caches (Streamlit <1.25 compatibility)

from streamlit_autorefresh import st_autorefresh as _st_autorefresh
if auto_refresh_sec and auto_refresh_sec > 0:
    _st_autorefresh(interval=auto_refresh_sec * 1000, key="auto_refresh_key")
