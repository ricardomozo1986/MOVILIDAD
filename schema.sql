-- Schema para tráfico en vivo – Cajicá (MVP)
-- Ejecutar en PostgreSQL (idealmente con extensión PostGIS si almacenarás geometrías).

-- Tabla base de segmentos (línea central o subtramo)
CREATE TABLE IF NOT EXISTS segments (
  segment_id SERIAL PRIMARY KEY,
  name TEXT,
  source VARCHAR(64),        -- p.ej. 'OSM'
  ref_code TEXT,             -- id externo si aplica
  length_m NUMERIC,
  geom_geojson JSONB         -- almacena GeoJSON de la geometría (o usar GEOGRAPHY/GEOMETRY con PostGIS)
);

-- Observaciones de velocidad por segmento y timestamp
CREATE TABLE IF NOT EXISTS speed_observations (
  obs_id BIGSERIAL PRIMARY KEY,
  segment_id INTEGER REFERENCES segments(segment_id),
  observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
  speed_kmh NUMERIC,
  duration_s NUMERIC,
  distance_m NUMERIC,
  provider TEXT DEFAULT 'google_routes',
  raw JSONB                   -- respuesta cruda por si necesitas auditoría
);

-- Vista materializada con la última medición por segmento
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_speed AS
SELECT so.*
FROM speed_observations so
JOIN (
  SELECT segment_id, MAX(observed_at) AS max_t
  FROM speed_observations
  GROUP BY segment_id
) t ON t.segment_id = so.segment_id AND t.max_t = so.observed_at;

-- Para refrescar la vista cuando cargues nuevos datos
-- REFRESH MATERIALIZED VIEW CONCURRENTLY latest_speed;
