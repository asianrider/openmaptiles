
-- etldoc: layer_city[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_city | <z2_14> z2-z14+" ] ;

-- etldoc: osm_city_point -> layer_city:z2_14
CREATE OR REPLACE FUNCTION layer_city(bbox geometry, zoom_level int, pixel_width numeric)
RETURNS TABLE(osm_id bigint, geometry geometry, name text, name_en text, tags hstore, place city_place, "rank" int, capital int) AS $$
  SELECT * FROM (
    SELECT osm_id, geometry, name,
    COALESCE(NULLIF(name_en, ''), tags->'name:latin', name) AS name_en,
    tags,
    place, "rank", normalize_capital_level(capital) AS capital
    FROM osm_city_point
    WHERE geometry && bbox
      AND ((zoom_level = 2 AND "rank" = 1)
        OR (zoom_level BETWEEN 3 AND 6 AND "rank" <= zoom_level + 2)
      )
    UNION ALL
    SELECT osm_id, geometry, name,
        COALESCE(NULLIF(name_en, ''), tags->'name:latin', name) AS name_en,
        tags,
        place,
        COALESCE("rank", gridrank + 10),
        normalize_capital_level(capital) AS capital
    FROM (
      SELECT osm_id, geometry, name,
      COALESCE(NULLIF(name_en, ''), tags->'name:latin', name) AS name_en,
      tags,
      place, "rank", capital,
      row_number() OVER (
        PARTITION BY LabelGrid(geometry, 128 * pixel_width)
        ORDER BY "rank" ASC NULLS LAST,
        place ASC NULLS LAST,
        population DESC NULLS LAST,
        length(name) ASC
      )::int AS gridrank
        FROM osm_city_point
        WHERE geometry && bbox
          AND ((zoom_level = 6 AND place <= 'town'::city_place
            OR (zoom_level BETWEEN 7 AND 10 AND place <= 'village'::city_place)

            OR (zoom_level BETWEEN 11 AND 13 AND place <= 'suburb'::city_place)
            OR (zoom_level >= 14)
          ))
    ) AS ranked_places
    WHERE (zoom_level BETWEEN 6 AND 7 AND (gridrank <= 4 OR "rank" IS NOT NULL))
       OR (zoom_level = 8 AND (gridrank <= 8 OR "rank" IS NOT NULL))
       OR (zoom_level = 9 AND (gridrank <= 12 OR "rank" IS NOT NULL))
       OR (zoom_level BETWEEN 10 AND 11 AND (gridrank <= 14 OR "rank" IS NOT NULL))
       OR (zoom_level >= 12);
$$ LANGUAGE SQL IMMUTABLE;
