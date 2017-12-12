
-- etldoc: layer_poi[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_poi | <z12> z12 | <z13> z13 | <z14_> z14+" ] ;

CREATE OR REPLACE FUNCTION layer_poi(bbox geometry, zoom_level integer, pixel_width numeric)
RETURNS TABLE(osm_id bigint, geometry geometry, name text, name_en text, name_de text, name_fr text, name_it text, name_es text, name_nl text, name_ru text, tags hstore, class text, subclass text, agg_stop integer, "rank" int) AS $$
    SELECT osm_id_hash AS osm_id, geometry, NULLIF(name, '') AS name,
        COALESCE(NULLIF(name_en, ''), tags->'name:latin', name) AS name_en,
        COALESCE(NULLIF(name_de, ''), tags->'name:latin', name) AS name_de,
        COALESCE(NULLIF(name_fr, ''), tags->'name:latin', name) AS name_fr,
        COALESCE(NULLIF(name_it, ''), tags->'name:latin', name) AS name_it,
        COALESCE(NULLIF(name_es, ''), tags->'name:latin', name) AS name_es,
        COALESCE(NULLIF(name_nl, ''), tags->'name:latin', name) AS name_nl,
        COALESCE(NULLIF(name_ru, ''), tags->'name:latin', name) AS name_ru,
        tags,
        poi_class(subclass, mapping_key) AS class,
        CASE
            WHEN subclass = 'information'
                THEN NULLIF(information, '')
            ELSE subclass
        END AS subclass,
        agg_stop,
        row_number() OVER (
            PARTITION BY LabelGrid(geometry, 100 * pixel_width)
            ORDER BY CASE WHEN name = '' THEN 2000 ELSE poi_class_rank(poi_class(subclass, mapping_key)) END ASC
        )::int AS "rank"
    FROM (
        -- etldoc: osm_poi_point ->  layer_poi:z12
        -- etldoc: osm_poi_point ->  layer_poi:z13
        SELECT *,
            osm_id*10 AS osm_id_hash FROM osm_poi_point
            WHERE geometry && bbox
                AND zoom_level BETWEEN 12 AND 13
                AND ((subclass='station' AND mapping_key = 'railway')
                    OR subclass IN ('halt', 'ferry_terminal'))
        UNION ALL

        -- etldoc: osm_poi_point ->  layer_poi:z14_
        SELECT *,
            osm_id*10 AS osm_id_hash FROM osm_poi_point
            WHERE geometry && bbox
                AND zoom_level >= 14

        UNION ALL
        -- etldoc: osm_poi_polygon ->  layer_poi:z12
        -- etldoc: osm_poi_polygon ->  layer_poi:z13
        SELECT *,
            NULL::INTEGER AS agg_stop,
            CASE WHEN osm_id<0 THEN -osm_id*10+4
                ELSE osm_id*10+1
            END AS osm_id_hash
        FROM osm_poi_polygon
            WHERE geometry && bbox
                AND zoom_level BETWEEN 12 AND 13
                AND ((subclass='station' AND mapping_key = 'railway')
                    OR subclass IN ('halt', 'ferry_terminal'))

        UNION ALL
        -- etldoc: osm_poi_polygon ->  layer_poi:z14_
        SELECT *,
            NULL::INTEGER AS agg_stop,
            CASE WHEN osm_id<0 THEN -osm_id*10+4
                ELSE osm_id*10+1
            END AS osm_id_hash
        FROM osm_poi_polygon
            WHERE geometry && bbox
                AND zoom_level >= 14
        ) as poi_union
    ORDER BY "rank"
    ;
$$ LANGUAGE SQL IMMUTABLE;
