SELECT
    id,
    law_type,
    topic,
    zone,
    region,
    ST_CollectionExtract(
        CASE
            WHEN ST_NPoints(geometry_simplified) < 10000 THEN geometry_simplified
            WHEN ST_NPoints(geometry_simplified) < 50000 THEN ST_MakeValid(ST_SimplifyPreserveTopology(ST_CurveToLine(geometry), 0.001))
            ELSE ST_MakeValid(ST_SimplifyPreserveTopology(ST_CurveToLine(geometry), 0.01))
        END
    ) AS geometry_simplified
FROM regulations
WHERE
    geometry_simplified IS NOT NULL
    AND law_type IS NOT NULL
    AND topic IS NOT NULL
    AND zone IS NOT NULL
