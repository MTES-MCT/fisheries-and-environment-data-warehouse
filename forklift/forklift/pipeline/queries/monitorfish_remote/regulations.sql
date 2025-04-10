SELECT
    id,
    law_type,
    topic,
    zone,
    region,
    geometry_simplified
FROM regulations
WHERE
    geometry_simplified IS NOT NULL
    AND law_type IS NOT NULL
    AND topic IS NOT NULL
    AND zone IS NOT NULL
