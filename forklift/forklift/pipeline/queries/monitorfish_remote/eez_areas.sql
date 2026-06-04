SELECT
    id AS ogc_fid,
    geom AS wkb_geometry,
    "TERRITORY1" AS territory1,
    "ISO_TER1" AS iso_ter1,
    "SOVEREIGN1" as sovereign1,
    "ISO_SOV1" as iso_sov1,
    "AREA_KM2" AS area_km2
FROM eez_areas