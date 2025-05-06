CREATE DICTIONARY monitorfish.eez_areas_dict (
    iso_sov1 String,
    wkb_geometry MultiPolygon
)
PRIMARY KEY wkb_geometry
SOURCE(
    CLICKHOUSE(
        TABLE 'eez_areas'
        USER '{user}'
        PASSWORD '{password}'
        DB 'monitorfish'
    )
)
LIFETIME(MIN 0 MAX 0)
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))