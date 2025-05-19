CREATE DICTIONARY monitorfish.facade_areas_dict (
    facade String,
    geometry MultiPolygon
)
PRIMARY KEY geometry
SOURCE(
    CLICKHOUSE(
        TABLE 'facade_areas_subdivided'
        USER '{user}'
        PASSWORD '{password}'
        DB 'monitorfish'
    )
)
LIFETIME(MIN 0 MAX 0)
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))