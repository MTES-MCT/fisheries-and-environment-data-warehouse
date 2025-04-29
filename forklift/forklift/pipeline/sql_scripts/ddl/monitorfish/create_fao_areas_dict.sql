CREATE DICTIONARY monitorfish.fao_areas_dict (
    f_code String,
    geometry MultiPolygon
)
PRIMARY KEY geometry
SOURCE(
    CLICKHOUSE(
        TABLE 'non_overlapping_fao_areas'
        USER '{user}'
        PASSWORD '{password}'
        DB 'monitorfish'
    )
)
LIFETIME(MIN 0 MAX 0)
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))