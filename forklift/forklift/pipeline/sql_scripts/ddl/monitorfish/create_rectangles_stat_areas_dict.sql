CREATE DICTIONARY monitorfish.rectangles_stat_areas_dict (
    icesname String,
    wkb_geometry MultiPolygon
)
PRIMARY KEY wkb_geometry
SOURCE(
    CLICKHOUSE(
        TABLE 'rectangles_stat_areas'
        USER '{user}'
        PASSWORD '{password}'
        DB 'monitorfish'
    )
)
LIFETIME(MIN 0 MAX 0)
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))