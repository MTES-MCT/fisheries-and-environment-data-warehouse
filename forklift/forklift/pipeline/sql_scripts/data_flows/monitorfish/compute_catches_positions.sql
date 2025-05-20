INSERT INTO monitorfish.catches_positions

WITH daily_average_fishing_positions AS (
    SELECT
        cfr,
        toStartOfDay(date_time) AS vms_day,
        toStartOfDay(date_time) + INTERVAL 1 DAY AS vms_day_plus_one,
        avg(longitude) AS vms_longitude,
        avg(latitude) AS vms_latitude
    FROM monitorfish.vms
    WHERE
        toYear(date_time) = {catch_year:Integer}
        AND is_fishing
        AND cfr != 'NO_CFR'
    GROUP BY cfr, toStartOfDay(date_time)
),

far_reports AS (
    SELECT DISTINCT ON (report_id)
        report_id,
        cfr,
        toStartOfDay(far_datetime_utc) AS far_day,
        toYear(far_datetime_utc) AS far_year,
        CASE WHEN latitude = 0 AND longitude = 0 THEN NULL ELSE longitude END AS far_longitude,
        CASE WHEN latitude = 0 AND longitude = 0 THEN NULL ELSE latitude END AS far_latitude
    FROM monitorfish.catches
    WHERE toYear(far_datetime_utc) = {catch_year:Integer}
)

SELECT
    {catch_year:Integer} AS catch_year,
    r.report_id,
    r.far_latitude,
    r.far_longitude,
    COALESCE(p.vms_latitude, p_previous_day.vms_latitude) AS vms_latitude,
    COALESCE(p.vms_longitude, p_previous_day.vms_longitude) AS vms_longitude,
    dictGetOrNull(
        monitorfish.facade_areas_dict,
        'facade',
        (
            COALESCE(p.vms_longitude, p_previous_day.vms_longitude, 0.0),
            COALESCE(p.vms_latitude, p_previous_day.vms_latitude, 90.0)
        )::Point
    ) AS facade
FROM far_reports r
LEFT JOIN daily_average_fishing_positions p
ON
    r.cfr = p.cfr
    AND r.far_day = p.vms_day
LEFT JOIN daily_average_fishing_positions p_previous_day
ON
    r.cfr = p_previous_day.cfr
    AND r.far_day = p_previous_day.vms_day_plus_one

SETTINGS join_use_nulls=1