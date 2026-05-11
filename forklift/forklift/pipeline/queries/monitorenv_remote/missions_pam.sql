SELECT DISTINCT m.id
FROM missions m
JOIN missions_control_units mcu ON mcu.mission_id = m.id
WHERE m.start_datetime_utc >= '2025-01-01'
  AND mcu.control_unit_id IN (10404, 10080, 10141, 10121);
