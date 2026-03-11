SELECT
    t.id AS pno_type_id,
    t.name AS pno_type_name,
    t.minimum_notification_period,
    t.has_designated_ports,
    r.id AS pno_type_rule_id,
    species.species,
    fao_areas.fao_areas AS fao_area,
    gears.gears AS gear,
    flag_states.flag_states AS flag_state,
    r.minimum_quantity_kg
FROM pno_types t
JOIN pno_type_rules r
ON t.id = r.pno_type_id
LEFT JOIN unnest(r.species) AS species ON true
LEFT JOIN unnest(r.fao_areas) AS fao_areas ON true
LEFT JOIN unnest(r.gears) AS gears ON true
LEFT JOIN unnest(r.flag_states) AS flag_states ON true
ORDER BY t.id, r.id