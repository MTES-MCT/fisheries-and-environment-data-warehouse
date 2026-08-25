SELECT
    env_actions.id AS env_action_id,
    infraction->>'id' AS infraction_id,
    ARRAY(SELECT jsonb_array_elements_text(infraction->'natinf'))::INTEGER[] AS natinf,
    infraction->>'administrativeResponse' AS administrative_response,
    infraction->>'companyName' AS company_name,
    infraction->>'controlledPersonIdentity' AS controlled_person_identity,
    infraction->>'formalNotice' AS formal_notice,
    infraction->>'imo' AS imo,
    infraction->>'infractionType' AS infraction_type,
    infraction->>'mmsi' AS mmsi,
    infraction->>'nbTarget' AS nb_target,
    infraction->>'observations' AS observations,
    infraction->>'registrationNumber' AS registration_number,
    infraction->>'relevantCourt' AS relevant_court,
    infraction->>'seizure' AS seizure,
    infraction->>'toProcess' AS to_process,
    infraction->>'vesselBatchId' AS vessel_batch_id,
    infraction->>'vesselName' AS vessel_name,
    infraction->>'vesselRowNumber' AS vessel_row_number,
    infraction->>'vesselShipId' AS vessel_ship_id,
    infraction->>'vesselSize' AS vessel_size,
    infraction->>'vesselType' AS vessel_type
FROM env_actions
JOIN missions ON missions.id = env_actions.mission_id
JOIN jsonb_array_elements(value->'infractions') AS infraction ON true
WHERE
    action_type = 'CONTROL'
    AND env_actions.completion = 'COMPLETED'
    AND missions.deleted = 'false'
    AND infraction->'natinf' != 'null'