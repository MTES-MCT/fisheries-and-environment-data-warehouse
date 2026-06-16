SELECT
    operation_number,
    operation_country,
    operation_datetime_utc,
    report_id,
    report_datetime_utc,
    cfr,
    ircs,
    external_identification,
    vessel_name,
    flag_state,
    imo,
    sales_type,
    integration_datetime_utc,
    sender_id,
    sender_name,
    provider_id,
    provider_name,
    buyer_id,
    buyer_name,
    recipient_id,
    recipient_name,
    carrier_id,
    carrier_name,
    sales_datetime_utc,
    sales_country,
    sales_port_code,
    sales_contract_reference,
    bcd_number,
    takeover_organization_name,
    storage_facility_name,
    storage_facility_address,
    transport_document_reference,
    invoice_datetime_utc,
    invoice_number,
    takeover_contract_reference,
    trip_number,
    sales_id,
    landing_port_code,
    departure_datetime_utc,
    landing_datetime_utc,
    transmission_format::TEXT,
    (product->>'usage') AS product_usage,
    (product->>'weight')::DOUBLE PRECISION AS product_weight,
    (product->>'faoZone') AS product_fao_zone,
    (product->>'species') AS product_species,
    (product->>'currency') AS product_currency,
    (product->>'freshness') AS product_freshness,
    (product->>'sizeClass') AS product_size_class,
    (product->>'totalPrice')::DOUBLE PRECISION AS product_total_price,
    (product->>'presentation') AS product_presentation,
    (product->>'sizeCategory') AS product_size_category,
    (product->>'preservationState') AS product_preservation_state,
    (product->>'unitPrice')::DOUBLE PRECISION AS product_unit_price,
    (product->>'withdrawn') AS product_withdrawn,
    (product->>'productDestination') AS product_destination,
    (product->>'fishSize') AS product_fish_size,
    (product->>'producerOrganization') AS product_producer_organization,
    (product->>'nbFish') AS product_nb_fish,
    (product->>'economicZone') AS product_economic_zone,
    (product->>'statisticalRectangle') AS product_statistical_rectangle,
    (product->>'effortZone') AS product_effort_zone,
    (product->>'packaging') AS product_packaging,
    (product->>'conversionFactor') AS product_conversion_factor
FROM sales_notes, jsonb_array_elements(products) product
WHERE
    operation_datetime_utc >= :min_date AND
    operation_datetime_utc < :max_date AND
    operation_type IN ('DAT', 'COR') AND
    report_id NOT IN (
        SELECT referenced_report_id
        FROM sales_notes
        WHERE operation_datetime_utc >= :min_date AND operation_type = 'DEL'
        UNION ALL
        SELECT referenced_report_id
        FROM sales_notes
        WHERE operation_datetime_utc >= :min_date AND operation_type = 'COR'
    )
