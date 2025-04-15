ALTER TABLE monitorfish.vms
ADD PROJECTION IF NOT EXISTS vms_h3_8_projection (
    SELECT *
    ORDER BY h3_8, date_time
);