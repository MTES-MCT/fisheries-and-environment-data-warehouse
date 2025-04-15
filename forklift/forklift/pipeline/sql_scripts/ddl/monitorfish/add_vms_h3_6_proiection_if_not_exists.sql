ALTER TABLE monitorfish.vms
ADD PROJECTION IF NOT EXISTS vms_h3_6_projection (
    SELECT *
    ORDER BY h3_6, date_time
);