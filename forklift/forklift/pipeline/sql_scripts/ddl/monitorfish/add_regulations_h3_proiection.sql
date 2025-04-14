ALTER TABLE monitorfish.regulations_h3
ADD PROJECTION monitorfish_regulations_h3_projection (
    SELECT *
    ORDER BY h3, law_type, topic, id
);