SET search_path TO nsi, public;

INSERT INTO nsi.sat_organization_changes (
    hub_org_hash_key, attribute_name, attribute_value, valid_from, valid_to, loaded_at
)
WITH unrolled_attributes AS (
    SELECT 
        encode(digest(parsed.org_id, 'sha256'), 'hex') AS hub_org_hash_key,
        item->>'column' AS attribute_name,
        item->>'value' AS attribute_value,
        src.loaded_at AS loaded_at,
        src.source_version AS source_version
    FROM nsi.raw_medical_organizations src,
    LATERAL jsonb_array_elements(src.payload) AS item
    CROSS JOIN LATERAL (
        SELECT max(CASE WHEN x->>'column' = 'id' THEN x->>'value' END) AS org_id
        FROM jsonb_array_elements(src.payload) AS x
    ) parsed
    WHERE parsed.org_id IS NOT NULL
      AND item->>'column' IN ('nameFull', 'nameShort', 'OGRN', 'INN', 'address', 'vedAffiliationId', 'inclusionDate')
),
detected_changes AS (
    SELECT 
        hub_org_hash_key,
        attribute_name,
        attribute_value,
        loaded_at::DATE AS valid_from,
        LEAD(loaded_at::DATE) OVER (
            PARTITION BY hub_org_hash_key, attribute_name 
            ORDER BY loaded_at ASC
        ) AS valid_to,
        LAG(attribute_value) OVER (
            PARTITION BY hub_org_hash_key, attribute_name 
            ORDER BY loaded_at ASC
        ) AS prev_value,
        loaded_at
    FROM unrolled_attributes
)
SELECT 
    hub_org_hash_key,
    attribute_name,
    attribute_value,
    valid_from,
    valid_to,
    loaded_at
FROM detected_changes
WHERE attribute_value IS DISTINCT FROM prev_value;

