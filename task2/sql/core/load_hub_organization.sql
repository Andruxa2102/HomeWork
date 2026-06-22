SET search_path TO nsi, public;

INSERT INTO nsi.hub_organization (hub_org_hash_key, org_id, load_date, record_source)
SELECT DISTINCT
    encode(digest(parsed.org_id::text, 'sha256'::text), 'hex') AS hub_org_hash_key,
    parsed.org_id AS org_id,
    src.loaded_at AS load_date,
    'nsi.rosminzdrav.ru' AS record_source
FROM nsi.raw_medical_organizations src,
LATERAL (
    SELECT max(CASE WHEN item->>'column' = 'id' THEN item->>'value' END) AS org_id
    FROM jsonb_array_elements(src.payload) AS item
) parsed
WHERE parsed.org_id IS NOT NULL
ON CONFLICT (hub_org_hash_key) DO NOTHING;

