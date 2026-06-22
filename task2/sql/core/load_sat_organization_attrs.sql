SET search_path TO nsi, public;


INSERT INTO nsi.sat_organization_attrs (
    hub_org_hash_key, hashdiff, load_date, 
    full_name, short_name, ogrn, inn, address, 
    ved_affiliation_id, inclusion_date, record_source
)
SELECT 
    encode(digest(parsed.org_id::text, 'sha256'::text), 'hex') AS hub_org_hash_key,
    encode(digest(
        concat_ws('||', 
            parsed.org_id, parsed.name_full, parsed.name_short, 
            parsed.ogrn, parsed.inn, parsed.address, 
            parsed.parsed_ved_id, parsed.inclusion_date
        )::text, 
        'sha256'::text
    ), 'hex') AS hashdiff,
    src.loaded_at AS load_date,
    parsed.name_full AS full_name,
    parsed.name_short AS short_name,
    parsed.ogrn AS ogrn,
    parsed.inn AS inn,
    parsed.address AS address,
    parsed.parsed_ved_id AS ved_affiliation_id,
    NULLIF(parsed.inclusion_date, '')::DATE AS inclusion_date,
    'nsi.rosminzdrav.ru' AS record_source
FROM nsi.raw_medical_organizations src,
LATERAL (
    SELECT 
        max(CASE WHEN item->>'column' = 'id' THEN item->>'value' END) AS org_id,
        max(CASE WHEN item->>'column' = 'nameFull' THEN item->>'value' END) AS name_full,
        max(CASE WHEN item->>'column' = 'nameShort' THEN item->>'value' END) AS name_short,
        max(CASE WHEN item->>'column' = 'OGRN' THEN item->>'value' END) AS ogrn,
        max(CASE WHEN item->>'column' = 'INN' THEN item->>'value' END) AS inn,
        max(CASE WHEN item->>'column' = 'address' THEN item->>'value' END) AS address,
        max(CASE WHEN item->>'column' = 'vedAffiliationId' THEN item->>'value' END) AS parsed_ved_id,
        max(CASE WHEN item->>'column' = 'inclusionDate' THEN item->>'value' END) AS inclusion_date
    FROM jsonb_array_elements(src.payload) AS item
) parsed
WHERE parsed.org_id IS NOT NULL
ON CONFLICT (hub_org_hash_key, load_date) DO NOTHING;

