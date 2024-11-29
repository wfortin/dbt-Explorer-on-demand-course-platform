WITH stores AS (
  SELECT
    id,
    name,
    opened_at,
    tax_rate
  FROM {{ source('jaffle_shop', 'stores') }}
), formula_1 AS (
  SELECT
    *,
    id AS location_id,
    name AS location_name,
    opened_at AS location_opened_at,
    tax_rate AS tax_rate_renamed
  FROM stores
)
SELECT
  *
FROM formula_1