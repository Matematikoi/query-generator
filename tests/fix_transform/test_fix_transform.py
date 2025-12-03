from query_generator.extensions.fix_transform import apply_replace_min_max

UNPARSABLE_QUERY = """
WITH RECURSIVE recursive_promo_chain AS (
    -- Seed: promotions with NULL end dates (obscure case)
    SELECT
        p_promo_sk,
        p_start_date_sk,
        p_end_date_sk,
        p_response_target,
        1 AS depth
    FROM promotion
    WHERE p_end_date_sk IS NULL OR p_start_date_sk > p_end_date_sk
    UNION ALL
    -- Recursive: chain promotions where start overlaps with previous end
    SELECT
        p.p_promo_sk,
        p.p_start_date_sk,
        p.p_end_date_sk,
        p.p_response_target,
        rpc.depth + 1
    FROM promotion p
    INNER JOIN
        recursive_promo_chain rpc
        ON p.p_start_date_sk = rpc.p_end_date_sk
    WHERE rpc.depth < 5
),
address_anomalies AS (
    -- Find addresses with bizarre characteristics
    SELECT
        ca_address_sk,
        ca_gmt_offset,
        CASE
            WHEN ca_zip IS NULL AND ca_state IS NOT NULL THEN 'phantom_location'
            WHEN ca_gmt_offset NOT BETWEEN -12 AND 14 THEN 'temporal_anomaly'
            WHEN
                ca_city = ca_county AND ca_county = ca_state
                THEN 'geographic_singularity'
            ELSE 'normal'
        END AS anomaly_type,
        -- Obscure: treating address as both dimension and measure
        CAST(ca_address_sk AS DECIMAL(10, 2))
        / NULLIF(CAST(ca_gmt_offset AS DECIMAL(5, 2)), 0)
            AS address_temporal_quotient
    FROM customer_address
    -- Tautological but semantically interesting
    WHERE ca_address_sk IN (133220, 133220 + 0, 133220 * 1)
),
date_paradoxes AS (
    -- Dates where multiple temporal properties conflict
    SELECT
        d_date_sk,
        d_qoy,
        d_year,
        -- Obscure: dates that are both weekend AND holiday but marked as current_day
        CASE
            WHEN
                d_weekend = 'Y' AND d_holiday = 'Y' AND d_current_day = 'Y'
                THEN 1
            ELSE 0
        END AS temporal_impossibility,
        -- Bizarre: dates where following_holiday occurs before the date itself
        CASE
            WHEN
                d_following_holiday = 'Y' AND d_date > CURRENT_DATE
                THEN 1
            ELSE 0
        END AS causality_violation
    FROM date_dim
    WHERE
        d_date_sk IS NOT NULL
        AND (d_qoy NOT BETWEEN 1 AND 4 OR d_qoy IS NULL) -- Invalid quarters
)
SELECT
    -- Standard count with DISTINCT to handle duplicates from recursive CTE
    COUNT(DISTINCT cs.cs_order_number) AS distinct_orders,
    -- Count with FILTER clause (SQL:2003 feature, less common)
    COUNT(cs.cs_net_paid_inc_ship) FILTER (WHERE cs.cs_net_paid_inc_ship < 0)
        AS negative_payments,
    -- Obscure: COUNT with CASE returning NULL to exclude from count
    COUNT(CASE WHEN aa.anomaly_type = 'normal' THEN ca.ca_address_sk END)
        AS normal_addresses,
    -- Bizarre: COUNT of arithmetic expression involving NULL propagation
    COUNT(NULLIF(d.d_qoy, 0) * NULLIF(dp.temporal_impossibility, 1))
        AS valid_temporal_products,
    -- Unusual: COUNT with nested COALESCE and type coercion
    COUNT(DISTINCT COALESCE(
        NULLIF(p.p_response_target, 0),
        CAST(rpc.depth AS INTEGER),
        -999
    )) AS coalesced_targets,
    -- Corner case: SUM used as existence check (returns NULL if no rows)
    COALESCE(
        SUM(
            CASE
                WHEN cs.cs_quantity = 0 AND cs.cs_net_paid_inc_ship > 0 THEN 1
            END
        ),
        0
    ) AS zero_quantity_paid_sales,
    -- Obscure aggregation: COUNT with ALL keyword (explicit default)
    COUNT(all aa.address_temporal_quotient) AS all_temporal_quotients,
    -- Bizarre: Nested aggregation through subquery in COUNT
    COUNT(
        (
            SELECT MAX(rpc2.depth)
            FROM recursive_promo_chain rpc2
            WHERE rpc2.p_promo_sk = cs.cs_promo_sk
        )
    ) AS max_promo_depths
FROM catalog_sales cs
-- Intentional CROSS JOIN for Cartesian semantics
CROSS JOIN address_anomalies aa
LEFT JOIN date_paradoxes dp ON cs.cs_ship_date_sk = dp.d_date_sk
INNER JOIN customer_address ca ON cs.cs_bill_addr_sk = ca.ca_address_sk
INNER JOIN date_dim d ON cs.cs_ship_date_sk = d.d_date_sk
-- RIGHT JOIN to include orphaned promotions
RIGHT JOIN promotion p ON cs.cs_promo_sk = p.p_promo_sk
LEFT JOIN recursive_promo_chain rpc ON p.p_promo_sk = rpc.p_promo_sk
WHERE
    cs.cs_ship_addr_sk = 133220
    -- Obscure: Redundant condition with different semantics
    AND (cs.cs_ship_addr_sk <> 133221 OR cs.cs_ship_addr_sk IS NULL)
    -- Bizarre: Self-referential inequality
    AND cs.cs_net_paid_inc_ship <> cs.cs_net_paid_inc_ship + 0.0
    -- Corner case: Checking for IEEE 754 special values
    AND aa.address_temporal_quotient IS NOT NULL
HAVING COUNT(*) > 0 OR COUNT(*) = 0
"""


def test_min_max_unparsable_query():
  result = apply_replace_min_max(
    UNPARSABLE_QUERY, None, apply_transformation=True
  )
  assert result == UNPARSABLE_QUERY
