WITH cte_store3 AS (
    SELECT DISTINCT
        product_id,
        unit_price,
        product_detail
    FROM {{source('source_etl', '3')}}
),

cte_store5 AS (
    SELECT DISTINCT
        product_id,
        unit_price,
        product_detail
    FROM {{source('source_etl', 'store5')}}
),

cte_store8 AS (
    SELECT DISTINCT
        product_id,
        unit_price,
        product_detail
    FROM {{source('source_etl', 'store_8')}}
),

cte_union AS (
SELECT * FROM cte_store3
UNION ALL
SELECT * FROM cte_store5
UNION ALL 
SELECT * FROM cte_store8
)

SELECT DISTINCT
    CONCAT(product_id, unit_price, product_detail) AS product_key,
    product_id,
    unit_price,
    product_detail
FROM cte_union
