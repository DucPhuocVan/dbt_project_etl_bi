WITH cte_store3 AS (
    SELECT DISTINCT
        b.product_key,
        c.product_category_key,
        d.product_type_key,
        e.date_key,
        b.unit_price AS price,
        SUM(a.transaction_qty) AS quantity,
        SUM(a.unit_price * a.transaction_qty) AS total_amount
    FROM {{source('source_etl', '3')}} a
    LEFT JOIN {{ref('product_dim')}} b
        ON a.product_id = b.product_id
        AND a.unit_price = b.unit_price
        AND a.product_detail = b.product_detail
    LEFT JOIN {{ref('product_category_dim')}} c 
        ON a.product_category = c.product_category
    LEFT JOIN {{ref('product_type_dim')}} d
        ON a.product_type = d.product_type
    LEFT JOIN {{ref('date')}} e
        ON a.transaction_date = e.date 
        AND DATEPART(HOUR, CAST(transaction_time AS TIME)) = e.hour
    GROUP BY
        b.product_key,
        c.product_category_key,
        d.product_type_key,
        e.date_key,
        b.unit_price
),

cte_store5 AS (
    SELECT DISTINCT
        b.product_key,
        c.product_category_key,
        d.product_type_key,
        e.date_key,
        CAST(b.unit_price AS FLOAT) AS price,
        SUM(CAST(a.transaction_qty AS INT)) AS quantity,
        SUM(CAST(a.unit_price AS FLOAT) * CAST(a.transaction_qty AS INT)) AS total_amount
    FROM {{source('source_etl', 'store5')}} a
    LEFT JOIN {{ref('product_dim')}} b
        ON a.product_id = b.product_id
        AND a.unit_price = b.unit_price
        AND a.product_detail = b.product_detail
    LEFT JOIN {{ref('product_category_dim')}} c 
        ON a.product_cate_id = c.product_category_id
    LEFT JOIN {{ref('product_type_dim')}} d
        ON a.product_type_id = d.product_type_id
    LEFT JOIN {{ref('date')}} e
        ON CAST(transaction_date_time AS DATE) = e.date 
        AND DATEPART(HOUR, CAST(transaction_date_time AS TIME)) = e.hour
    GROUP BY
        b.product_key,
        c.product_category_key,
        d.product_type_key,
        e.date_key,
        b.unit_price
),

cte_store8 AS (
    SELECT DISTINCT
        b.product_key,
        c.product_category_key,
        d.product_type_key,
        e.date_key,
        CAST(b.unit_price AS FLOAT) AS price,
        SUM(CAST(a.transaction_qty AS INT)) AS quantity,
        SUM(CAST(a.unit_price AS FLOAT) * CAST(a.transaction_qty AS INT)) AS total_amount
    FROM {{source('source_etl', 'store_8')}} a
    LEFT JOIN {{ref('product_dim')}} b
        ON a.product_id = b.product_id
        AND a.unit_price = b.unit_price
        AND a.product_detail = b.product_detail
    LEFT JOIN {{ref('product_category_dim')}} c 
        ON a.product_category = c.product_category
    LEFT JOIN {{ref('product_type_dim')}} d
        ON a.product_type = d.product_type
    LEFT JOIN {{ref('date')}} e
        ON CAST(CONCAT(a.transaction_year,'-',a.transaction_month,'-', a.transaction_day) AS DATE) = e.date 
        AND DATEPART(HOUR, CAST(a.transaction_time AS TIME)) = e.hour
    GROUP BY
        b.product_key,
        c.product_category_key,
        d.product_type_key,
        e.date_key,
        b.unit_price
)

SELECT * FROM cte_store3
UNION ALL
SELECT * FROM cte_store5
UNION ALL 
SELECT * FROM cte_store8