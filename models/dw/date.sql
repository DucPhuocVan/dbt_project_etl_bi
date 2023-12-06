WITH cte_store3 AS (
    SELECT DISTINCT
        CAST(transaction_date AS DATE) AS date,
        -- CAST(transaction_time AS TIME) AS time,
        DATEPART(HOUR, CAST(transaction_time AS TIME)) AS hour
    FROM {{source('source_etl', '3')}}
),

cte_store5 AS (
    SELECT DISTINCT
        CAST(transaction_date_time AS DATE) AS date,
        -- CAST(transaction_date_time AS TIME) AS time,
        DATEPART(HOUR, CAST(transaction_date_time AS TIME)) AS hour
    FROM {{source('source_etl', 'store5')}}
),

cte_store8_concat_date AS (
    SELECT DISTINCT
        CONCAT(transaction_year,'-',transaction_month,'-', transaction_day) AS transaction_date,
        transaction_time
    FROM {{source('source_etl', 'store_8')}}
),

cte_store8 AS (
    SELECT DISTINCT
        CAST(transaction_date AS DATE) AS date,
        -- CAST(transaction_time AS TIME) AS time,
        DATEPART(HOUR, CAST(transaction_time AS TIME)) AS hour
    FROM cte_store8_concat_date
),

cte_union AS (
SELECT * FROM cte_store3
UNION ALL
SELECT * FROM cte_store5
UNION ALL 
SELECT * FROM cte_store8
)

SELECT DISTINCT
    CONCAT(date, hour) AS date_key,
    date,
    MONTH(date) AS month,
    YEAR(date) AS year,
    hour
FROM cte_union
