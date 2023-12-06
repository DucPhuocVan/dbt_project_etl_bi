SELECT DISTINCT
    CONCAT(product_type_id, product_type) AS product_type_key,
    product_type_id,
    product_type
FROM {{source('source_etl', 'product_type')}}
