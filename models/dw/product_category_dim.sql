SELECT DISTINCT
    CONCAT(product_cate_id, product_category) AS product_category_key,
    product_cate_id AS product_category_id,
    product_category
FROM {{source('source_etl', 'product_category')}}
