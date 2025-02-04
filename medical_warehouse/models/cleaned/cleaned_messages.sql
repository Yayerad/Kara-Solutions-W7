{{
    config(
        materialized='table'
    )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_messages') }}
),

cleaned AS (
    SELECT
        id,
        channel,
        TRIM(text) AS text,
        date,
        image_path,
        ROW_NUMBER() OVER (PARTITION BY text, date ORDER BY id) AS row_num
    FROM staging
    WHERE text IS NOT NULL
)

SELECT
    id,
    channel,
    text,
    date,
    image_path
FROM cleaned
WHERE row_num = 1