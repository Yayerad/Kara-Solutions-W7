WITH raw_messages AS (
    SELECT * FROM public.messages
)

SELECT
    id,
    channel,
    text,
    date,
    image_path
FROM raw_messages