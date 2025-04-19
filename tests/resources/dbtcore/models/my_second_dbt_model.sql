SELECT
    t1.id AS pk_id,
    t1.data_value AS data_value1,
    CONCAT(t1.column_3, '-concat-1', t1.data_value, t2.row_data) AS data_value2,
    t3.event_tstamp AS event_tstamp
FROM {{ ref('my_first_dbt_model') }} AS t1
LEFT JOIN {{ ref('my_core_table1') }} AS t2 ON t1.id = t2.id
LEFT JOIN {{ ref('my_executedlt_model') }} AS t3 ON t1.id = t3.event_id
WHERE t1.id IN (1, 2)
