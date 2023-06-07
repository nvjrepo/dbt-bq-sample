list down column and its data type
    SELECT column_name, data_type
    FROM MY_PROJECT.MY_DATASET.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'MY_TABLE'

date series
    SELECT *
    FROM UNNEST(GENERATE_DATE_ARRAY('2018-10-01', '2020-09-30', INTERVAL 1 DAY)) AS example