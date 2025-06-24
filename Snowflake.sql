create or replace database HEALTHCARE_DB;

create or replace schema STAGING;
create or replace schema ANALYTICS;


create or replace table HEALTHCARE_DB.STAGING.LOAD_WATERMARK (
  table_name STRING,
  last_loaded TIMESTAMP
);

insert into STAGING.LOAD_WATERMARK VALUES('PATIENT', '2024-01-01 00:00:00');

select * from STAGING.LOAD_WATERMARK ;

use schema STAGING;

create or replace table STAGING.PATIENT_STAGING (
  id STRING,
  gender STRING,
  age STRING,
  hypertension STRING,
  heart_disease STRING,
  ever_married STRING,
  work_type STRING,
  Residence_type STRING,
  avg_glucose_level STRING,
  bmi STRING,
  smoking_status STRING,
  stroke STRING,
  LOAD_TIMESTAMP TIMESTAMP
  );

select * from STAGING.PATIENT_STAGING;

use schema ANALYTICS;


create or replace table HEALTHCARE_DB.ANALYTICS.PATIENT_FACT (
  id STRING,
  gender STRING,
  age NUMBER,
  hypertension BOOLEAN,
  heart_disease BOOLEAN,
  ever_married STRING,
  work_type STRING,
  Residence_type STRING,
  avg_glucose_level FLOAT,
  bmi FLOAT,
  smoking_status STRING,
  stroke BOOLEAN,
  LOAD_TIMESTAMP TIMESTAMP
);

select * from HEALTHCARE_DB.ANALYTICS.PATIENT_FACT;



create or replace procedure TRANSFORM_PATIENT_DATA(LAST_WATERMARK TIMESTAMP)
returns STRING
language SQL
AS
$$
begin
    merge into ANALYTICS.PATIENT_FACT tgt
    USING (
        SELECT *
        FROM (
            SELECT
                TRY_CAST(id AS STRING) AS id,
                INITCAP(gender) AS gender,
                TRY_CAST(age AS FLOAT) AS age,
                TRY_CAST(hypertension AS BOOLEAN) AS hypertension,
                TRY_CAST(heart_disease AS BOOLEAN) AS heart_disease,
                INITCAP(ever_married) AS ever_married,
                INITCAP(work_type) AS work_type,
                INITCAP(CAST(residence_type AS STRING)) AS residence_type,
                TRY_CAST(avg_glucose_level AS FLOAT) AS avg_glucose_level,
                COALESCE(TRY_CAST(bmi AS FLOAT), 25.0) AS bmi ,
                INITCAP(smoking_status) AS smoking_status,
                TRY_CAST(stroke AS BOOLEAN) AS stroke,
                load_timestamp,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY load_timestamp DESC) AS rn
            FROM STAGING.PATIENT_STAGING
            WHERE id IS NOT NULL
              AND load_timestamp > :LAST_WATERMARK  
        )
        WHERE rn = 1
    ) src
    ON tgt.id = src.id
    WHEN MATCHED THEN UPDATE SET
        gender = src.gender,
        age = src.age,
        hypertension = src.hypertension,
        heart_disease = src.heart_disease,
        ever_married = src.ever_married,
        work_type = src.work_type,
        residence_type = src.residence_type,
        avg_glucose_level = src.avg_glucose_level,
        bmi = src.bmi,
        smoking_status = src.smoking_status,
        stroke = src.stroke,
        load_timestamp = src.load_timestamp
    WHEN NOT MATCHED THEN INSERT (
        id, gender, age, hypertension, heart_disease, ever_married,
        work_type, residence_type, avg_glucose_level, bmi,
        smoking_status, stroke, load_timestamp
    )
    VALUES (
        src.id, src.gender, src.age, src.hypertension, src.heart_disease,
        src.ever_married, src.work_type, src.residence_type,
        src.avg_glucose_level, src.bmi, src.smoking_status,
        src.stroke, src.load_timestamp
    );

    RETURN 'Merge transformation complete.';
END;
$$;




call TRANSFORM_PATIENT_DATA(TO_TIMESTAMP('2024-01-01 00:00:00'));


select * from HEALTHCARE_DB.ANALYTICS.PATIENT_FACT;

create or replace table STAGING.PIPELINE_ERROR_LOG (
  pipeline_name STRING,
  activity_name STRING,
  error_message STRING,
  error_time TIMESTAMP
);
select * from healthcare_db.STAGING.PIPELINE_ERROR_LOG;

-- Security and Compliance:
-- Snowflake RBAC: Implement robust Role-Based Access Control (RBAC) in  Snowflake to grant least privilege access to databases, schemas, and tables.



create or replace role data_engineer;
create or replace role analyst;
grant usage on database healthcare_db to role analyst;
grant usage on schema healthcare_db.analytics to role analyst;
grant select on all tables in schema healthcare_db.analytics to role analyst;

grant usage on warehouse compute_wh to role analyst;
grant role analyst to user chandrika1610;




create or replace masking policy mask_bmi_policy
as (val float) returns float ->
  case
    when current_role() = 'ANALYST' then null
    else val
  end;

create or replace masking policy mask_worktype_policy
as (val string) returns string ->
  case
    when current_role() = 'ANALYST' then '***MASKED***'
    else val
  end;



alter table healthcare_db.analytics.patient_fact
modify column bmi set masking policy mask_bmi_policy;

alter table healthcare_db.analytics.patient_fact
modify column work_type set masking policy mask_worktype_policy;

use role analyst;
use warehouse compute_wh;

select current_role();  -- must return ANALYST

select id, gender, bmi, work_type, stroke
from healthcare_db.analytics.patient_fact;
use role accountadmin;


-- Clustering Keys: Define clustering keys on large Snowflake tables (PATIENT_FACT) to optimize query performance for frequently filtered columns (e.g., AGE, RESIDENCE_TYPE).


-- When you filter large tables by a column (like AGE, RESIDENCE_TYPE), clustering helps Snowflake read only relevant parts of the data → much faster queries.

--  First, get structure of the table:
SELECT GET_DDL('TABLE', 'ANALYTICS.PATIENT_FACT');

-- Add Clustering to PATIENT_FACT

CREATE OR REPLACE TABLE analytics.patient_fact_clustered
CLUSTER BY (AGE, RESIDENCE_TYPE)
AS
SELECT * FROM analytics.patient_fact;


SELECT SYSTEM$CLUSTERING_INFORMATION('analytics.patient_fact_clustered');


SELECT *
FROM analytics.patient_fact
WHERE AGE BETWEEN 40 AND 60
  AND RESIDENCE_TYPE = 'Urban';


-- query history

  SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
ORDER BY TOTAL_ELAPSED_TIME DESC;----give evry query in lower case