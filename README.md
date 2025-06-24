** Problem Statement **
Stroke is a major global health concern causing death and long-term disability. Early
prediction can significantly reduce risk and improve patient outcomes. With increasing
healthcare data, analytics can help predict stroke risk effectively. However, processing
large volumes of sensitive health data securely and efficiently is a major challenge. This
project aims to develop a secure, scalable, and efficient data pipeline that performs ETL
(Extract, Transform, Load) operations on patient health records. The system will support
incremental loading to handle fresh data updates seamlessly, ensuring real-time insights.
Additionally, it must comply with data privacy standards by applying data masking and
role-based access control. The final objective is to enable meaningful stroke prediction
analytics and deliver interactive Power BI dashboards for stakeholders and analysts

Tech: Snowflake, Azure, ADF, Power BI
Languages: SQL
Modules:

1. Data Ingestion:
● I downloaded the Stroke Prediction CSV from Kaggle.
● Uploaded it to Azure Blob Storage inside a container called raw/data/.

2. Snowflake Setup:
● Created a database named HEALTHCARE_DB to store all project-related data.
● Inside it, created two schemas: STAGING (for raw data) and ANALYTICS (for cleaned,
transformed data).
● Created two tables: PATIENT_STAGING (stores raw CSV data) and PATIENT_FACT
(stores final cleaned data for reporting).

3. Copy Data from Blob to Snowflake:
● Connected Azure Data Factory to Azure Blob Storage and Snowflake using linked
services.
● Created source dataset for the Blob CSV file and a sink dataset for the Snowflake
PATIENT_STAGING table.
● Used a Lookup activity (get_watermark) to fetch the last loaded timestamp from the
LOAD_WATERMARK table in Snowflake.
● Copy Data activity was used to load only new records (based on the watermark) from
Blob Storage into PATIENT_STAGING, and it added a LOAD_TIMESTAMP column
during ingestion.

4. Transformations in Snowflake:
● I created a stored procedure TRANSFORM_PATIENT_DATA(last_watermark) which:
● Converts data types using TRY_CAST
● Uses ROW_NUMBER() to remove duplicates
● Uses MERGE to insert/update new data in PATIENT_FACT
● Handles null BMI using COALESCE

5. Running Transformations through Script in ADF:
● Script activity (run_transformation) called a stored procedure in Snowflake that cleaned
data, removed duplicates using ROW_NUMBER(), and performed a MERGE into
PATIENT_FACT.
● Loading transformed data to Patient_Fact.

6. Updating Watermark:
● Another Script activity (update_watermark) updated the LOAD_WATERMARK table
with the latest timestamp after successful data load.
● An optional error logging path was included to log any failures into the
PIPELINE_ERROR_LOG table in Snowflake.

7. Data Visualization:
● Connected Power BI to the PATIENT_FACT table in Snowflake using DirectQuery for
live data access.
● Designed interactive dashboards that included:
● Slicers for filtering by Gender, Age Group, Residence Type, Work Type, and Smoking
Status.
● Bar charts displaying stroke distribution by Work Type, Smoking Status, and Gender vs
Residence Type
● Pie charts to visualize proportion of stroke vs non-stroke cases.
● Line graphs to observe stroke trends across different age bracket
