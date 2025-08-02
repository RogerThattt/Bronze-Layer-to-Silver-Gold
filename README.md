# Bronze-Layer-to-Silver-Gold

Phase 1: Setup and Bronze Layer Preparation
This phase confirms the initial state of the data pipeline.

Data Source: The pipeline begins with streaming data from a Raspberry Pi IoT simulator, which sends messages (temperature, humidity, RPM, etc.) to an Azure IoT Hub.
Bronze Table Ingestion: The raw, unprocessed streaming data from the IoT Hub is ingested into a Delta table named bronze_iot. This table serves as the "single source of truth," capturing the complete, unaltered history of incoming data in an append-only fashion.
Phase 2: Bronze to Silver Layer Transformation (Deduplication & Quality)
The goal of this phase is to cleanse and prepare the data, preserving its original structure while improving its reliability.

Create the Silver Table: Define and create a new Delta table, silver_iot, with a schema identical to the bronze_iot table. This will be the destination for the cleansed data.

Implement the Streaming Deduplication Logic:

Create a streaming read from the bronze_iot table using spark.readStream.
To handle late-arriving data and manage state efficiently, apply a watermark on the timestamp column (e.g., withWatermark("timestamp", "10 seconds")).
Drop duplicate records based on a unique key, such as device_id, using dropDuplicates(["device_id"]).
Develop the Upsert (Merge) Function:

Create a Python function that will be called for each micro-batch of data from the stream. This function will perform an "insert-only merge" to prevent writing records that already exist in the silver table.
Inside the function:
Register the incoming micro-batch DataFrame as a temporary view (e.g., bronze_microbatch).
Execute a MERGE INTO SQL command:
Target: The silver_iot table.
Source: The bronze_microbatch temporary view.
Condition: Match records on device_id.
Action: WHEN NOT MATCHED THEN INSERT *. This ensures only new, unique records are added to the silver table.
Construct and Run the Bronze-to-Silver Stream:

Combine the previous steps into a single streaming write query.
Use .writeStream on the deduplicated streaming DataFrame.
Use the .foreachBatch() sink to apply your upsert function to each micro-batch.
Crucially, specify a .checkpointLocation. This is essential for the stream to track its state and recover gracefully.
Set the stream reader option skipChangeCommits to true. This prevents the pipeline from failing if records are updated or deleted in the source bronze table, making your pipeline more robust.
Start the stream. It will process existing data in the bronze table and then continuously process new data as it arrives.
Implement Data Quality Checks (Choose one method):

Method A: Quarantining:
Create a third table, rpm_quarantine_table, to hold records that fail quality checks.
Modify your logic to filter the data based on a quality rule (e.g., RPM > 0).
Write valid records to the silver_iot table.
Write invalid records to the rpm_quarantine_table.
Method B: Flagging (Recommended):
Add a new column to the silver_iot table (e.g., quality_flag).
In your transformation logic, use a CASE WHEN statement to populate this flag based on your quality rules (e.g., CASE WHEN RPM <= 0 THEN 'INVALID' ELSE 'VALID' END). This avoids managing multiple tables and keeps all data in one place with context.
Phase 3: Silver to Gold Layer Transformation (Aggregation)
The final phase involves creating aggregated, business-ready tables (data marts) for analytics and reporting. This example creates a materialized view that calculates aggregate metrics per device.

Define the Aggregation Logic:

Write a Spark SQL query that calculates the required business metrics. For example, select device_id and calculate the MIN, MAX, and AVG of temperature and rpm from the silver_iot table, grouping by device_id.
Create a Materialized View using Structured Streaming:

Since CREATE MATERIALIZED VIEW is primarily a Delta Live Tables feature, you can replicate its functionality manually using Structured Streaming.
Create a streaming read from the silver_iot table.
Apply the aggregation query from the previous step to the streaming DataFrame.
Use .writeStream to write the aggregated results to a new gold_device_aggregates table.
Output Mode: Use outputMode("complete"). This is critical for aggregations, as it overwrites the gold table with the complete, updated set of aggregated results in each batch.
Specify a new, unique .checkpointLocation for this gold-level stream.
Start the stream. The gold_device_aggregates table will now be continuously updated with the latest aggregations as new data flows into the silver layer.
