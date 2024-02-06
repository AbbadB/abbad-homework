# Growth Data Pipelines Runbook

## Group

V3 Combined America/New_York 6

- Bruno Abbad (Analytics Engineer)
- Ilich Briceno (Data Engineer)

## Pipeline Owners

| Pipeline                         | Primary Owner    | Secondary Owner  |
|----------------------------------|------------------|------------------|
| Aggregate Growth (Investors)     | Bruno Abbad      | Ilich Briceno    |
| Daily Growth (Experiments)       | Ilich Briceno    | Bruno Abbad      |
| Aggregate Profit (Investors)     | Bruno Abbad      | Ilich Briceno    |
| Unit-level Profit (Experiments)  | Ilich Briceno    | Bruno Abbad      |
| Aggregate engagement (Investors) | Bruno Abbad      | Ilich Briceno    |

## On-Call Rotation

| Pipeline             | On-Call Member | Notes              |
|----------------------|----------------|--------------------|
| Aggregate Growth     | Bruno Abbad    |                    |
| Daily Growth         | Ilich Briceno  |                    |
| Aggregate Profit     | Bruno Abbad    |                    |
| Aggregate Profit     | Ilich Briceno  | Holiday adjustment |
| Unit-level Profit    | Ilich Briceno  |                    |
| Unit-level Profit    | Bruno Abbad    | Holiday adjustment |
| Aggregate engagement | Bruno Abbad    |                    |

## Run books

| Pipeline                             | Potential Problem               | Action Step                                                                 | Notification Required          |
|--------------------------------------|---------------------------------|-----------------------------------------------------------------------------|--------------------------------|
| **Aggregate Growth (Investors)**     | Data Quality Issue              | 1. Verify data integrity at source. <br>2. Rerun ETL jobs if data is corrected. | Bruno to notify Data Quality Team. |
|                                      | Pipeline Failure                | 1. Check error logs.<br>2. Resolve code/configuration issues.<br>3. Restart pipeline. | Ilich to notify Engineering Manager.|
|                                      | Slowness in Data Processing     | 1. Analyze processing bottlenecks.<br>2. Scale up resources if needed.<br>3. Optimize queries. | Bruno to notify if delay > X hours. |
|                                      | Minimum Rows on the data Ingestion | 1. Verify data sources for completeness.<br>2. Investigate reasons for low row count.<br>3. Adjust data ingestion process if needed.<br>4. Implement a solution to ensure minimum rows are met.<br>5. Monitor data ingestion regularly for recurrence. | Ilich to analyze if there is any problem with the data ingestion |
| **Daily Growth (Experiments)**       | Delay in Data Availability      | 1. Check upstream data sources.<br>2. Communicate with upstream teams for ETA.<br>3. Adjust schedules accordingly. | Ilich to notify Experiment Owners. |
|                                      | Incorrect Data Transformation   | 1. Review transformation logic.<br>2. Correct any errors and rerun.<br>3. Validate output with stakeholders. | Bruno to notify Data Analysts. |
|                                      | External API Downtime           | 1. Check API status page.<br>2. Implement retry logic or switch to backup if available.<br>3. Communicate ETA to stakeholders. | Ilich to notify External Partners. |
| **Aggregate Profit (Investors)**     | Data Sync Issues                | 1. Verify last successful data sync.<br>2. Manually trigger sync if necessary.<br>3. Check for errors and resolve. | Bruno to notify Finance Team.      |
|                                      | Financial Data Mismatch         | 1. Cross-verify with source financial systems.<br>2. Correct any discrepancies.<br>3. Reconcile and rerun pipeline. | Ilich to notify Financial Analysts.|
| **Unit-level Profit (Experiments)**  | Incorrect Profit Calculations   | 1. Validate calculation logic against business rules.<br>2. Correct any errors in the logic.<br>3. Rerun calculations and validate. | Bruno to notify Experiment Owners. |
|                                      | Missing Cost Data               | 1. Identify missing cost components.<br>2. Liaise with Finance to obtain data.<br>3. Update pipeline and rerun. | Ilich to notify Finance. |
|                                      | Performance Degradation         | 1. Profile pipeline to identify bottlenecks.<br>2. Optimize performance-critical sections.<br>3. Monitor changes for improvement. | Bruno to notify if significant impact. |
| **Aggregate Engagement (Investors)** | Inconsistent Engagement Metrics | 1. Audit data collection methods for engagement metrics.<br>2. Validate aggregation logic against expected outcomes.<br>3. Correct any discrepancies and rerun the pipeline. | Ilich to notify Product and Analytics Teams. |
|                                      | User Segmentation Error         | 1. Review segmentation criteria for accuracy.<br>2. Adjust criteria and resegment data if necessary.<br>3. Validate changes with stakeholders. | Bruno to notify Marketing. |
|                                      | Slow Reporting Dashboard        | 1. Identify bottlenecks in the dashboard query or data model.<br>2. Optimize for performance.<br>3. Test and deploy optimizations. | Ilich to notify BI Team. |