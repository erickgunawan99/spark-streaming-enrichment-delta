**PySpark Structured Streaming: Stateful Deduplication & Enrichment**
**Project Overview** :
This project implements a real-time ingestion and enrichment pipeline for stock trading data. The core challenge addressed is the "Multiplication Problem" inherent in standard stream-stream joins, where multiple updates for the same key within a micro-batch result in duplicate output rows.

By leveraging Stateful Processing (applyInPandasWithState) combined with a Union-based architecture, this pipeline ensures that only the most recent state of a record is used for enrichment and final persistence.

**Workflow**
The pipeline follows a specialized "Union & State" pattern rather than a direct join to maintain a "Single Source of Truth" per key.
1. Data Ingestion
   + Trade Stream (stock-trades) : High-frequency raw trading data (Symbol, Price, Volume).
   + Reference Stream (stock-info) : Metadata or enrichment updates (Symbol, Company Name, Sector).
2. Stateful Union
   Instead of joining the streams immediately, we Union the two streams into a single DataFrame. We add a type column to distinguish between "Trade" and "Reference" data.
3. Deduplication via applyInPandasWithState
   We group the unioned stream by symbol. The stateful function performs the following:
   + State Storage: Maintains a persistent state in rocksDB (backed by a Checkpoint) for every symbol. It acts as the seed for the beginning of micro-batch enrichment.
   + Sort: set the unioned DF to be sorted by event_timestamp
   + Pandas ffill: Once the data is sorted by timestamp, we have a chronological sequence of rows. However, some rows are "Type: Reference" (containing company info) and some are "Type: Trade" (containing price). We use the Pandas Forward Fill (ffill) operation on the stateful DataFrame:
        + The Mechanism: Sorting puts the last known "Reference" row immediately before the "Trade" rows that follow it in time.
        + The Fill: ffill() carries the "Company" and "Sector" values forward from the reference row into the empty columns of the subsequent trade rows.
        + The Guarantee: Even if a Kafka partition was delayed, once the micro-batch is sorted, the trade "looks back" at the most recent reference data available in that timeline.
   + Output DF: We added column type for both stream DF ("Trade" and "Reference") before. The purpose is so that now we can drop the "Reference" (enrichment info) rows out of this Union DF.
   + Updating state: Update state only when the latest reference timestamp is greater that the reference timestamp that's currently in the state
4. Output to Delta and Postgres
   + Delta Lake: Serves as the high-scale storage layer.
   + Postgres: Serves as the serving layer for the Streamlit dashboard. Running separate pyspark job to incrementally write the raw delta table and aggregation to postgres tables

**Why use union instead of standard join**
In a standard Spark Stream-Stream Join, even when you set the stateful function output to update, the join buffer will keep incrementally append the reference for each symbol every micro-batch.
In result, Spark will join each trade row with possibly many reference rows post batch 0 and emit many duplicate trade rows. This results in inflated metrics (e.g., tripled trade counts). Deduplication on the foreachbatch is another possible option to solve this, but that would happen after the exploding cartesian join which seems inefficient

**Delta Maintenance**
Streaming writes to Delta Lake create many small files (one per micro-batch). A separate maintenance script runs OPTIMIZE and VACUUM to:
  + Combine small 2KB-3KB files into larger, query-efficient Parquet files.
  + Physical deletion of "tombstoned" files after the retention period.

**Summary: Key Concepts**
1. Engine: Pyspark Structured Streaming (3.x)
2. Stateful Function: applyInPandasWithState
3. State Store: RocksDB
4. Deduplication and enrichment strategy: Union both streams -> order by timestamp -> pandas ffill
5. Storage: Delta Lake (ACID Transactions on HDFS/Local).
6. Database: Postgres
7. Visualization: Streamlit + Plotly
