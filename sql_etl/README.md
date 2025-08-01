# SQL ETL Pipeline for UM_Terrorism_DW

This pipeline performs a full star schema ETL for the terrorism data warehouse project using T-SQL.

---

## 📂 File: `full_etl_pipeline.sql`

This script includes:

- Logging setup (`ETLLog` table, view, and procedure)
- Foreign key drop and re-add procedures
- View definitions for each dimension
- Insert procedures for each `Dim` table
- Insert procedure for `Fact_Terror_Events`
- Final execution block for running all steps
- Logs each action to `vETLLog`

---

## ▶️ How to Run

1. Open SSMS or Azure Data Studio  
2. Connect to your `UM_Terrorism_DW` database  
3. Run the script section-by-section, or as one file:

```sql
USE UM_Terrorism_DW;
GO

:r ./sql_etl/full_etl_pipeline.sql
