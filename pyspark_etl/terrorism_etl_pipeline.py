# Set Access Key (secure in production)
spark.conf.set(
  "fs.azure.account.key.gtdstorage20250707js.blob.core.windows.net",
  "<YOUR_AZURE_BLOB_KEY>"
)

file_path = "wasbs://raw@gtdstorage20250707js.blob.core.windows.net/globalterrorismdb_integrity_final.csv"

# Load CSV
df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load(file_path)

df.cache()
df.printSchema()

# Write to Azure SQL
df.write \
  .format("jdbc") \
  .option("url", "jdbc:sqlserver://<SQL_SERVER_NAME>.database.windows.net:1433;database=UM_Terrorism_DW") \
  .option("dbtable", "FactTerrorEvents") \
  .option("user", "<SQL_USERNAME>") \
  .option("password", "<SQL_PASSWORD>") \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .mode("overwrite") \
  .save()
