import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date,trim, col, current_timestamp, input_file_name, date_format, unix_timestamp, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


logistics_schema = StructType([
    StructField("Transaction ID", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Product Category", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price per Unit", DoubleType(), True),
    StructField("Total Amount", DoubleType(), True)
])


df_raw = spark.read.csv(
    args['input_path'], 
    schema=logistics_schema, 
    header=True
)

df_standardized = df_raw \
    .withColumnRenamed("Transaction ID", "transaction_id") \
    .withColumnRenamed("Customer ID", "customer_id") \
    .withColumnRenamed("Product Category", "product_category") \
    .withColumnRenamed("Price per Unit", "price_per_unit") \
    .withColumnRenamed("Total Amount", "total_amount") \
    .withColumnRenamed("Gender", "gender") \
    .withColumnRenamed("Age", "age") \
    .withColumnRenamed("Quantity", "quantity")


df_silver = df_standardized \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("batch_id", date_format(col("processing_time"), "yyyy-MM-dd_HH-mm")) \
    .withColumn("load_date", to_date(col("Date"), "yyyy-MM-dd"))

"""print("DIAGNOSTIC: Sample of converted data")
df_silver.select("Date", "partition_date").show(10)

null_count = df_silver.filter(col("partition_date").isNotNull()).count()
print(f"DIAGNOSTIC: Successfully parsed {null_count} rows.")"""

df_silver.write.format("delta") \
    .mode("append") \
    .partitionBy("batch_id") \
    .save(args['output_path'])

job.commit()
print(f"Job {args['JOB_NAME']} completed successfully. Silver layer updated at {args['output_path']}")