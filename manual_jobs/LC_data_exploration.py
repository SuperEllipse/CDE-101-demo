from pyspark.sql import SparkSession
from pyspark.sql import functions as F

## Launching Spark Session

spark = SparkSession\
    .builder\
    .appName("DataExploration")\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1")\
    .config("spark.yarn.access.hadoopFileSystems","s3a://demo-aws-2/")\
    .getOrCreate()

## Creating Spark Dataframe from raw CSV datagov

df = spark.read.option('inferschema','true').csv(
  "s3a://demo-aws-2/data/LendingClub/LoanStats_2015_subset.csv",
  header=True,
  sep=',',
  nullValue='NA'
)

## Printing number of rows and columns:
print('Dataframe Shape')
print((df.count(), len(df.columns)))

## Showing Different Loan Status Values
df.select("loan_status").distinct().show()

## Types of Loan Status Aggregated by Count

print(df.groupBy('loan_status').count().show())
