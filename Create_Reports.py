#
#Copyright (c) 2020 Cloudera, Inc. All rights reserved.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import sys

spark = SparkSession \
    .builder \
    .appName("Pyspark PPP ETL") \
    .getOrCreate()

#A simple script that runs aggregate queries to be used for reporting purposes.
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

print(f"Running report for Jobs Retained by City")

#Delete any reports that were previously run
spark.sql("drop table IF EXISTS texasppp.Jobs_Per_City_Report")
spark.sql("drop table IF EXISTS texasppp.Jobs_Per_Company_Type_Report")

#Create the Jobs Per City Report
cityReport = "create table texasppp.Jobs_Per_City_Report as \
select * from (Select \
  sum(jobsretained) as jobsretained, \
  city \
from \
  texasppp.loan_data \
group by \
  city \
) A order by A.jobsretained desc"

#Run the query to make the new table with the result data
print(f"Running - Jobs Per City Report \n")
spark.sql(cityReport)

#Show the top 10 results
print(f"Results - Jobs Per City Report \n")
cityReportResults = "select * from texasppp.Jobs_Per_City_Report limit 10"
spark.sql(cityReportResults).show()

#Create the Jobs Retained per Company Type Report
companyTypeReport = "create table texasppp.Jobs_Per_Company_Type_Report as \
select * from (Select \
  sum(jobsretained) as jobsretained, \
  businesstype \
from \
  texasppp.loan_data \
group by \
  businesstype \
) A order by A.jobsretained desc"

#Run the query to make the new table with the result data
print(f"Running - Jobs Per Company Type Report \n")
spark.sql(companyTypeReport)

#Show the top 10 results
print(f"Results - Jobs Per Company Type Report \n")
cityReportResults = "select * from texasppp.Jobs_Per_Company_Type_Report limit 10"
spark.sql(cityReportResults).show()