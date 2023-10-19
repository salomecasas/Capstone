# Setup - Load the database via a Spark Session and sparkSQL
# Previously CREATED the "creditcard_capstone" DATABASE directly within MySQL Workbench


# Import data types and classes from the PySpark library and then the "pyspark.sql.types" module
# these are used to define the schema of structured data when working with DataFrames in PySpark
# Importing these classes and data types allows the creation of a schema form the data in a PySpark DataFrame.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, TimestampType
# from pyspark.sql.functions import when, col, expr, concat, lit, lpad
# from pyspark.sql.functions import when, col, expr, concat, lit, lpad, lower, initcap, concat_ws
# from pyspark.sql import functions as F

import Celia_PW
# Set to another variable TO REPLACE THE HARDCODED user AND password
# New variable set to = Celia_PW.password
Uname = Celia_PW.Hidden_UserName
Upassword = Celia_PW.Hidden_Password

# Setting up the spark session
import pyspark
from pyspark.sql import SparkSession #Only need one session - Importing the class object SparkSession

#### Create an active SparkSession - to be able to utilize "Spark Dataframes" (not RDBMS/previously created) ####
spark = SparkSession.builder.appName('capstone').getOrCreate() # .builder is the method in the session

# Do this at the end of the program - It is good practice to close/stop a session
# spark.stop() - really want to stop on the variable previously created at the end
# spark = SparkSession.stop


### Use/Need the SparkContext for RDDs
# User interface for active sessions
# The SparkContext provides methods for creating RDDs, accumulators, 
# and broadcasting variables, as well as methods for starting tasks on the executors.
# The SparkSession does not provide theses methods, but it does provide methods for creating DataFrames and Datasets,
# as well as methods fo rreading and writing data.

# from pyspark import SparkContext
###


## BRANCH ###
# Setup to read the json file
# Both the read and json are methods on the SparkSession object
#df = spark.read.json("Data\cdw_sapp_branch.json") 


#Display data etc. to ensure a sucessful load of json file
#df.show()
#df.printSchema()
#df.describe().show()
#df.describe()
#df.columns

# Code to check if the SparkSession is up and running and Code to stop the SparkSession
#SparkSession.getActiveSession() # this will display my active sesson in "local host 4040"
# http://localhost:4040/jobs/
# spark = SparkSession.stop




## BRANCH ###
# Setup to read the json file
# Both the read and json are methods on the SparkSession object
#df = spark.read.json("Data\cdw_sapp_branch.json")

# Load Branch schema needed in RDBMS
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType, DoubleType,TimestampType
schema = StructType([
      StructField("BRANCH_CODE",IntegerType(),True),
      StructField("BRANCH_NAME",StringType(),True),
      StructField("BRANCH_STREET",StringType(),True),
      StructField("BRANCH_CITY",StringType(),True),
      StructField("BRANCH_STATE",StringType(),True),
      StructField("BRANCH_ZIP",IntegerType(),True),
      StructField("BRANCH_PHONE",StringType(),True),
      StructField("LAST_UPDATED",TimestampType(),True),
  ])


# Read the json file 
df_with_schema = spark.read.schema(schema).json("Data\cdw_sapp_branch.json")


# Checking the zipcode - if null - transform to 99999
# Adds the NEW value for "BRANCH_ZIP" to the existing df2 dataframe via .withColumn
from pyspark.sql.functions import when, col, expr, concat, lit
df2= df_with_schema.withColumn("BRANCH_ZIP", when(col("BRANCH_ZIP").isNull(), 99999).otherwise(col("BRANCH_ZIP")))

 
# PySpark #
# Adds a new column "BRANCH_PHONE" to the existing df2 dataframe via .withColumn
# Transforming the json file branch phone: from 1234567890 to (123)456-7890
# Used the concat function to create the final format 
# lit - used to add in the ( ) and -
# substr to find the necessary phone number groups
# Assigns to a new df3 dataframe
df3 = df2.withColumn(
    "BRANCH_PHONE",
    concat(
        lit("("),
        col("BRANCH_PHONE").substr(1, 3), lit(")"),
        col("BRANCH_PHONE").substr(4, 3), lit("-"),
        col("BRANCH_PHONE").substr(7, 4)
    )
)


# Create the "Branch" table in the RDBMS and load the table with the transformed data
df3.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "CDW_SAPP_BRANCH") \
  .option("user", Uname) \
  .option("password", Upassword) \
  .save()



### CREDIT CARD ###
# Setup to read the json file
# Both the read and json are methods on the SparkSession object
#df = spark.read.json("Data\cdw_sapp_credit.json")


# Load Credit schema needed in RDBMS
schema = StructType([
      StructField("TRANSACTION_ID",IntegerType(),True),
      StructField("BRANCH_CODE",IntegerType(),True),
      StructField("CREDIT_CARD_NO",StringType(),True),
      StructField("CUST_SSN",IntegerType(),True), 
      StructField("DAY",StringType(),True),
      StructField("MONTH",StringType(),True),
      StructField("YEAR",StringType(),True),
      StructField("TRANSACTION_TYPE",StringType(),True),
      StructField("TRANSACTION_VALUE",DoubleType(),True)   
  ])


# Read the json file 
df_with_schema = spark.read.schema(schema).json("Data\cdw_sapp_credit.json")


#pad the left column below with up to the length of 2 ie from 2 to 02 but if already 10 ie 2 digits does nothing
from pyspark.sql.functions import when, col, expr, concat, lit, lpad

df2= df_with_schema.withColumn("CUST_CC_NO", col("CREDIT_CARD_NO"))

df2 = df2.withColumn(
    "TIMEID",
    concat(
        col("YEAR"),
        lpad(col("MONTH"), 2, "0"),
        lpad(col("DAY"), 2, "0")
    )
)

# Don't need the "old" json column(s) / used transformed data and data mapping names
df2 = df2.drop("DAY", "MONTH", "YEAR", "CREDIT_CARD_NO")

# Create the "Credit Card" table in the RDBMS and load the table with the transformed data
df2.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
  .option("user", Uname) \
  .option("password", Upassword) \
  .save()



## Customer ###
# Setup to read the json file
# Both the read and json are methods on the SparkSession object
#df = spark.read.json("Data\cdw_sapp_custmer.json")

# Load Customer schema needed in RDBMS
schema = StructType([
      StructField("SSN",IntegerType(),True),
      StructField("FIRST_NAME",StringType(),True),
      StructField("MIDDLE_NAME",StringType(),True),
      StructField("LAST_NAME",StringType(),True), 
      StructField("CREDIT_CARD_NO",StringType(),True),
      StructField("STREET_NAME",StringType(),True),
      StructField("APT_NO",StringType(),True),
      StructField("CUST_CITY",StringType(),True),
      StructField("CUST_STATE",StringType(),True),
      StructField("CUST_COUNTRY",StringType(),True), 
      StructField("CUST_ZIP",StringType(),True),
      StructField("CUST_EMAIL",StringType(),True),
      StructField("LAST_UPDATED",TimestampType(),True),
      StructField("CUST_PHONE",StringType(),True)
  ])


# Read the json file 
df = spark.read.schema(schema).json("Data\cdw_sapp_custmer.json")

# Changing the zipcode if null to transform the data
# Pad the left column below with up to the length of 2 ie from 2 to 02 but if already 10 ie 2 digits does nothing
from pyspark.sql.functions import when, col, expr, concat, lit, lpad, lower, initcap, concat_ws
from pyspark.sql import functions as F


# Adding an area code - every record in the json data was missing an area code
# to the old column branch phone, used concat to match the spec, grab from 1 to 3, then grab from 4 for the next three
# Using the apache spark concat
df = df.withColumn(
    "CUST_PHONE",
    concat(
        lit("(206)"),
        col("CUST_PHONE").substr(1, 3), lit("-"),
        col("CUST_PHONE").substr(4, 7))
)


# Modifying the Name(s) to comply with the mapping document specification
df = df.withColumn("FIRST_NAME", initcap(df["FIRST_NAME"]))
df = df.withColumn("MIDDLE_NAME", lower(df["MIDDLE_NAME"]))
df = df.withColumn("LAST_NAME", initcap(df["LAST_NAME"]))


# Modifying the Street/Apt. in json file to Full Street Address to comply with the mapping document specification
df = df.withColumn("FULL_STREET_ADDRESS", concat_ws(", ", df["STREET_NAME"], df["APT_NO"]))


# If the zip code is less than 5 pad with a zero
df = df.withColumn("CUST_ZIP", F.lpad(df["CUST_ZIP"], 5, '0'))
#df = df.withColumn("CUST_ZIP", df["CUST_ZIP"].cast(IntegerType()))


#No longer need these columns as they were concationated above
df = df.drop("STREET_NAME", "APT_NO")


# Create the "Customer" table in the RDBMS and load the table with the transformed data
df.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "CDW_SAPP_CUSTOMER") \
  .option("user", Uname) \
  .option("password", Upassword) \
  .save()

spark.stop()
