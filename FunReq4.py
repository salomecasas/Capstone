

import requests
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"

import Celia_PW
# Set to another variable TO REPLACE THE HARDCODED user AND password
# New variable set to = Celia_PW.password
Uname = Celia_PW.Hidden_UserName
Upassword = Celia_PW.Hidden_Password

# Send GET request
response = requests.get(url)
print(f"Status Code: {response.status_code}")

# Check if request was successful
if response.status_code == 200:
    loan_data = response.json()
else:
    print("Failed to retrieve data")
    raise


# Create a Spark session
spark = SparkSession.builder.appName("LoanDataIngestion").getOrCreate()


# used for testing to view data etc.
# spark.createDataFrame(loan_data)
# df = spark.createDataFrame(loan_data)
# df.show()


# Define schema - 
schema = StructType([
    # Define your fields here according to the API response
    StructField("Application_ID", StringType(), True),
    StructField("Application_Status", StringType(), True),
    StructField("Credit_History", IntegerType(), True),
    StructField("Dependents", StringType(), True),
    StructField("Education", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Income", StringType(), True),
    StructField("Married", StringType(), True),
    StructField("Property_Area", StringType(), True),
    StructField("Self_Employed", StringType(), True)
])

# Convert JSON data to DataFrame
loan_df = spark.createDataFrame(loan_data, schema=schema)

loan_df.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "CDW_SAPP_LOAN_APPLICATION") \
  .option("user", Uname) \
  .option("password", Upassword) \
  .save()
spark.stop()