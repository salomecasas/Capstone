

# Setup by importing the necessary modules
import pyspark
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd
import Celia_PW


# New variable to reference the HIDDEN UserName and Password
# in the imported file
Uname = Celia_PW.Hidden_UserName
Upassword = Celia_PW.Hidden_Password


# Setup the Spark Session 
# spark = SparkSession.builder.master('local[1]').appName('demo.com').getOrCreate()
spark = SparkSession.builder.appName('capstone.com').getOrCreate()


# Read/Load data from the three RDBMS tables in the creditcard_capstone MySQL database
# Utilized Apache Spark to read the MySQL named tables. The .load method triggers the actual loading of data

# Read the CUSTOMER data from the RDBMS table and load into the df_customer, DataFrame
df_customer = spark.read.format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("user", Uname) \
    .option("password", Upassword) \
    .option("dbtable", "cdw_sapp_customer") \
    .load()

# Read the CREDIT CARD data from the RDBMS table and load into the df_credit, DataFrame:
df_credit = spark.read.format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("user", Uname) \
    .option("password", Upassword) \
    .option("dbtable", "cdw_sapp_credit_card") \
    .load()

# Read the BRANCH data from the RDBMS table and load into the df_branch, DataFrame:
df_branch = spark.read.format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("user", Uname) \
    .option("password", Upassword) \
    .option("dbtable", "cdw_sapp_branch") \
    .load()



### 3.3.1 ### - Functional Requirement
# Transaction Type that has the highest transaction count, find and plot

# Register the DataFrame as a temporary in memory view in Spark
# Using the createOrReplaceTempView method on the Apache Spark DataFrame (PySpark API) object
# Queries can now be performed on df_credit using SparkSQL leveraging the view as a table
df_credit.createOrReplaceTempView("cdw_sapp_credit_card")


# Create the SQL query to select the necessary data
query = """
SELECT TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT
FROM cdw_sapp_credit_card
GROUP BY TRANSACTION_TYPE
ORDER BY TRANSACTION_COUNT DESC
"""

# The query to select the desired dataset is now run using the previously created view table and Spark.sql
# Result dataset/table is now in the new variable transaction_type_counts
transaction_type_counts = spark.sql(query)

# Query results returned in MySQL workbench:
#TRANSACTION_TYPE TRANSACTION_COUNT
#Bills	          6861
#Healthcare	      6723
#Test	          6683
#Education	      6638
#Entertainment	  6635
#Gas	          6605
#Grocery	      6549

# Collecting results as a "LIST" to the driver (be cautious with large datasets) into results/iterable
# Each interation of the results variable is a row in the sql workbench query
results = transaction_type_counts.collect()

# Extract data for plotting graph
# Iterate throught the iterable object "results" twice: 
# Once to get "types" total and once to get the "counts" total for the visualization
types = [result['TRANSACTION_TYPE'] for result in results]
counts = [result['TRANSACTION_COUNT'] for result in results]

# Plot the results using pyplot
plt.figure(figsize=(12,6))
plt.barh(types, counts, color='skyblue')
plt.xlabel('Transaction Count')
plt.ylabel('Transaction Type')
plt.title('Transaction Type vs Transaction Count')
plt.gca().invert_yaxis()  # Highest count at the top
plt.show()



### 3.3.2 ### - Functional Requirement
# State with the highest number of customers, find and plot
# count is a builtin dataframe function 
from pyspark.sql.functions import count

# Created a new dataframe that only has the two grouped columns needed for the visualization
# Use the previouly created df_customer dataframe to prepare data for graphing (transforming the RDD)
df_state_counts = df_customer.groupBy("CUST_STATE").agg(count("*").alias("NUM_CUSTOMERS"))

# Optionally, sort the data
df_state_counts = df_state_counts.sort("NUM_CUSTOMERS", ascending=False)

# Convert to Pandas DataFrame for plotting (.toPandas is an action on an RDD)
pdf_state_counts = df_state_counts.toPandas()
pdf_state_counts.plot.bar(x="CUST_STATE", y="NUM_CUSTOMERS")
plt.xlabel('State')
plt.xticks(rotation=0, fontsize=8)
plt.ylabel('Number of Customers')
plt.title('Number of Customers per State')
plt.legend(["Customers"], loc="upper right")
plt.show()
# Hide the default pandas-generated legend
# ax.get_legend().remove()



### 3.3.3 ### - Functional Requirement
# The sum of all transactions for the top 10 customers
# and which customer has the highest transaction amount, find and plot

# Register the DataFrame as a temporary in memory view in Spark
# Using the createOrReplaceTempView method on the Apache Spark DataFrame (PySpark API) object
# Queries can now be performed on df_credit using SparkSQL leveraging the view as a table
df_customer.createOrReplaceTempView("cdw_sapp_customer")


# Create the SQL query to select the necessary data
query = """
    SELECT cust.SSN, cust.FIRST_NAME, cust.LAST_NAME, 
           SUM(credit.TRANSACTION_VALUE) AS TOTAL_TRANSACTIONS
    FROM cdw_sapp_customer AS cust
    JOIN cdw_sapp_credit_card AS credit
    ON cust.SSN = credit.CUST_SSN
    GROUP BY cust.SSN, cust.FIRST_NAME, cust.LAST_NAME
    ORDER BY TOTAL_TRANSACTIONS DESC
    LIMIT 10
"""

# The query to select the desired dataset is now run using the previously created view table and Spark.sql
# Result dataset/table is now in the new variable df_top_customers
# Query returns in MySQL workbench
df_top_customers = spark.sql(query)

# Convert to Pandas DataFrame for plotting
pdf_top_customers = df_top_customers.toPandas()
pdf_top_customers['CUSTOMER'] = pdf_top_customers['FIRST_NAME'] + ' ' + pdf_top_customers['LAST_NAME']
plt.barh(pdf_top_customers["CUSTOMER"], pdf_top_customers["TOTAL_TRANSACTIONS"], color='blue')

# Adding labels and title
# The tight_layout() function is used to ensure that labels and ticks are displayed clearly without overlapping, 
# and it’s especially useful when the x-axis labels are rotated
plt.ylabel('Customer')
plt.xlabel('Total Transaction Value')
plt.title('Top 10 Customers by Transaction Value')
plt.xticks(rotation=0, fontsize=8)
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()


# Query results returned in MySQL workbench:
# SSN       FIRST_NAME   LAST_NAME  TOTAL_TRANSACTIONS
# 123451125	Ty	         Daly	    5633.0700000000015
# 123452783	Jessie	     Boucher	5548.140000000001
# 123453486	Phoebe	     Martin	    5476.079999999999
# 123458668	Thurman	     Vera	    5314.970000000003
# 123456678	Bret	     Perkins	5261.030000000001
# 123452026	Joesph	     Mcclain	5241.18
# 123452518	Aurelia	     Haas	    5223.959999999999
# 123454933	Marcelo	     Emerson	5203.0800 
# 123457547	Alexis	     Villarreal	5149.75
# 123452085	Christina	 Snow	    5133.290000000002

spark.stop()