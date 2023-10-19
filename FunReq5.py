
# Setup by importing the necessary modules
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, substring
import matplotlib.pyplot as plt
import Celia_PW


# New variable to reference the HIDDEN UserName and Password
# in the imported file
Uname = Celia_PW.Hidden_UserName
Upassword = Celia_PW.Hidden_Password


# Setup the Spark Session 
# spark = SparkSession.builder.master('local[1]').appName('loan_app_analysis').getOrCreate()
spark = SparkSession.builder.appName('loan_app_analysis').getOrCreate()

# Read/Load data from the RDBMS table in the creditcard_capstone MySQL database
# Utilized Apache Spark to read the MySQL named tables. The .load method triggers the actual loading of data

# Read the CUSTOMER data from the RDBMS table and load into the df_customer, DataFrame
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

# Read the BRANCH data from the RDBMS table and load into the df_branch, DataFrame:
df_loan_application= spark.read.format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("user", Uname) \
    .option("password", Upassword) \
    .option("dbtable", "cdw_sapp_loan_application") \
    .load()

# Assuming you have read your loan_application dataframe as df_loan_application.
# df_loan_application = spark.read...





"using Appache Spark RDD operations"
# 5.1 - Functional Requirements
# Calculate the proportions
total_self_employed = df_loan_application.filter(col("Self_Employed") == "Yes").count()

num_yes = df_loan_application.filter(
    (col("Self_Employed") == "Yes") & (col("Application_Status") == "Y")
).count()

num_no = df_loan_application.filter(
    (col("Self_Employed") == "Yes") & (col("Application_Status") == "N")
).count()

prop_yes = (num_yes / total_self_employed) * 100
prop_no = (num_no / total_self_employed) * 100

# Printing the results
print(f"{round(prop_yes, 3)}% of applications are approved for self-employed applicants")
print(f"{round(prop_no, 3)}% of applications are denied for self-employed applicants")

# Plotting
x = ["Approved", "Denied"]
y = [prop_yes, prop_no]

plt.bar(x=x, height=y)
plt.xticks(fontsize=14)
plt.ylabel("Percent", rotation=90, labelpad=0, fontsize=14)
plt.title("Self-Employed Applicants", fontsize=16)
# plt.savefig("FILE_NAME.png")
plt.show()



# 5.2 - Functional Requirements
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder \
    .master('local[1]') \
    .appName('LoanApplications') \
    .getOrCreate()

# Assuming df_loan_app is your DataFrame loaded into Spark
# df_loan_app = spark.read...

# Count of married men
num_married_men = df_loan_application.filter(
    (col("Married") == "Yes") & (col("Gender") == "Male")
).count()

# Count of approved married men
num_approved_married_men = df_loan_application.filter(
    (col("Married") == "Yes") & (col("Gender") == "Male") & (col("Application_Status") == "Y")
).count()

# Calculate the proportions
prop_yes = (num_approved_married_men / num_married_men) if num_married_men != 0 else 0
prop_no = 1 - prop_yes

# Printing the proportions
print(f"{prop_yes * 100:.2f}% of applications by married men are accepted")
print(f"{prop_no * 100:.2f}% of applications by married men are rejected")

# Plotting
x = ["Accepted", "Rejected"]
y = [prop_yes * 100, prop_no * 100]

plt.bar(x=x, height=y)
plt.xticks(fontsize=14)
plt.ylabel("Percent", rotation=90, labelpad=0, fontsize=14)
plt.title("Married Men Applicants", fontsize=16)
# plt.savefig("FILE_NAME.png")
plt.show()


# 5.3 - Functional Requirements
# Extract month and year from TIMEID column
df_grouped = df_credit.withColumn("YEAR", substring(col("TIMEID"), 1, 4)) \
                           .withColumn("MONTH", substring(col("TIMEID"), 5, 2))

# Group by YEAR and MONTH, and count TRANSACTION_ID
df_aggregated = df_grouped.groupBy("YEAR", "MONTH") \
                          .agg(count("TRANSACTION_ID").alias("COUNT_TRANSACTIONS")) \
                          .orderBy(col("COUNT_TRANSACTIONS").desc())

# Get top 3 results
df_top3 = df_aggregated.limit(3)

# Convert the result to pandas for plotting
pdf_top3 = df_top3.toPandas()

# Plotting
pdf_top3['YearMonth'] = pdf_top3['YEAR'] + '-' + pdf_top3['MONTH']
pdf_top3.plot(kind='bar', x='YearMonth', y='COUNT_TRANSACTIONS', legend=None)
plt.title('Top 3 Months with the Highest Transaction Counts')
plt.ylabel('Number of Transactions')
plt.xlabel('Month-Year')
plt.xticks(rotation=45)
plt.tight_layout()

plt.show()





# 5.4
# Join, filter for healthcare, group by branch, and aggregate
df_aggregated = df_credit.join(df_branch, "BRANCH_CODE") \
                              .filter(df_credit["TRANSACTION_TYPE"] == "HEALTHCARE") \
                              .groupBy("BRANCH_NAME") \
                              .agg(sum("TRANSACTION_VALUE").alias("TOTAL_HEALTHCARE_VALUE")) \
                              .orderBy("TOTAL_HEALTHCARE_VALUE", ascending=False)

# Convert the result to pandas for plotting
pdf_aggregated = df_aggregated.toPandas()

# Plotting
pdf_aggregated.plot(kind='bar', x='BRANCH_NAME', y='TOTAL_HEALTHCARE_VALUE', legend=None)
plt.title('Total Healthcare Transaction Value by Branch')
plt.ylabel('Total Dollar Value')
plt.xlabel('Branch Name')
plt.tight_layout()
plt.xticks(rotation=0)
plt.show()

spark.stop()
