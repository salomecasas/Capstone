
# Necessary imports
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import mysql.connector
import contextlib
import sys
import os
import Celia_PW


#Put at top of code before running anything else
import warnings

# Suppress Python warnings - Ignore all warnings
warnings.filterwarnings("ignore")

# To reset the warning behavior to the default, you can use "default" or None as the argument:
# warnings.filterwarnings("default")

# Created Celia_PW.py
# import Celia_PW  -- Need to import to be able to use
# Celia_PW.Hidden_UserName   -- Use this in place of username
# Celia_PW.Hidden_Password   -- Use this in place of password

# Variables created in the Celia_PW.py file for username and password
# Hidden_UserName
# Hidden_Password

# To reference the variable values set in Celia_PW.py 
# Celia_PW.Hidden_UserName
# Celia_PW.Hidden_Password

# Set to another variable TO REPLACE THE HARDCODED user AND password
# New variable set to = Celia_PW.password
Uname = Celia_PW.Hidden_UserName
Upassword = Celia_PW.Hidden_Password

# ************** ASK BEN ABOUT THIS PIECE OF CODE **************
# spark = SparkSession.builder.appName('capstone').getOrCreate() #.builder is the method in the session 
spark =  SparkSession.builder.appName('capstone').getOrCreate()
# spark = SparkSession.builder.master('local[1]').appName('demo.com').getOrCreate()

# Set log level to ERROR to only see error log messages
spark.sparkContext.setLogLevel('ERROR')

# Read data from the customer table
df_customer = spark.read.format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("user", Uname) \
    .option("password", Upassword) \
    .option("dbtable", "cdw_sapp_customer") \
    .load()

# Read data from the credit card table
df_credit = spark.read.format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("user", Uname) \
    .option("password", Upassword) \
    .option("dbtable", "cdw_sapp_credit_card") \
    .load()

# Read data from the branch table
df_branch = spark.read.format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("user", Uname) \
    .option("password", Upassword) \
    .option("dbtable", "cdw_sapp_branch") \
    .load()

# Register the DataFrames as temporary views in Spark
df_customer.createOrReplaceTempView("cdw_sapp_customer")
df_credit.createOrReplaceTempView("cdw_sapp_credit_card")
df_branch.createOrReplaceTempView("cdw_sapp_branch")


done=False
while not done:
    # Need to pick between Transaction Details and the customer Details
    select=input("\n\n\n\nChoose which details you would like to see\n\t1-Transaction\n\t2-Customer\n")

    if select=="1": # Transaction Details was selected / Now choose 1, 2, or 3 
        option=input("\n\n\n\nSelect the option you would like to run\n\t1-Display the transactions made by " 
                    "customers living in a given zip code for a given month and year\n\t2-Display the " 
                    "number and total values of transactions for a given type\n\t3-Display the total "
                    "number and total values of transactions for branches in a given state\n")
        if option=="1":
            # Need the zipcode, month, and year
            zip_code=input("\n\n\n\nPlease enter the zip code\n")
            month=input("\n\n\n\nPlease enter the month as MM\n") 
            year=input("\n\n\n\nPlease enter the year as YYYY\n")
            query=f'''
                SELECT CUST.CUST_ZIP, CREDIT.TIMEID,
                SUBSTRING(CREDIT.TIMEID, 1, 4) AS YEAR,  
                SUBSTRING(CREDIT.TIMEID, 5, 2) AS MONTH, 
                SUBSTRING(CREDIT.TIMEID, 7, 2) AS DAY,   
                CREDIT.TRANSACTION_ID AS TRANS_ID,
                CREDIT.TRANSACTION_TYPE AS TRANS_TYPE,
                CREDIT.TRANSACTION_VALUE AS TRANS_VALUE
                FROM cdw_sapp_customer AS CUST
                INNER JOIN cdw_sapp_credit_card AS CREDIT
                ON CUST.SSN = CREDIT.cust_SSN
                WHERE CUST.CUST_ZIP = '{zip_code}' AND SUBSTRING(CREDIT.TIMEID, 1, 6) = '{year}{month}'  
                ORDER BY DAY DESC;
            '''
        
            # Return the query results into the dataframe
            df_result = spark.sql(query)

            # Show All of the result
            df_result.show(df_result.count())
            # df_result.show() - only shows the top 20 return results

            # Show all transactions made by all customers in this zip code in this month in this year
            # Order by day in descending order
        elif option=="2":
            # Need the type of transaction
            i=input("\n\n\n\nPlease enter the transaction type number from the list below"
                                "\n\t1-Grocery"
                                "\n\t2-Education"
                                "\n\t3-Bills"
                                "\n\t4-Gas"
                                "\n\t5-Entertainment"
                                "\n\t6-Test"
                                "\n\t7-Healthcare\n")
            done=False # For the while loop - waiting for a vaild number 1-7
            while not done:
                # Check if input is valid using a list comprehension []
                # Iterate through each element in 'x' in the range / converted to a string
                if i in [str(x) for x in range(1,8)]:
                #if so, then done
                    done=True
                else:
                #not valid (note that done will still be set to False)
                    i=input("\n\n\n\nPlease enter the transaction type from the list below"
                                "\n\t1-Grocery"
                                "\n\t2-Education"
                                "\n\t3-Bills"
                                "\n\t4-Gas"
                                "\n\t5-Entertainment"
                                "\n\t6-Test"
                                "\n\t7-Healthcare\n")
            # Use the input number i 1=7 to index the list and find out what 'transaction_type' the user entered       
            transaction_type=["Grocery","Education","Bills","Gas","Entertainment","Test","Healthcare"][int(i)-1]

            #show the total number and total value of these transaction types
            query=f'''
                SELECT  COUNT(*) AS Number_of_Transactions, Transaction_Type, 
                CONCAT('$', format_number(sum(TRANSACTION_value), 2)) AS Total_Dollar_Value
                FROM cdw_sapp_credit_card
                WHERE TRANSACTION_TYPE = '{transaction_type}'
                group by TRANSACTION_TYPE
            '''
            # Return the query results into the dataframe
            df_result = spark.sql(query)

            # Show the result
            df_result.show(df_result.count())

        elif option=="3":
            # Need the state
            state=input("\n\n\n\nPlease enter the state's two letter abbreviation (ex. GA)\n")
            query = f"""
                SELECT b.BRANCH_STATE, b.BRANCH_NAME, COUNT(c.TRANSACTION_ID) AS Total_Transactions,
                CONCAT('$', format_number(sum(c.TRANSACTION_value), 2)) AS Total_Value
                FROM cdw_sapp_credit_card c
                JOIN cdw_sapp_branch b ON c.BRANCH_CODE = b.BRANCH_CODE
                WHERE b.BRANCH_STATE = '{state}'
                GROUP BY b.BRANCH_STATE, b.BRANCH_NAME
                ORDER BY Total_Value DESC;
            """
        
            # Run the query and create a DataFrame
            df_result = spark.sql(query)
            df_result.show(df_result.count())
            # Sum the total number of transactions of the given state and then the total value
        else:
            print("Not an option- Start Over") # If they don't pick something in the transaction list 1-3
            
    elif select=="2": # Customer Details was selected / Now choose 1, 2, 3, or 4
        option=input("\n\n\n\nSelect the option you would like to run\n\t1-Check the existing account details of a customer " 
                    "\n\t2-Modify the existing account details of a customer " 
                    "\n\t3-Generate a monthly bill for a credit card number for a given month and year "
                    "\n\t4-Display the transactions made by a customer between two dates\n")
        
        if option=="1": # Need something to identify this customer to show account details
            SSn=input("\n\n\n\nPlease enter your SSN\n")
            query = f"""
                SELECT * 
                FROM cdw_sapp_customer 
                WHERE SSN = '{SSn}';
                """

            # Run the query and create a DataFrame
            df_result = spark.sql(query)
            df_result.show(df_result.count())
            #show the account details (The row of this SSN)

        elif option=="2": #To modify the account
            SSn=input("\n\n\n\nPlease enter your SSN\n")
            to_modify=input("\n\n\n\nSelect the option you would like to change"
                    "\n\t1-Last Name"
                    "\n\t2-Street Address"
                    "\n\t3-City"
                    "\n\t4-State"
                    "\n\t5-Country"
                    "\n\t6-Zip Code"
                    "\n\t7-Email"
                    "\n\t8-Phone\n")
            if to_modify=="1":#last name
                change_to=input("\n\n\n\nPlease enter your new last name\n")
                column_name="LAST_NAME"
            elif to_modify=="2":#street address
                change_to=input("\n\n\n\nPlease enter your new street address as (Street, Apartment)\n")
                column_name="FULL_STREET_ADDRESS"
            elif to_modify=="3": #city
                change_to=input("\n\n\n\nPlease enter your new city\n")
                column_name="CUST_CITY"
            elif to_modify=="4":#state
                change_to=input("\n\n\n\nPlease enter your new state\n")
                column_name="CUST_STATE"
            elif to_modify=="5":#country
                change_to=input("\n\n\n\nPlease enter your new country\n")
                column_name="CUST_COUNTRY"
            elif to_modify=="6":#zip code
                change_to=input("\n\n\n\nPlease enter your new zip code\n")
                column_name="CUST_ZIP"
            elif to_modify=="7":#email
                change_to=input("\n\n\n\nPlease enter your new email\n")
                column_name="CUST_EMAIL"
            elif to_modify=="8":#phone
                change_to=input("\n\n\n\nPlease enter your new phone number\n")
                column_name="CUST_PHONE"
            else:
                print("Not an option- Start Over")

            # Make a connection to your MySQL server
            # Need the connector in order to modify the data ie update the changes to a customer above
            mydb = mysql.connector.connect(
                host="localhost",
                user=Uname,
                password=Upassword,
                database="creditcard_capstone"
            )

            # This creates a new cursor object on the database connection object
            # Enables interaction with the database
            mycursor = mydb.cursor()

            # Build the UPDATE SQL query string
            # Used f-string (formatted string) to embed expressions inside curly braces {} to evaluate and include their values in the string
            # The first %s will be the "change_to" and the second %s will be the "SSn" - the the below execute parameters
            sql_query = f"UPDATE cdw_sapp_customer SET {column_name} = %s WHERE SSN = %s"

            # Executing the query securely with placeholder values
            mycursor.execute(sql_query, (change_to, SSn))

            # Committing the transaction to the database
            mydb.commit()

            # Closing the connection
            mycursor.close()
            mydb.close()

            # Read data from the customer table
            df_customer = spark.read.format("jdbc") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                .option("user", Uname) \
                .option("password", Upassword) \
                .option("dbtable", "cdw_sapp_customer") \
                .load()

            # Read data from the credit card table
            df_credit = spark.read.format("jdbc") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                .option("user", Uname) \
                .option("password", Upassword) \
                .option("dbtable", "cdw_sapp_credit_card") \
                .load()

            # Read data from the branch table
            df_branch = spark.read.format("jdbc") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                .option("user", Uname) \
                .option("password", Upassword) \
                .option("dbtable", "cdw_sapp_branch") \
                .load()

            # Register the DataFrames as temporary views in Spark
            df_customer.createOrReplaceTempView("cdw_sapp_customer")
            df_credit.createOrReplaceTempView("cdw_sapp_credit_card")
            df_branch.createOrReplaceTempView("cdw_sapp_branch")
            query = f"""
                SELECT * 
                FROM cdw_sapp_customer 
                WHERE SSN = '{SSn}';
                """

            # Run the query and create a DataFrame
            df_result = spark.sql(query)
            df_result.show(df_result.count())
            #show the account details (The row of this SSN)
            #print("\nNote: Changes are not reflected unless program is restarted\n")
        elif option=="3": # Generate a monthly bill for a credit card number for a given month/year
            credit_card_number=input("\n\n\n\nPlease enter the credit card number\n")
            month=input("\n\n\n\nPlease enter the month as MM\n")
            year=input("\n\n\n\nPlease enter the year as YYYY\n")
        
            # Formulating the SQL query to display transaction details
            query_transactions = f"""
            SELECT 
                CREDIT.CUST_CC_NO, 
                CREDIT.TIMEID,
                SUBSTRING(CREDIT.TIMEID, 1, 4) AS YEAR,
                SUBSTRING(CREDIT.TIMEID, 5, 2) AS MONTH,
                SUBSTRING(CREDIT.TIMEID, 7, 2) AS DAY,
                CREDIT.TRANSACTION_ID AS TRANS_ID,
                CREDIT.TRANSACTION_TYPE AS TRANS_TYPE,
                CREDIT.TRANSACTION_VALUE AS TRANS_VALUE
            FROM 
                cdw_sapp_credit_card AS CREDIT
            WHERE 
                CREDIT.CUST_CC_NO = '{credit_card_number}'
                AND SUBSTRING(CREDIT.TIMEID, 1, 6) = '{year}{month}'
            ORDER BY 
                DAY;
            """

            # Formulating the SQL query to calculate total amount owed
            query_total_owed = f"""
            SELECT 
                ROUND(SUM(CREDIT.TRANSACTION_VALUE),2) AS TOTAL_OWED
            FROM 
                cdw_sapp_credit_card AS CREDIT
            WHERE 
                CREDIT.CUST_CC_NO = '{credit_card_number}'
                AND SUBSTRING(CREDIT.TIMEID, 1, 6) = '{year}{month}';
            """

            # Executing the SQL queries
            monthly_bill = spark.sql(query_transactions)
            total_owed = spark.sql(query_total_owed)

            # Displaying the monthly bill
            print("\n\n\n\nDetailed Transactions:")
            monthly_bill.show(monthly_bill.count())

            # Displaying the total owed amount
            print("Total Owed:")
            total_owed.show(total_owed.count())

            # sum the transaction values for a given CCn and month/year
        elif option=="4": #Show transactions for a customer between two dates
            SSn=input("\n\n\n\nPlease enter your SSN\n")
            date1=input("\n\n\n\nPlease enter the first date as YYYYMMDD\n")
            date2=input("\n\n\n\nPlease enter the second date as YYYYMMDD\n")
            # Order by year, month, and day in descending order
            # Formulating the SQL query to display transaction details between two dates
            query_transactions = f"""
            SELECT 
                CUST.FIRST_NAME as First_Name,
                CUST.LAST_NAME as Last_Name,
                SUBSTRING(CREDIT.TIMEID, 1, 4) AS YEAR,
                SUBSTRING(CREDIT.TIMEID, 5, 2) AS MONTH,
                SUBSTRING(CREDIT.TIMEID, 7, 2) AS DAY,
                CREDIT.TRANSACTION_ID AS TRANS_ID,
                CREDIT.TRANSACTION_TYPE AS TRANS_TYPE,
                CREDIT.TRANSACTION_VALUE AS TRANS_VALUE
            FROM 
                cdw_sapp_credit_card AS CREDIT
            INNER JOIN
                cdw_sapp_customer AS CUST
            ON 
                CREDIT.CUST_SSN = CUST.SSN
            WHERE 
                CUST.SSN = '{SSn}'
                AND CREDIT.TIMEID BETWEEN '{date1}' AND '{date2}'
            ORDER BY 
                YEAR DESC, MONTH DESC, DAY DESC;
            """

            # Executing the SQL query
            transactions_between_dates = spark.sql(query_transactions)

            # Displaying the transactions
            print("Transactions between", date1, "and", date2, ":")
            transactions_between_dates.show(transactions_between_dates.count())
        else:
            print("Not an option- Start Over") # If they don't pick one of the four options in the customer list
    else:
        print("Not an option- Start Over") # If user didn't pick on two of the top menu (Tranaction or Customer)
    last=input("\nWhat would you like to do next?\n\t1-Start Over\n\t2-Exit\n")
    if last=="1":
        done=False
    else:
        done=True
spark.stop()