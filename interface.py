import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import mysql.connector
import contextlib
import sys
import os

@contextlib.contextmanager
def suppress_print_statements():
    # Save the original sys.stdout
    original_stdout = sys.stdout

    try:
        # Open a dummy stream that does nothing, and redirect sys.stdout to it
        with open(os.devnull, 'w') as dummy_stream:
            sys.stdout = dummy_stream
            yield
    finally:
        # Restore sys.stdout to its original value
        sys.stdout = original_stdout


# Usage
with suppress_print_statements():
    spark = SparkSession.builder.master('local[1]').appName('demo.com').getOrCreate()
    # "jdbc:mysql://localhost:3306/creditcard_capstone"
    # Read data from the customer table
    df_customer = spark.read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("user", "root") \
        .option("password", "password") \
        .option("dbtable", "cdw_sapp_customer") \
        .load()

    # Read data from the credit card table
    df_credit = spark.read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("user", "root") \
        .option("password", "password") \
        .option("dbtable", "cdw_sapp_credit_card") \
        .load()
    df_branch = spark.read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("user", "root") \
        .option("password", "password") \
        .option("dbtable", "cdw_sapp_branch") \
        .load()
    # Register the DataFrames as temporary views in Spark
    df_customer.createOrReplaceTempView("cdw_sapp_customer")
    df_credit.createOrReplaceTempView("cdw_sapp_credit_card")
    df_branch.createOrReplaceTempView("cdw_sapp_branch")
    #Need to pick between Transaction details and the customer
select=input("Choose which details you would like to see\n\t1-Transaction\n\t2-Customer\n")

if select=="1": #Transaction
    option=input("\n\n\n\nSelect the option you would like to run\n\t1-Display the transactions made by " 
                 "customers living in a given zip code for a given month and year\n\t2-Display the " 
                 "number and total values of transactions for a given type\n\t3-Display the total "
                 "number and total values of transactions for branches in a given state\n")
    if option=="1":
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
        df_result = spark.sql(query)

        # Show the result
        df_result.show()
        #show all transactions made by all customers in this zip code in this month in this year
        #order by day in descending order
        input("\nPress enter to exit:\n")
    elif option=="2":
        #Need the type of transaction
        i=input("\n\n\n\nPlease enter the transaction type from the list below"
                               "\n\t1-Grocery"
                               "\n\t2-Education"
                               "\n\t3-Bills"
                               "\n\t4-Gas"
                               "\n\t5-Entertainment"
                               "\n\t6-Test"
                               "\n\t7-Healthcare\n")
        transaction_type=["Grocery","Education","Bills","Gas","Entertainment","Test","Healthcare"][int(i)-1]
        #show the total number and total value of these transaction types
        query=f'''
            SELECT  COUNT(*), TRANSACTION_TYPE, 
            CONCAT('$', format_number(sum(TRANSACTION_value), 2)) AS TotalDollarValue
            FROM cdw_sapp_credit_card
            WHERE TRANSACTION_TYPE = '{transaction_type}'
            group by TRANSACTION_TYPE
        '''
        df_result = spark.sql(query)

        # Show the result
        df_result.show()
        input("\nPress enter to exit:\n")
    elif option=="3":
        ##Need the state
        state=input("\n\n\n\nPlease enter the state's two letter abbreviation (ex. GA)\n")
        query = f"""
            SELECT b.BRANCH_STATE, b.BRANCH_NAME, COUNT(c.TRANSACTION_ID) AS total_transactions, 
			CONCAT('$', format_number(sum(c.TRANSACTION_value), 2)) AS total_value
            FROM cdw_sapp_credit_card c
            JOIN cdw_sapp_branch b ON c.BRANCH_CODE = b.BRANCH_CODE
            WHERE b.BRANCH_STATE = '{state}'
            GROUP BY b.BRANCH_STATE, b.BRANCH_NAME
            ORDER BY total_value DESC;
        """

            # Run the query and create a DataFrame
        df_result = spark.sql(query)
        df_result.show()
        input("\nPress enter to exit:\n")
        #sum the total number of transactions of the given state and then the total value
    else:
        print("Not an option- Start Over")
        raise
        
elif select=="2": #Customer
    option=input("\n\n\n\nSelect the option you would like to run\n\t1-Check the existing account details of a customer " 
                 "\n\t2-Modify the existing account details of a customer " 
                 "\n\t3-Generate a monthly bill for a credit card number for a given month and year "
                 "\n\t4-Display the transactions made by a customer between two dates\n")
    if option=="1": #Need something to identify this customer to show account details
        SSn=input("\n\n\n\nPlease enter your SSN\n")
        query = f"""
            SELECT * 
            FROM cdw_sapp_customer 
            WHERE SSN = '{SSn}';
            """

            # Run the query and create a DataFrame
        df_result = spark.sql(query)
        df_result.show()
        input("\nPress enter to exit:\n")
        #show the account details (probably the row of this SSN)
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
            raise

        # Make a connection to your MySQL server
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="creditcard_capstone"
        )

        mycursor = mydb.cursor()

        # Build the UPDATE SQL query string
        sql_query = f"UPDATE cdw_sapp_customer SET {column_name} = %s WHERE SSN = %s"

        # Executing the query securely with placeholder values
        mycursor.execute(sql_query, (change_to, SSn))

        # Committing the transaction to the database
        mydb.commit()

        # Closing the connection
        mycursor.close()
        mydb.close()
        input("\nPress enter to exit:\n")
        #df_updated_customer = df_customer.withColumn(
        #    column_name,
        #    when(col("SSN") == SSn, change_to).otherwise(col(column_name))
        #)   
        ## Writing the updated DataFrame back to the database.
        ## Be cautious, this will overwrite the existing table.
        #df_updated_customer.write.format("jdbc") \
        #    .option("driver", "com.mysql.cj.jdbc.Driver") \
        #    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        #    .option("user", "root") \
        #    .option("password", "password") \
        #    .option("dbtable", "cdw_sapp_customer") \
        #    .mode("overwrite") \
        #    .save()
        #    #Now use the SSN, column_name and the change_to variable to modify the dataframe
        
    elif option=="3": #Generate a monthly bill for a credit card number for a given month/year
        credit_card_number=input("\n\n\n\nPlease enter the credit card number\n")
        month=input("\n\n\n\nPlease enter the month as MM\n")
        year=input("\n\n\n\nPlease enter the year as YYYY\n")
        # Formulating the SQL query
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

        # Formulating the SQL query to calculate total owed amount
        query_total_owed = f"""
        SELECT 
            SUM(CREDIT.TRANSACTION_VALUE) AS TOTAL_OWED
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
        monthly_bill.show()

        # Displaying the total owed amount
        print("Total Owed:")
        total_owed.show()
        input("\nPress enter to exit:\n")
        #sum the transaction values for a given CCn and month/year
    elif option=="4": #Show transactions for a customer between two dates
        SSn=input("\n\n\n\nPlease enter your SSN\n")
        date1=input("\n\n\n\nPlease enter the first date as YYYYMMDD\n")
        date2=input("\n\n\n\nPlease enter the second date as YYYYMMDD\n")
        #Order by year, month, and day in descending order
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
        transactions_between_dates.show()
        input("\nPress enter to exit:\n")
    else:
        print("Not an option- Start Over")
        raise
else:
    print("Not an option- Start Over")
    raise
spark.stop()