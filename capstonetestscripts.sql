USE creditcard_capstone;
SELECT * FROM  cdw_sapp_branch
LIMIT 10;


USE creditcard_capstone;
SELECT * FROM  cdw_sapp_branch
where branch_zip is null
LIMIT 10;

USE creditcard_capstone;
SELECT * FROM  cdw_sapp_customer
LIMIT 100

USE creditcard_capstone;
SELECT ssn  FROM  cdw_sapp_customer
where ssn is cdw_sapp_branchcdw_sapp_credit_cardnull
LIMIT 10


USE creditcard_capstone;
SELECT CUST_SSN FROM  cdw_sapp_credit_card
WHERE CUST_SSN < 9;

USE creditcard_capstone;
Select distinct CUST_ZIP 
from cdw_sapp_customer
order by CUST_ZIP


USE creditcard_capstone;
Select distinct CUST_ZIP 
from cdw_sapp_customer
where (len(str(CUST_ZIP)) < 5)
order by CUST_ZIP

USE creditcard_capstone;
Select distinct CUST_ZIP from cdw_sapp_customer
where len(CUST_ZIP) = 4
order by CUST_ZIP

USE creditcard_capstone;
Select distinct CUST_ZIP from cdw_sapp_customer
order by CUST_ZIP

Select * from cdw_sapp_customer
where CUST_ZIP = '1810'
order by CUST_ZIP 

Select COUNT(*) from cdw_sapp_customer
where CUST_ZIP = LENGTH(5)
order by CUST_ZIP

USE creditcard_capstone;
Select distinct CUST_ZIP from cdw_sapp_customer
where length(CUST_ZIP) < 5
order by CUST_ZIPcdw_sapp_branch



USE creditcard_capstone;
Select * from cdw_sapp_customer
where CUST_ZIP >= 0  and CUST_ZIP < 1000
order by CUST_ZIP


Select * from cdw_sapp_customer
where FIRST_NAME = 'Abby'
order by FIRST_NAME

Select distinct FIRST_NAME from cdw_sapp_customer
order by FIRST_NAME

Select * from cdw_sapp_customer
where FIRST_NAME = 'Abby'
order by FIRST_NAME

Select distinct CUST_ZIP from cdw_sapp_customer
order by CUST_ZIP

Select  CUST_ZIP from cdw_sapp_customer
WHERE CUST_ZIP = '10598'
order by CUST_ZIP

Select  CUST_ZIP from cdw_sapp_customer
WHERE CUST_ZIP = '1810'
order by CUST_ZIP


ORDER BY cdw_sapp_customer.CUST_ZIP



SELECT lAST_NAME, CUST_ZIP, SSN, TIMEID, year(TIMEID), month(TIMEID), day(TIMEID) as dayOrder
FROM cdw_sapp_customer
INNER JOIN cdw_sapp_credit_card
ON cdw_sapp_customer.SSN = cdw_sapp_credit_card.cust_SSN
WHERE cdw_sapp_customer.CUST_ZIP = '1810'
AND cdw_sapp_credit_card.timeid = '20181207'
ORDER BY dayOrder Desc

SELECT lAST_NAME, CUST_ZIP, SSN, TIMEID, year(TIMEID), month(TIMEID), day(TIMEID) as dayOrder
FROM cdw_sapp_customer
INNER JOIN cdw_sapp_credit_card
ON cdw_sapp_customer.SSN = cdw_sapp_credit_card.cust_SSN
WHERE cdw_sapp_customer.CUST_ZIP = '1810'
AND cdw_sapp_credit_card.timeid = '20181207'
ORDER BY dayOrder Desc


SELECT lAST_NAME, CUST_ZIP, SSN, TIMEID, year(TIMEID), month(TIMEID), day(TIMEID) as dayOrder
FROM cdw_sapp_customer
INNER JOIN cdw_sapp_credit_card
ON cdw_sapp_customer.SSN = cdw_sapp_credit_card.cust_SSN
WHERE cdw_sapp_customer.CUST_ZIP = '1810'
AND cdw_sapp_credit_card.timeid = '20181207'
ORDER BY dayOrder Desc

SELECT cdw_sapp_customer.lAST_NAME, .cdw_sapp_customer.CUST_ZIP, SSN, TIMEID, year(TIMEID), month(TIMEID), day(TIMEID) as dayOrder



USE creditcard_capstone;
select year(timeid) as MyNewYear, month(timeid) as MyNewMonth
from cdw_sapp_credit_card

USE creditcard_capstone;
select month(timeid)
from cdw_sapp_credit_card

USE creditcard_capstone
select month(timeid) is null
from cdw_sapp_credit_card


select * from cdw_sapp_credit_card
where timeid is null or timeid = ''


USE creditcard_capstone;
Select distinct BRANCH_ZIP from cdw_sapp_BRANCH
where length(BRANCH_ZIP) < 5
order by BRANCH_ZIP


SELECT CUST.lAST_NAME, CUST.CUST_ZIP, CUST.SSN, 
CREDIT.TIMEID, year(CREDIT.TIMEID) AS YEAR, month(CREDIT.TIMEID) AS MONTH, day(CREDIT.TIMEID) as DAY
FROM cdw_sapp_customer AS CUST
INNER JOIN cdw_sapp_credit_card AS CREDIT
ON CUST.SSN = CREDIT.cust_SSN
WHERE CUST.CUST_ZIP = '1810'
AND CREDIT.timeid = '20181207'
ORDER BY DAY Desc

SELECT CUST.CUST_ZIP, CREDIT.TIMEID,
	year(CREDIT.TIMEID) AS YEAR, 
    month(CREDIT.TIMEID) AS MONTH, 
    day(CREDIT.TIMEID) AS DAY,
    CREDIT.TRANSACTION_ID AS TRANS_ID,
    CREDIT.TRANSACTION_TYPE AS TRANS_TYPE,
    CREDIT.TRANSACTION_VALUE AS TRANS_VALUE
FROM cdw_sapp_customer AS CUST
INNER JOIN cdw_sapp_credit_card AS CREDIT
ON CUST.SSN = CREDIT.cust_SSN
WHERE CUST.CUST_ZIP = '1810'
AND CREDIT.timeid = '20181207'
ORDER BY DAY Desc

SELECT CUST.CUST_ZIP, CREDIT.TIMEID,
	year(CREDIT.TIMEID) AS YEAR, 
    month(CREDIT.TIMEID) AS MONTH, 
    day(CREDIT.TIMEID) AS DAY,
    CREDIT.TRANSACTION_ID AS TRANS_ID,
    CREDIT.TRANSACTION_TYPE AS TRANS_TYPE,
    CREDIT.TRANSACTION_VALUE AS TRANS_VALUE
FROM cdw_sapp_customer AS CUST
INNER JOIN cdw_sapp_credit_card AS CREDIT
ON CUST.SSN = CREDIT.cust_SSN
WHERE CUST.CUST_ZIP = '1810'
AND CREDIT.timeid = '20181207'
ORDER BY DAY Desc

SELECT * FROM CDW_SAPP_CUSTOMER ORDER BY CUST_ZIP

SELECT DISTINCT TRANSACTION_TYPE FROM cdw_sapp_credit_card




2.2.2 USED TO DISPLAY THE NUMBER AND TOTAL VALUES OF TRANSACTIONS FOR A GIVEN "TYPE"
SHOWS ALL OF THE TYPES TO ADD INTO THE INTERFACE FOR SELECTION:
SELECT DISTINCT TRANSACTION_TYPE FROM cdw_sapp_credit_card

SELECT  COUNT(*), TRANSACTION_TYPE, sum(TRANSACTION_value) FROM cdw_sapp_credit_card
WHERE TRANSACTION_TYPE = 'Grocery'
group by TRANSACTION_TYPE

SELECT  COUNT(*),
		TRANSACTION_TYPE,
        CONCAT('$', FORMAT(sum(TRANSACTION_value), 2)) AS TotalDollarValue
FROM cdw_sapp_credit_card
WHERE TRANSACTION_TYPE = 'Grocery'
GROUP BY TRANSACTION_TYPE


SELECT  branch_code

FROM cdw_sapp_branc
WHERE TRANSACTION_TYPE = 'Grocery'
GROUP BY TRANSACTION_TYPE


  SELECT b.BRANCH_STATE, b.BRANCH_NAME, COUNT(c.TRANSACTION_ID) AS total_transactions, 
            SUM(c.TRANSACTION_VALUE) AS total_value
            FROM cdw_sapp_credit_card c
            JOIN cdw_sapp_branch b ON c.BRANCH_CODE = b.BRANCH_CODE
            WHERE b.BRANCH_STATE = 'GA'
            GROUP BY b.BRANCH_STATE, b.BRANCH_NAME
            ORDER BY total_value DESC;

SELECT  CONCAT('$', FORMAT(TRANSACTION_value, 2)) FROM cdw_sapp_credit_card
LIMIT 10

CHAT GPT TOLD ME TO DO THIS:
SELECT name, CONCAT('$', FORMAT(price, 2)) AS formatted_price


SELECT CUST.CUST_ZIP, CREDIT.TIMEID,
	year(CREDIT.TIMEID) AS YEAR, 
    month(CREDIT.TIMEID) AS MONTH, 
    day(CREDIT.TIMEID) AS DAY,
    CREDIT.TRANSACTION_ID AS TRANS_ID,
    CREDIT.TRANSACTION_TYPE AS TRANS_TYPE,
    CREDIT.TRANSACTION_VALUE AS TRANS_VALUE
FROM cdw_sapp_customer AS CUST
INNER JOIN cdw_sapp_credit_card AS CREDIT
ON CUST.SSN = CREDIT.cust_SSN
WHERE CUST.CUST_ZIP = '1810'
AND CREDIT.timeid = '20181207'
ORDER BY DAY Desc

!!!THIS IS THE QUERY TO USE TO GET THE ANSWER TO THE CUSTOMERS INPUT - IN THIS EXAMPLE 'Grocery'!!!
!!!Returns 6549	Grocery	$337,051.63
SELECT  COUNT(*),
		TRANSACTION_TYPE,
        CONCAT('$', FORMAT(sum(TRANSACTION_value), 2)) AS TotalDollarValue
FROM cdw_sapp_credit_card
WHERE TRANSACTION_TYPE = 'Grocery'
GROUP BY TRANSACTION_TYPE



Used to check the existing account details of a customer.
select * from cdw_sapp_customer
limit 10

2.2.1 Used to check the existing account details of a customer.
SELECT
	CUST.FIRST_NAME, 
	CUST.MIDDLE_NAME, 
	CUST.LAST_NAME, 
	CUST.FULL_STREET_ADDRESS,
	CUST_CITY, 
	CUST_STATE, 
	CUST_COUNTRY, 
	CUST_ZIP, 
	CUST_EMAIL, 
	CUST_PHONE
FROM cdw_sapp_customer AS CUST
limit 10



SELECT CUST.CUST_ZIP, CREDIT.TIMEID,
	year(CREDIT.TIMEID) AS YEAR, 
    month(CREDIT.TIMEID) AS MONTH, 
    day(CREDIT.TIMEID) AS DAY,
    CREDIT.TRANSACTION_ID AS TRANS_ID,
    CREDIT.TRANSACTION_TYPE AS TRANS_TYPE,
    CREDIT.TRANSACTION_VALUE AS TRANS_VALUE
FROM cdw_sapp_customer AS CUST
INNER JOIN cdw_sapp_credit_card AS CREDIT
ON CUST.SSN = CREDIT.cust_SSN
WHERE CUST.CUST_ZIP = '1810'
AND CREDIT.timeid = '20181207'
ORDER BY DAY Desc

  CONCAT('$', FORMAT(sum(TRANSACTION_value), 2)) AS TotalDollarValue
SELECT  COUNT(*), TRANSACTION_TYPE, sum(TRANSACTION_value) FROM cdw_sapp_credit_card


SELECT  COUNT(*), TRANSACTION_TYPE, CONCAT('$', FORMAT(sum(TRANSACTION_value), 2)) AS TotalDollarValue
FROM cdw_sapp_credit_card
WHERE TRANSACTION_TYPE = 'Grocery'
group by TRANSACTION_TYPE


CONCAT('$', FORMAT(sum(TRANSACTION_value), 2)) AS TotalDollarValue

SELECT  COUNT(*),
		TRANSACTION_TYPE,
        CONCAT('$', FORMAT(sum(TRANSACTION_value), 2)) AS TotalDollarValue
FROM cdw_sapp_credit_card
WHERE TRANSACTION_TYPE = 'Grocery'
GROUP BY TRANSACTION_TYPE

 SELECT b.BRANCH_STATE, b.BRANCH_NAME, COUNT(c.TRANSACTION_ID) AS total_transactions, 
			CONCAT('$', format(sum(c.TRANSACTION_value), 2)) AS total_value
            FROM cdw_sapp_credit_card c
            JOIN cdw_sapp_branch b ON c.BRANCH_CODE = b.BRANCH_CODE
            WHERE b.BRANCH_STATE = 'GA'
            GROUP BY b.BRANCH_STATE, b.BRANCH_NAME
            ORDER BY total_value DESC;
            
			SELECT * 
            FROM cdw_sapp_customer 
            WHERE SSN = '123451799';

			SELECT * 
            FROM cdw_sapp_customer
            limit 10
            
            
            
	year(CREDIT.TIMEID) AS YEAR, 
    month(CREDIT.TIMEID) AS MONTH, 
    day(CREDIT.TIMEID) AS DAY,
            
            ##############SATURDAY WORKING ON QUERIES #######################################################
            #################################################################################################
            !!!!! TEST THIS PIECE OF SQL ON SUNDAY NOTE THAT THE TIMEID HAS ODD DATES FOR THE SSN 
            LIKE 20182605, 20181409 THIS MEANS THE MONTH IS 26 AND 14 IN THIS CASE MAYBE THEY JUST NEED TO BE REORDERED
            2.2.3 Used to generate a monthly bill for a credit card number for a given month and year
            NUMBER TO USE FOR TESTING 4210653310061055
            SELECT * FROM cdw_sapp_credit_card ORDER BY CUST_CC_NO
            THIS QUERY STILL NEEDS A TOTAL AMOUNT 
		
            SELECT * FROM cdw_sapp_credit_card
            WHERE CUST_CC_NO = '4210653310061055'
            AND year(TIMEID) = '2018'
            AND month(TIMEID) = '12'
			ORDER BY CUST_CC_NO
            LIMIT 200
            
              SUM(TRANSACTION_VALUE) AS 'GRAND_TOTAL', NEED A GROUP BY IF USING SUM
            
            &&& TO DO CHANGE USE THE VALUE SENT IN AS THE DATE ON THE DISPLAY INSTEAD OF TIMEID &&&
            &&& ALSO NEED TO SUM UP THE AMOUNT CHARGED FOR THE RECORDS RETURNED WITH A AMOUNT DUE HEADING ROW
            SELECT CUST_CC_NO AS 'Customer_Number',
            TRANSACTION_TYPE AS 'Transaction_Type',
            TRANSACTION_VALUE AS 'Amount_Charged',
            TIMEID,BRANCH_CODE, CUST_SSN, TRANSACTION_ID
            FROM 
				cdw_sapp_credit_card
            WHERE 
				CUST_CC_NO = '4210653310061055'
            AND 
				year(TIMEID) = '2018'
            AND
				month(TIMEID) = '12'
			ORDER BY 
				CUST_CC_NO
            LIMIT 200
            
            99999 USING THE AGGREGATE SUM FUNCTION ON TRAN TO GET A TOTAL
            SELECT 
            SUM(TRANSACTION_VALUE)
            FROM 
				cdw_sapp_credit_card
            WHERE 
				CUST_CC_NO = '4210653310061055'
            AND 
				year(TIMEID) = '2018'
            AND
				month(TIMEID) = '12'
            LIMIT 200
            
  777777777777777777SATURDAY WORK FO THIS LAST QUERY&&&&&&&&&&&&&&&&&&&&
          2.2.4) Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
          the user will enter in a customer 
          use creditcard_capstone;
          SELECT Cust.First_Name, Cust.LAST_NAME, 
          FROM cdw_sapp_credit_card as Credit
          INNER JOIN cdw_sapp_customer as Cust
          on Credit.cust_ssn = Cust.ssn
		  WHERE Credit.timeid BETWEEN '2018-01-01' AND '2020-12-31'
          and Cust.ssn = '123451310'
          order by credit.timeid
          limit 20
          
          
          
          WHERE cdw_sapp_customer = cdw_sapp_credit_card
          
          SELECT * FROM orders
          WHERE order_date BETWEEN '2022-01-01' AND '2022-12-31';
          
          WHERE date_column BETWEEN start_date AND end_date;
          USE THE DATES (YEAR/MONTH/DAY
			ORDER BY year, month, day DESC
            
            NEED TO FIND SOME TEST DATA TO WORK WITH THIS QUERY
          
           SELECT CUST_CC_NO AS Customer Number,
                  TRANSACTION_TYPE AS 'Transaction_Type',
				  TRANSACTION_VALUE AS 'Amount_Charged',
				  TIMEID,BRANCH_CODE, CUST_SSN, TRANSACTION_ID
            FROM 
				  cdw_sapp_credit_card
            WHERE 
				  CUST_CC_NO = '4210653310061055'
            AND 
				  TIMEID BETWEEN '20181211' AND '20181211'
			ORDER BY 
				  TIMEID
            LIMIT 200
            
          SELECT * 
          FROM cdw_sapp_credit_card
          WHERE CUST_CC_NO = '4210653310061055'
          ORDER BY TIMEID ASC

		  SELECT * 
          FROM cdw_sapp_credit_card
          WHERE MONTH(TIMEID) >= '12'
          ORDER BY TIMEID DESC
          
          SELECT * 
          FROM cdw_sapp_credit_card
          WHERE DAY(TIMEID) >= 12
          ORDER BY TIMEID DESC
          
          SELECT * 
          FROM cdw_sapp_credit_card
          ORDER BY TIMEID DESC
          
		  SELECT * 
          FROM cdw_sapp_credit_card
          ORDER BY TIMEID ASC