import pandas as pd
import pymysql

# Reading all the Data_sets
customers = pd.read_csv('customers.csv')
accounts = pd.read_csv('accounts.csv')
loans = pd.read_json('loans.json')
branches = pd.read_json('branches.json', lines=True)
transactions = pd.read_csv('transactions.csv')
credit_cards = pd.read_json('credit_cards.json')
support_tickets = pd.read_json('support_tickets.json')

# connecting to Mysql
con = pymysql.connect(
    host="localhost",
    user="root",
    password="Yuva123",
    autocommit=True
)

cursor = con.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS Banking_Insights;")

cursor.execute("USE Banking_Insights;")

#Creating table for customers
cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    gender ENUM('M', 'F') NOT NULL,
    age INT CHECK (age > 0),
    city VARCHAR(100),
    account_type ENUM('Savings', 'Current') NOT NULL,
    join_date DATE
);
""")

#Creating table for Branches
cursor.execute("""
CREATE TABLE IF NOT EXISTS branches (
    Branch_ID INT PRIMARY KEY,
    Branch_Name VARCHAR(100),
    City VARCHAR(100),
    Manager_Name VARCHAR(100),
    Total_Employees INT,
    Branch_Revenue DECIMAL(15,2),
    Opening_Date DATE,
    Performance_Rating INT
);
""")

#Creating table for Accounts
cursor.execute("""
CREATE TABLE IF NOT EXISTS accounts (
    customer_id VARCHAR(20),
    account_balance FLOAT,
    last_updated DATETIME,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
""")

#Creating table for Transactions
cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    txn_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    txn_type VARCHAR(50),
    amount DECIMAL(12,2),
    txn_time DATETIME,
    status VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
""")

#Creating table for Loans
cursor.execute("""
CREATE TABLE IF NOT EXISTS loans (
    Loan_ID INT PRIMARY KEY,
    customer_id VARCHAR(20),
    Account_ID INT,
    Branch VARCHAR(100),
    Loan_Type VARCHAR(50),
    Loan_Amount INT,
    Interest_Rate DECIMAL(5,2),
    Loan_Term_Months INT,
    Start_Date DATE,
    End_Date DATE,
    Loan_Status VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
""")

#Creating table for Credit Cards
cursor.execute("""
CREATE TABLE IF NOT EXISTS credit_cards(
    Card_ID INT PRIMARY KEY,
    customer_id VARCHAR(50),
    Account_ID INT,
    Branch VARCHAR(100),
    Card_Number VARCHAR(30),
    Card_Type VARCHAR(50),
    Card_Network VARCHAR(50),
    Credit_Limit INT,
    Current_Balance DECIMAL(12,2),
    Issued_Date DATE,
    Expiry_Date DATE,
    Status VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
""")


#Creating table for Support Tickets
cursor.execute("""
CREATE TABLE IF NOT EXISTS support_tickets (
    Ticket_ID VARCHAR(50) PRIMARY KEY,
    Customer_ID VARCHAR(50),
    Account_ID VARCHAR(50),
    Loan_ID VARCHAR(50),
    Branch_Name VARCHAR(100),
    Issue_Category VARCHAR(100),
    Description TEXT,
    Date_Opened DATE,
    Date_Closed DATE,
    Priority VARCHAR(50),
    Status VARCHAR(50),
    Resolution_Remarks TEXT,
    Support_Agent VARCHAR(100),
    Channel VARCHAR(50),
    Customer_Rating INT,
    FOREIGN KEY (Customer_ID) REFERENCES customers(customer_id)
);
""")
con.commit()

# customers
for _, row in customers.iterrows():
    cursor.execute("""
        INSERT IGNORE INTO customers 
        (customer_id, name, gender, age, city, account_type, join_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))

# branches
for _, row in branches.iterrows():
    cursor.execute("""
        INSERT IGNORE INTO branches 
        (Branch_ID, Branch_Name, City, Manager_Name, Total_Employees,
         Branch_Revenue, Opening_Date, Performance_Rating)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))

# accounts
for _, row in accounts.iterrows():
    cursor.execute("""
        INSERT IGNORE INTO accounts (customer_id, account_balance, last_updated)
        VALUES (%s, %s, %s)
    """, tuple(row))

# transactions
for _, row in transactions.iterrows():
    cursor.execute("""
        INSERT IGNORE INTO transactions (txn_id, customer_id, txn_type, amount, txn_time, status)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, tuple(row))

# loans
for _, row in loans.iterrows():
    cursor.execute("""
        INSERT INTO loans 
        (Loan_ID, customer_id, Account_ID, Branch, Loan_Type, Loan_Amount, Interest_Rate,
         Loan_Term_Months, Start_Date, End_Date, Loan_Status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))
con.commit()
# credit_cards
for _, row in credit_cards.iterrows():
    cursor.execute("""
        INSERT INTO credit_cards 
        (Card_ID, customer_id, Account_ID, Branch, Card_Number, Card_Type,
         Card_Network, Credit_Limit, Current_Balance, Issued_Date, Expiry_Date, Status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['Card_ID'],
        row['customer_id'],
        row['Account_ID'],
        row['Branch'],
        row['Card_Number'],
        row['Card_Type'],
        row['Card_Network'],
        row['Credit_Limit'],
        row['Current_Balance'],
        row['Issued_Date'],
        row['Expiry_Date'],
        row['Status']
    ))
con.commit()
# support_tickets
for _, row in support_tickets.iterrows():
    cursor.execute("""
        INSERT IGNORE INTO support_tickets 
        (Ticket_ID, Customer_ID, Account_ID, Loan_ID, Branch_Name, Issue_Category,
         Description, Date_Opened, Date_Closed, Priority, Status, Resolution_Remarks,
         Support_Agent, Channel, Customer_Rating)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))

con.commit()
cursor.close()
con.close()