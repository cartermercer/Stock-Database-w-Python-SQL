# Set up for script and connection to MySQL
%reset
import pandas as pd
import os
import requests
import mysql.connector
from xml.dom import minidom
import xml.etree.ElementTree as ET

# you should set up a .xml file which would encrypt the password to your database
# Parse Secrets.xml file to get database password
os.chdir('whatever directory you put the Secrets.xml file in')

tree = ET.parse('Secrets.xml')
root = tree.getroot()

for f in root.iter("string"):
    password = f.text

# Connect to MySQL
mydb = mysql.connector.connect(
  host="your database host name",
  user="you database user name",
  password = password, 
  database="mydb"
)


# Creating a stock dictionary from the Stock Table that gives symbols and stock id to be used for the loop to get data 
mydb.reconnect()
mycursor = mydb.cursor()
mycursor.execute("SELECT idStock,Symbol FROM Stock")
myresult = mycursor.fetchall()

Symbols = []
stock_dict = {}

for i in myresult:
    idStock, Symbol = i
    Symbols.append(Symbol)
    stock_dict[Symbol] = idStock


# IEX Credentials
iex_base_url = 'https://cloud.iexapis.com/v1'
iex_token = 'your IEX Token'
params = {'token': iex_token}



# Now that we have our symbols loaded into Python from our database, we can iterate over these symbols to get data from IEX to go into a Stock Info table
# Note that there are tables called Sectors, Industries, and Exchange that new data gets added to and then these are inserted as Foreign Keys into the Stock Info Table
# There is quite a bit of try: except: handling due to missing data that can occur in the IEX API. In addition, checking if certain values are present is important to not break the entire loop!

for symbol in Symbols:
                
    # Make an API call for Company information (IEX)
    try:
        resp_iex_company = requests.get(iex_base_url + '/stock/' + symbol + '/company', params = params)
        resp_iex_company = resp_iex_company.json()
    except:
        continue
    
    
    # Make an API call for Company Logo (IEX)
    try:
        resp_iex_logo = requests.get(iex_base_url + '/stock/' + symbol + '/logo', params = params)
        resp_iex_logo = resp_iex_logo.json()
        Logo =  "'" + resp_iex_logo['url'] + "'"
    except:
        Logo = "NULL"
        
    # Create the Exchange, Industrym and Sector Variables
    if resp_iex_company['exchange']:
        Exchange = resp_iex_company['exchange']
    if resp_iex_company['industry']:
        Industry = resp_iex_company['industry']
    if resp_iex_company['sector']:
        Sector = resp_iex_company['sector']
    
    # Creating more variables
    if resp_iex_company['companyName']:
        Name = "'" + resp_iex_company['companyName'] + "'"
    if resp_iex_logo['url']:
        Logo =  "'" + resp_iex_logo['url'] + "'"
    if resp_iex_company['website']:
        Website = "'" + resp_iex_company['website'] + "'"
    if resp_iex_company['issueType']:
        IssueType = "'" + resp_iex_company['issueType'] + "'"
    
    if resp_iex_company['address']:
        Address = "'" + resp_iex_company['address'] + "'"
    if resp_iex_company['state']:
        State = "'" + resp_iex_company['state'] + "'"
    if resp_iex_company['city']:
        City = "'" + resp_iex_company['city'] + "'"
    if resp_iex_company['zip']:
        Zip = "'" + resp_iex_company['zip'] + "'"
    if resp_iex_company['country']:
        Country = "'" + resp_iex_company['country'] + "'"
    
    # Some formatting changes that agrees with the mysql functions
    symbol_stockinfo = "'" + symbol + "'" # This is just the symbol in whatever iteration we are on 
    industry_stockinfo = "'" + Industry + "'"
    sector_stockinfo = "'" + Sector + "'"
    exchange_stockinfo = "'" + Exchange + "'"    
    
    # sometimes it's good to reconnect so not to lose connection which can be a pain and happens sometimes.
    mydb.reconnect()
    mycursor = mydb.cursor()
    
    # Note that these first three tables the sql can be written as such "INSERT IGNORE INTO TableName (Value) VALUES (%s);"
    
    # Write SQL to insert any new values into Sectors Table
    sql = "INSERT IGNORE INTO Sectors (Sector) VALUES (%s)"
    val = (Sector)
    mycursor.execute(sql, (val,))
    mydb.commit()
        
    # Write SQL to insert any new values into Industries Table
    sql = "INSERT IGNORE INTO Industries (Industry) VALUES (%s)"
    val = (Industry)
    mycursor.execute(sql, (val,))
    mydb.commit()
        
    # Write SQL to insert any new values into Exchange Table
    sql = "INSERT IGNORE INTO Exchange (Exchange) VALUES (%s)"
    val = (Exchange)
    mycursor.execute(sql, (val,))
    mydb.commit()
    
    
    # The sql is written a bit differently here as a multi-line string to write some more advanced sql. We need to select some values from other tables
    
    sql = f''' INSERT IGNORE INTO StockInfo (Name, Logo, Website, IssueType, Address, State, City, Zip, Country, Stock_idStock, Industries_idIndustries,                                    Sectors_idSectors, Exchange_idExchange)
                    VALUES({Name}, {Logo}, {Website}, {IssueType}, {Address}, {State}, {City}, {Zip}, {Country},
                    (SELECT idStock FROM Stock WHERE Symbol = {symbol_stockinfo}),
                    (SELECT idIndustries FROM Industries WHERE Industry = {industry_stockinfo}),
                    (SELECT idSectors FROM Sectors WHERE Sector = {sector_stockinfo}),
                    (SELECT idExchange FROM Exchange WHERE Exchange = {exchange_stockinfo}) )
            '''
    try:
        mycursor.execute(sql)
    except: 
        continue
    mydb.commit()
