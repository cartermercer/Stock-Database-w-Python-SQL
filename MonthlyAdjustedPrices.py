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


# Alpha Vantage Credentials
alpha_base_url = "https://www.alphavantage.co/query" 
alpha_token = "Your Alpha Vantage token"

# Get data for Monthly Adjusted Close
# We are using a new library here because it is much more efficient to upload data as a pandas dataframe!
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
        .format(host="your host name", db="your database name", user="your user name", pw=password))

# Outer loop to make a request for each symbol
for symbol in Symbols:
    
    # time.sleep necessary to space out API calls, depending on your subscription to Alpha Vantage -- free version allows 5 calls per minute at 500 per day
    time.sleep(12)
    
    # parameters for Alpha Vantage monthly adjusted prices
    data = { 
    "function": "TIME_SERIES_MONTHLY_ADJUSTED", 
    "symbol": symbol,
    "outputsize" : "full",
    "datatype": "json", 
    "apikey": alpha_token} 
    try:
        resp = requests.get(alpha_base_url, data) 
        resp = resp.json()
    except:
        continue
    try: 
        if resp['Error Message']:
            continue
    except:
        if not resp['Monthly Adjusted Time Series']:
            continue
    
    # Commented out section was original row by row method of getting data in but very slow! See method after this section for more efficient pandas df method
    
    # Inner loop to get row by row data for the symbol used in outer loop
    #for i in range(0, len(list(resp['Monthly Adjusted Time Series'].items()))):
        #date = list(resp['Monthly Adjusted Time Series'].items())[i][0]
        #Open = list(resp['Monthly Adjusted Time Series'].items())[i][1]['1. open']
        #high = list(resp['Monthly Adjusted Time Series'].items())[i][1]['2. high']
        #low = list(resp['Monthly Adjusted Time Series'].items())[i][1]['3. low']
        #close = list(resp['Monthly Adjusted Time Series'].items())[i][1]['4. close']
        #adjustedClose = list(resp['Monthly Adjusted Time Series'].items())[i][1]['5. adjusted close']
        #volume = list(resp['Monthly Adjusted Time Series'].items())[i][1]['6. volume']
        #dividendAmount = list(resp['Monthly Adjusted Time Series'].items())[i][1]['7. dividend amount']
        #idStock = stock_dict[symbol]
        
        # insert each row into table
        #mycursor = mydb.cursor()
        #sql = "INSERT INTO MonthlyAdjustedPrices (Stock_idStock, Date, Open, High, Low, Close, AdjustedClose, Volume, DividendAmount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        #val = (idStock, date, Open, high, low, close, adjustedClose, volume, dividendAmount)
        #mycursor.execute(sql, val)
    
    # Better, more efficient method! Saving data as dataframe and then uploading a data frame for each stock.
    df = pd.DataFrame(resp['Monthly Adjusted Time Series']).transpose()
    df.index.name = 'Date'
    df.reset_index(inplace=True)
    df["Stock_idStock"] = stock_dict[symbol]
    df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'AdjustedClose', 'Volume', 'DividendAmount', 'Stock_idStock']
    df = df[['Stock_idStock', 'Date', 'Open', 'High', 'Low', 'Close', 'AdjustedClose', 'Volume', 'DividendAmount']]
    
    df.to_sql('MonthlyAdjustedPrices', engine, if_exists='append', index=False)
    
mydb.commit()

# Close connection to MySQL
mycursor.close()
mydb.close()
