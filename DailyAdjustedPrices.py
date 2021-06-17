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
iex_token = 'Your IEX Token'
params = {'token': iex_token,'chartCloseOnly':True} #chartCloseOnly param makes data much cheaper to get from IEX


# New library used to be able to insert data as batches of dataframes to MySQL
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
        .format(host="Your Host Name", db="Your database name", user="your user name", pw=password))

# if you want you could time both methods. Originally, it was taking about a minute per symbol, but using the pandas df upload it only takes a second per symbol
import datetime
StartTime = datetime.datetime.now()

for symbol in Symbols:
        
    try:
        resp_daily_adj = requests.get(iex_base_url + '/stock/' + symbol +'/chart/2d', params = params)
        resp_daily_adj = resp_daily_adj.json()
    except:
        print(symbol)
        continue

    if not resp_daily_adj:
        continue
    
    # Commented out  (previous method of saving each variable day by day, but was very slow!)
    
    #for i in range(0,len(resp_daily_adj)):
        #date = resp_daily_adj[i]['date']
        #adjClose = resp_daily_adj[i]['close']
        #adjVolume = resp_daily_adj[i]['volume']
        # insert each row into table
        #mycursor = mydb.cursor()
        #sql = "INSERT INTO DailyAdjustedPrices (Stock_idStock, Date, adjClose, adjVolume) VALUES (%s, %s, %s, %s)"
        #val = (idStock, date, adjClose, adjVolume)
        #mycursor.execute(sql, val)
        #mydb.commit()
        
    idStock = stock_dict[symbol]
        
    # Create dataframe to upload to MySQL
    df = pd.DataFrame(resp_daily_adj)[['change' ,'date','close','volume']]
    df['change'] = idStock
    df.columns = ['stock_idStock', 'Date','adjClose','adjVolume']
    
    df.to_sql('DailyAdjustedPrices', engine, if_exists='append', index=False)



# get elapsed time
EndTime = datetime.datetime.now()
print(EndTime - StartTime)



# Close connection to MySQL
mycursor.close()
mydb.close()
