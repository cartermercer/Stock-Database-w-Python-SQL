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


# Change directory to where you store your stock symbols
os.chdir('/Users/carterm/Desktop/Todds Data/Data')
symbols = pd.read_excel('updated_symbols.xlsx', header = None)
symbols = symbols.iloc[:,0]

# Importing data into the Stock Table
mycursor = mydb.cursor()

for symbol in symbols:
    
    sql = "INSERT IGNORE INTO Stock (Symbol) VALUE (%s)"
    val = (symbol)
    
    try:
        mycursor.execute(sql, (val,))
    except Exception as e:
        print(e)
        print(symbol)
    mydb.commit()

print(mycursor.rowcount, "record inserted.")

# Check to make sure data was inserted as expected!
mycursor = mydb.cursor()
mycursor.execute("SELECT * FROM Stock")
myresult = mycursor.fetchall()
len(myresult)

# Close connection to MySQL
mycursor.close()
mydb.close()
