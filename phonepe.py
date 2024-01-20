import os
import json
from PIL import Image
import plotly.express as px
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
from sqlalchemy import create_engine
import mysql.connector as sql


# DataBase connection 
mydb=sql.connect(host='localhost',port=3306,user='root',password='Momprincess@13',database='phonepe')
mycursor = mydb.cursor(buffered=True)
cursor = mydb.cursor()



Path_1=r"C:\\phonepe\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\"
agg_trans_list = os.listdir(Path_1)

columns_1 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],
            'Transaction_amount': []}
for state in agg_trans_list:
    cur_state = Path_1 + state + "/"
    agg_year_list = os.listdir(cur_state)
    
    for year in agg_year_list:
        cur_year = cur_state + year + "/"
        agg_file_list = os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            A = json.load(data)
            try:  
                for i in A['data']['transactionData']:
                    name = i['name']
                    count = i['paymentInstruments'][0]['count']
                    amount = i['paymentInstruments'][0]['amount']
                    columns_1['Transaction_type'].append(name)
                    columns_1['Transaction_count'].append(count)
                    columns_1['Transaction_amount'].append(amount)
                    columns_1['State'].append(state)
                    columns_1['Year'].append(year)
                    columns_1['Quarter'].append(int(file.strip('.json')))
            except:
                pass
                
df_agg_trans = pd.DataFrame(columns_1)
print(df_agg_trans)

mycursor.execute("CREATE TABLE IF NOT EXISTS aggregated_trans(State varchar(100), Year int, Quarter int, Transaction_type varchar(100), Transaction_count int, Transaction_amount double)")
for i,row in df_agg_trans.iterrows():
    sql = "INSERT INTO aggregated_trans VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()


Path_2 =r"C:\\phonepe\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\"
agg_user_list = os.listdir(Path_2)

columns_2 = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'Count': [], 'Percentage': []}
for state in agg_user_list:
    cur_state = Path_2 + state + "/"
    agg_year_list = os.listdir(cur_state)

    for year in agg_year_list:
        cur_year = cur_state + year + "/"
        agg_file_list = os.listdir(cur_year)

        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            B = json.load(data)
            try:
                for i in B["data"]["usersByDevice"]:
                    brand_name = i["brand"]
                    counts = i["count"]
                    percents = i["percentage"]
                    columns_2["Brands"].append(brand_name)
                    columns_2["Count"].append(counts)
                    columns_2["Percentage"].append(percents)
                    columns_2["State"].append(state)
                    columns_2["Year"].append(year)
                    columns_2["Quarter"].append(int(file.strip('.json')))
            except:
                pass
df_agg_user = pd.DataFrame(columns_2)
print(df_agg_user)

mycursor.execute("CREATE TABLE IF NOT EXISTS aggregate_user (State varchar(100), Year int, Quarter int, Brands varchar(100), Count int, Percentage double)")
for i,row in df_agg_user.iterrows():
    sql = "INSERT INTO aggregate_user VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql,tuple(row))
    mydb.commit()


Path_3 =r"C:\\phonepe\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\"
map_Tran_List = os.listdir(Path_3)
columns_3 = {'State':[], 'Year':[], 'Quarter':[], 'District':[], 'Count':[], 'Amount':[]}
for state in map_Tran_List:
    cur_state = Path_3 + state + "/"
    map_year_list =os.listdir(cur_state)
    
    for year in map_year_list:
        cur_year =  cur_state+ year + "/"
        map_file_list = os.listdir(cur_year)
        
        for file in map_file_list:
            cur_file = cur_year +file
            data = open(cur_file,'r')
            c = json.load(data)
            try:
                for i in c["data"]["hoverDataList"]:
                    district = i["name"]
                    count = i["metric"][0]["count"]
                    amount = i["metric"][0]["amount"]
                    columns_3["District"].append(district)
                    columns_3["Count"].append(count)
                    columns_3["Amount"].append(amount)
                    columns_3['State'].append(state)
                    columns_3['Year'].append(year)
                    columns_3['Quarter'].append(int(file.strip('.json')))
            except:
                pass
Map_Trans = pd.DataFrame(columns_3)                
print(Map_Trans)

mycursor.execute("CREATE TABLE IF NOT EXISTS map_trans (State varchar(100),Year int, Quarter int, District varchar(100), Count int, Amount double)")
for i,row in Map_Trans.iterrows():
    sql = "INSERT INTO map_trans VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql,tuple(row))
    mydb.commit()


Path_4 =r"C:\\phonepe\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\"
map_user = os.listdir(Path_4)
Columns_4  = {'State':[], 'Year':[], 'Quarter':[], 'District':[], 'RegisteredUser':[], 'AppOpens':[]}
for state in map_user:
    cur_state = Path_4 + state + "/"
    map_year_list = os.listdir(cur_state)
    for year in map_year_list:
        cur_year = cur_state+year+"/"
        map_list = os.listdir(cur_year)
        for file in map_list:
            cur_file = cur_year+file
            data = open(cur_file,'r')
            d = json.load(data)
            try:
                for i in d["data"]["hoverData"].items():
                    district = i[0]
                    registereduser = i[1]["registeredUsers"]
                    appOpens = i[1]['appOpens']
                    Columns_4["District"].append(district)
                    Columns_4["RegisteredUser"].append(registereduser)
                    Columns_4["AppOpens"].append(appOpens)
                    Columns_4['State'].append(state)
                    Columns_4['Year'].append(year)
                    Columns_4['Quarter'].append(int(file.strip('.json')))
            except:
                pass
data_map_User = pd.DataFrame(Columns_4)
print(data_map_User)

mycursor.execute("CREATE TABLE IF NOT EXISTS map_user (State varchar(100), Year int, Quarter int, District varchar(100), RegisteredUser int, AppOpens int)")

for i, row in data_map_User.iterrows():
    sql = ("INSERT INTO map_user (State, Year, Quarter, District, RegisteredUser, AppOpens) VALUES (%s, %s, %s, %s, %s, %s)")
    values = (row['State'], row['Year'], row['Quarter'], row['District'], row['RegisteredUser'], row['AppOpens'])
    mycursor.execute(sql, values)
    mydb.commit()



Path_5 =r"C:\\phonepe\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\"
top_trans_list = os.listdir(Path_5)
Columns_5 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [],'Transaction_amount': []}
for state in top_trans_list:
    cur_state = Path_5 + state + "/"
    top_year_list = os.listdir(cur_state)
    for year in top_year_list:
        cur_year = cur_state + year + "/"
        top_file_list = os.listdir(cur_year)
        for file in top_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            A = json.load(data)
            try:
                for i in A['data']['pincodes']:
                    name = i['entityName']
                    count = i['metric']['count']
                    amount = i['metric']['amount']
                    Columns_5['Pincode'].append(name)
                    Columns_5['Transaction_count'].append(count)
                    Columns_5['Transaction_amount'].append(amount)
                    Columns_5['State'].append(state)
                    Columns_5['Year'].append(year)
                    Columns_5['Quarter'].append(int(file.strip('.json')))
            except:
                pass
df_top_trans = pd.DataFrame(Columns_5)
print(df_top_trans)

mycursor.execute("CREATE TABLE IF NOT EXISTS top_trans (State varchar(100), Year int, Quarter int, Pincode int, Transaction_count int, Transaction_Amount double)")
for i,row in df_top_trans.iterrows():
    sql = ("INSERT INTO top_trans VALUES (%s,%s,%s,%s,%s,%s)")
    mycursor.execute(sql,tuple(row))
    mydb.commit()


Path_6 ="C:\\phonepe\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\"
user_data = os.listdir(Path_6)
Columns_6 = {'State':[], 'Year':[], 'Quarter':[], 'Pincode':[], 'RegisteredUsers':[]}
for state in user_data:
    cur_state=Path_6 + state + "/"
    Year_data = os.listdir(cur_state)
    for year in Year_data:
        cur_year =cur_state + year + "/"
        Top_file = os.listdir(cur_year)
        for file in Top_file:
            cur_file = cur_year+file
            data = open(cur_file,'r')
            Data = json.load(data)
            try:
                for i in Data['data']['pincodes']:
                    name = i['name']
                    registeredUsers = i['registeredUsers']
                    Columns_6['Pincode'].append(name)
                    Columns_6['RegisteredUsers'].append(registeredUsers)
                    Columns_6['State'].append(state)
                    Columns_6['Year'].append(year)
                    Columns_6['Quarter'].append(int(file.strip('.json')))
            except:
                pass
Top_user_Data = pd.DataFrame(Columns_6)
print(Top_user_Data)

mycursor.execute("CREATE TABLE IF NOT EXISTS top_user (State varchar(100), Year int, Quarter int, Pincode int, RegisteredUsers int)")
for i,row in Top_user_Data.iterrows():
    sql = ("INSERT INTO top_user VALUES (%s,%s,%s,%s,%s)")
    mycursor.execute(sql,tuple(row))
    mydb.commit()


df_agg_trans.to_csv('Aggregated_Trans.csv',index=False)
df_agg_user.to_csv("Aggregated_User.csv",index=False)
Map_Trans.to_csv("Map_Trans.csv",index=False)
data_map_User.to_csv("Map_User_data.csv",index=False)
df_top_trans.to_csv("Top_Trans_data.csv",index=False)
Top_user_Data.to_csv("Top_User_data.csv",index=False)


mycursor.execute("show tables")
mycursor.fetchall()
[('aggregate_user',),
 ('aggregated_trans',),
 ('map_trans',),
 ('map_user',),
 ('top_trans',),
 ('top_user',)]

df_agg_trans['State'] = df_agg_trans['State'].replace({
    'himachal-pradesh': 'Himachal Pradesh',
    'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'gujarat': 'Gujarat',
    'madhya-pradesh': 'Madhya Pradesh',
    'arunachal-pradesh': 'Arunachal Pradesh',
    'meghalaya': 'Meghalaya',
    'jharkhand': 'Jharkhand',
    'assam': 'Assam',
    'andhra-pradesh': 'Andhra Pradesh',
    'manipur': 'Manipur',
    'mizoram': 'Mizoram',
    'ladakh': 'Ladakh',
    'chhattisgarh': 'Chhattisgarh',
    'tripura': 'Tripura',
    'tamil-nadu': 'Tamil Nadu',
    'uttarakhand': 'Uttarakhand',
    'bihar': 'Bihar',
    'goa': 'Goa',
    'Kerala': 'Kerala',
    'rajasthan': 'Rajasthan',
    'haryana': 'Haryana',
    'nagaland': 'Nagaland',
    'odisha': 'Odisha',
    'uttar-pradesh': 'Uttar Pradesh',
    'west-bengal': 'West Bengal',
    'andaman-&-nicobar-islands': 'Andaman & Nicobar',
    'telangana': 'Telangana',
    'punjab': 'Punjab',
    'delhi': 'Delhi',
    'karnataka': 'Karnataka',
    'jammu-&-kashmir': 'Jammu & Kashmir',
    'maharashtra': 'Maharashtra',
    'chandigarh': 'Chandigarh',
    'puducherry': 'Puducherry',
    'sikkim': 'Sikkim',
    'lakshadweep': 'Lakshadweep'
})

state_names = df_agg_trans['State'].unique()
state_names_df = pd.DataFrame({'state': state_names})

# Save the DataFrame to a CSV file
state_names_df.to_csv("C:\phonepe\state_names.csv", index=False)

# Read the saved CSV file
state_names_read = pd.read_csv("C:\phonepe\state_names.csv")
print(state_names_read)