# Databricks notebook source
# MAGIC %md
# MAGIC #Installing the necessary libraries

# COMMAND ----------

!pip install barnum
!pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC #Importing the necessary libraries

# COMMAND ----------

from faker import Faker
import csv
import random
from time import time
from decimal import Decimal
import pandas as pd
from datetime import datetime
import sys
import random
import time
from barnum import gen_data
import numpy as np
import re
fake=Faker()

# COMMAND ----------

# MAGIC %md
# MAGIC #Template for the table and attributes

# COMMAND ----------

df = spark.read.option("delimiter", ",").option("header", True).csv("{path}")
df.display()
df.createOrReplaceTempView('df_table')
df1 = spark.read.option("delimiter", ",").option("header", True).csv("{path}")
df1.display()
df1.createOrReplaceTempView('df1_table')

# COMMAND ----------

# MAGIC %md
# MAGIC #Function for the fake data generation

# COMMAND ----------

def fake_data_create(a,datatype):
        array =[]
        for j in range(0,a):
            if datatype == "number" :
                array.append(fake.pyint())
                continue
            elif datatype== "company_name" :
                array.append(gen_data.create_company_name())
                continue
            elif datatype=="address":
                st=fake.address()
                line = st.replace('\n','')
                array.append(line)
                continue
            elif datatype=="country":
                array.append(fake.country())
                continue
            elif datatype=="birthday":
                array.append(fake.date_of_birth())
                continue
            elif datatype=="name": 
                array.append(fake.name())
                continue
            elif datatype=="date":
                array.append(fake.date())
                continue
            elif datatype=="phone":
                array.append(gen_data.create_phone())
                continue
            elif datatype=="email":
                array.append(gen_data.create_email())
                continue
            else:
                my_list = datatype.split(",")
                array.append(random.choice((my_list)))
                continue
#         print(array)
        return array

# COMMAND ----------

# MAGIC %md
# MAGIC #Dataframe to csv

# COMMAND ----------

from pyspark.sql import SparkSession
import numpy as np
import pyspark.sql.functions as f
Details = {}
primary_array=[]
path = "{path}"
spark = SparkSession \
    .builder \
    .appName("Python Fake Data App") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
RECORD_COUNT =[]
#taking the count of each table
count_data = spark.sql("select count from df_table order by table_id")
count = count_data.rdd.flatMap(lambda x: x).collect()
#total count of tables
total_tables = count_data.count()
#for loop for creating each table in the csv
for i in range (1,total_tables+1):
    filter_value=i
    #selecting the column names of the table
    query1 = f'''SELECT column_name
           FROM df1_table
           WHERE table_id = {filter_value}'''
    columns = spark.sql(query1)
    column_data=columns.rdd.flatMap(lambda x: x).collect()
    #selecting the format of the columns
    query2 = f'''select format from df1_table where table_id ={filter_value}'''
    type = spark.sql(query2)
    datatype=type.rdd.flatMap(lambda x: x).collect()
    #selecting the name of the table for saving the csv
    query3 = f'''SELECT table_name
           FROM df_table
           WHERE table_id = {filter_value}'''
    p = spark.sql(query3)
    n =str(p.select(f.collect_list('table_name')).first()[0])
    name = re.sub(r"[\([{''})\]]", "", n)
    
    #selecting the count of the current table needed to populate
    query1 = f'''SELECT count
                       FROM df_table
                       WHERE table_id = {filter_value}'''
    count = spark.sql(query1)
    RECORD_COUNT=count.rdd.flatMap(lambda x: x).collect()
    #assigning the count to "a"
    a= int(RECORD_COUNT[0])
    #initalise the empty array
    primary_array = np.empty(a, dtype=object)
    #selecting the primary column
    primary = spark.sql(f'''select column_name from df1_table where table_id={filter_value} and is_primary =1''')
    pr_column = str(primary.rdd.flatMap(lambda x: x).collect())
    #removing any kind of brackets and qoutes
    primary_column = re.sub(r"[\([{''})\]]", "", pr_column)
    #selecting the format of primary column
    p_type = spark.sql(f'''select format from df1_table where table_id={filter_value} and is_primary =1''')
    pr_type = str(p_type.rdd.flatMap(lambda x: x).collect())
    prime_type = re.sub(r"[\([{''})\]]", "", pr_type)
    #checking the format is number or not
    if prime_type == 'number':
        for j in range(0,a):
            primary_array[j] = int(j+10000)   
    else :
        print("no")
    #converting the data into pandas dataframe
    panda_df = pd.DataFrame(data = primary_array, 
                    index = range(0,a), 
                    columns = [primary_column])
    #selecting the total count of the columns
    c_count = spark.sql(f'''select int(count(column_name)) as count from df1_table where table_id={filter_value}''')
    col_count =str(c_count.rdd.flatMap(lambda x: x).collect())
    column_count = re.sub(r"[\([{''})\]]", "", col_count)
    #initalizing the datframe
    results = pd.DataFrame()
    #for loop upto the column counts
    for k in range(0,int(column_count)):
#                     co =int(c[i-1])
                    #selecting the each datatype from the array
                    data_c = datatype[k]
                    #creating the data by calling the function using the number of count and datatype(format)
                    data = fake_data_create(a,data_c)
                    #returning array append to the dataframe
                    df = pd.DataFrame(data, columns = [column_data[k]])
                    #concating the dataframe to the result dataframe
                    results = pd.concat([results, df], axis=1).reset_index(drop=True)  
    #assigning the unique primary data to the dataframe
    results[primary_column] = panda_df[primary_column]
    results.to_csv(path+name +'.csv', index= False)
#     display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC #Foreign key

# COMMAND ----------

#selecting the number of foreign keys in the table
c = spark.sql(f'''select count(foreign_key_id) as count from df1_table where foreign_key_id !=0''')
co = str(c.rdd.flatMap(lambda x: x).collect())
count =re.sub(r"[\([{''})\]]", "", co)
new =[]
#checking foreign key is empty or not
if count :
    #selecting the column names 
    tg = spark.sql(f'''select column_name from df1_table where foreign_key_id !=0''')
    tar = tg.rdd.flatMap(lambda x: x).collect()
    for i in range(0,int(count)):
        #selecting the one column
        target_col =tar[i]
        #source column name for the getting the data
        pr = spark.sql(f'''select column_name from df1_table where column_id = (select foreign_key_id from df1_table where column_name="{target_col}")''')
        par = str(pr.rdd.flatMap(lambda x: x).collect())
        parent_col = re.sub(r"[\([{''})\]]", "", par)
        #selecting the table name where data should be taken
        parent = spark.sql(f'''select table_name from df_table where table_id=(select table_id from df1_table where column_name ="{parent_col}")''')
        parent_n = str(parent.rdd.flatMap(lambda x: x).collect())
        parent_name = re.sub(r"[\([{''})\]]", "", parent_n)
        #target table name
        child = spark.sql(f'''select table_name from df_table where table_id=(select table_id from df1_table where column_name ="{target_col}")''')
        child_n = str(child.rdd.flatMap(lambda x: x).collect())
        child_name = re.sub(r"[\([{''})\]]", "", child_n)
        #read the csv using the names
        parent_df = pd.read_csv(path+parent_name +'.csv')
        child_df = pd.read_csv(path+child_name +'.csv')
        #count of the source table
        p_count =spark.sql(f'''select Count from df_table where table_name ="{parent_name}"''')
        pr_count =str(p_count.rdd.flatMap(lambda x: x).collect())
        parent_count = int(re.sub(r"[\([{''})\]]", "", pr_count))
        #count of the target table
        c_count =spark.sql(f'''select Count from df_table where table_name ="{child_name}"''')
        ch_count =str(c_count.rdd.flatMap(lambda x: x).collect())
        child_count =int(re.sub(r"[\([{''})\]]", "", ch_count))
        #selecting the source column as
        k =parent_df[parent_col].tolist()
        child_df[target_col] = k *int(child_count / parent_count)
#         display(child_df)
        child_df.to_csv(path+child_name +'.csv', index= False)
else :
    print("You dont have any foreign key")

# COMMAND ----------

dbutils.fs.rm("/FileStore/shared_uploads/safna.h@reflectionsinfos.com/faker_utility/order.csv",recurse=True)

# COMMAND ----------

df = spark.read.option("delimiter", ",").option("header", True).csv("{path}")
df.display()

# COMMAND ----------

# %sql
df = spark.read.option("delimiter", ",").option("header", True).csv("{path}")
df.display()

# COMMAND ----------


