#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from ipynb.fs.full.source_datalake import Sources
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import DataFrame 
from pathlib import Path
import os
import pandas as pd
import configparser
import logging


# In[2]:


# Logger
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Spark session
spark=SparkSession.builder.appName("DataLake").getOrCreate()


# In[3]:


config1 = configparser.ConfigParser(interpolation=None)
config1.read('config/config.ini')
path2 = config1['path']['path']
schema2 = config1['schema']['schema']


# In[4]:


class Transform:
    global d1
    d1 = Sources.getfromCSV(spark, path2)
    logger.info("dataframe is ready for transformation.")
    if d1.count != 0:       
        def columnNameTitle():
            global d1
            try:
                for col in d1.columns:
                    d1 = d1.withColumnRenamed(col, col.title())
            #         df=df.select(df["Region_Id"].cast('int'),df["Region_Name"].cast('string'))
                logger.info("Column name changed to title case successfully.")
                return d1
            except Exception as e:
                logger.info(e) 


# In[5]:


Transform.columnNameTitle().show(5)


# In[ ]:




