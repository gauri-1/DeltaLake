#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
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


class Sources:
    def getfromCSV(spark:SparkSession, path1:str) -> DataFrame :
        try:
            logger.info("Reading the csv..")
            return (spark.read.format('csv').schema(schema2).options(header=True).load(path2))
        except Exception as e:
            logger.info(e)







