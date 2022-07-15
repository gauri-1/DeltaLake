#!/usr/bin/env python
# coding: utf-8

# In[1]:


from ipynb.fs.full.trasnformation_datalake import Transform
import logging
import configparser


# In[5]:


spark=SparkSession.builder.appName("DataLake").getOrCreate()
config1 = configparser.ConfigParser(interpolation=None)
config1.read('config/config.ini')
path2 = config1['path']['path']
schema2 = config1['schema']['schema']


# In[6]:


# Logger
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# In[7]:


class Target:
    global df
    df = Transform.columnNameTitle()
    def writeDelta():
        global df
        try:
            df1=df.write.format("delta").save(r"C:\Users\GauriPawar\Ingestion-layer\DataLake\delta")
            logger.info("Data written successfully")
            return df1
        except Exception as e:
            logger.info("An error occured in writing the file")
            logger.error(e)           


# In[8]:


Target.writeDelta()


# In[ ]:




