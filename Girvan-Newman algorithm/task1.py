#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from pyspark import SparkConf,SparkContext
import sys
import time


# In[2]:


import os
from graphframes import *
from pyspark.sql.functions import col, lit, when
from pyspark.sql import SQLContext


# In[3]:


t_start = time.time()


# In[4]:


threshold = int(sys.argv[1])
input_file = sys.argv[2]
output_file = sys.argv[3]
#input_file = sys.argv[1]


# In[5]:


os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell")


# In[6]:


conf = SparkConf().setAppName("inf553")
conf.set('spark.executor.memory', '4G')
conf.set('spark.driver.memory', '4G')


# In[7]:


sc = SparkContext(conf=conf)


# In[8]:


sqlContext = SQLContext(sc)


# In[19]:


def graph_construction(chunk):
    user = chunk[0]
    business = chunk[1]
    result = []
    for key in user_dict:
        if key != user:
            if len(business&user_dict[key]) >= threshold:
                result.append((user,key))
    return result


# In[ ]:





# In[13]:


input_data = sc.textFile(input_file)
first = input_data.first()
user_business = input_data.filter(lambda x: x != first).map(lambda s: s.split(','))


# In[17]:


user_business_set = user_business.groupByKey().map(lambda t: (t[0], set(t[1]))).cache()


# In[18]:


user_dict = dict(user_business_set.collect())


# In[ ]:





# In[27]:


connect = user_business_set.flatMap(graph_construction)


# In[38]:


node = connect.map(lambda t: (t[0],)).distinct()


# In[42]:


vertices = sqlContext.createDataFrame(node, schema=['id'])


# In[43]:


edges = sqlContext.createDataFrame(connect, schema=['src','dst'])


# In[44]:


g = GraphFrame(vertices, edges)


# In[50]:


result = g.labelPropagation(maxIter=5)


# In[75]:


communities = result.rdd.map(lambda row: (row['label'], row['id'])).groupByKey().map(lambda t: (t[0], sorted(list(t[1]))))     .sortBy(lambda t:(len(t[1]),t[1][0])).collect()


# In[80]:


with open(output_file, "w") as f:
    for community in communities:
        f.write(str(community[1])[1:-1] + '\n')
    


# In[ ]:





# In[12]:


t_end = time.time()
print('Duration:', t_end-t_start)


# In[ ]:




