#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from pyspark import SparkConf,SparkContext
import sys


# In[19]:


review_file = 'review.json'
business_file = 'business.json'
output_file = 'user_business.csv'


# In[3]:


conf = SparkConf().setAppName("inf553")
conf.set('spark.executor.memory', '4G')
conf.set('spark.driver.memory', '4G')


# In[4]:


sc = SparkContext(conf=conf)


# In[ ]:





# In[8]:


review = sc.textFile(review_file).map(lambda x:json.loads(x))
business = sc.textFile(business_file).map(lambda x:json.loads(x))


# In[ ]:





# In[14]:


NVbusiness = business.filter(lambda d: d['state'] == 'NV').map(lambda d: d['business_id']).collect()


# In[17]:


user_business = review.filter(lambda d: d['business_id'] in NVbusiness)     .map(lambda d: (d['user_id'],d['business_id'])).collect()


# In[24]:


with open(output_file, 'w') as f:
    f.write(('user_id, business_id\n'))
    for i in user_business:
        f.write((i[0]+', '+i[1]+'\n'))
    
    


# In[ ]:





# In[ ]:





# In[ ]:




