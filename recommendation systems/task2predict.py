#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from pyspark import SparkConf,SparkContext
import sys
import random
import math


# In[2]:


test_file = sys.argv[1]
model_file = sys.argv[2]
output_file = sys.argv[3]
#input_file = sys.argv[1]


# In[12]:


import time
t_start = time.time()


# In[3]:


conf = SparkConf().setAppName("inf553")
conf.set('spark.executor.memory', '4G')
conf.set('spark.driver.memory', '4G')


# In[4]:


sc = SparkContext(conf=conf)


# In[5]:


with open(model_file) as f:
    model = json.load(f)


# In[6]:


test = sc.textFile(test_file).map(lambda x:json.loads(x))


# In[7]:


business_profile = model['business_profile']


# In[8]:


user_profile = model['user_profile']


# In[9]:


def cos_similarity(user_id,business_id):
    try:
        l1 = user_profile[user_id]
        l2 = business_profile[business_id]
    except:
        return 0
    intersection = len(set(l1)&set(l2))
    length = math.sqrt(len(l1)) * math.sqrt(len(l2))
    return intersection/length


# In[ ]:





# In[10]:


sim_list = test.map(lambda t: (t['user_id'], t['business_id'], cos_similarity(t['user_id'], t['business_id'])))     .filter(lambda t: t[2] >= 0.01).collect()


# In[ ]:





# In[ ]:





# In[ ]:





# In[11]:


with open(output_file, 'w') as fw:
    for t in sim_list:
        result = {}
        result['user_id'] = t[0]
        result['business_id'] = t[1]
        result['sim'] = t[2]
        json.dump(result, fw)
        fw.write('\n')
    


# In[ ]:





# In[ ]:


t_end = time.time()
print('Duration:', t_end-t_start)


# In[ ]:




