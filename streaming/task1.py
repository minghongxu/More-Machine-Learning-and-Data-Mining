#!/usr/bin/env python
# coding: utf-8

# In[15]:


import json
from pyspark import SparkConf,SparkContext
import sys
import time
import random


# In[6]:


import binascii


# In[2]:


t_start = time.time()


# In[3]:


first_json_path = sys.argv[1]
second_json_path = sys.argv[2]
output_file = sys.argv[3]
#input_file = sys.argv[1]


# In[4]:


conf = SparkConf().setAppName("inf553")
conf.set('spark.executor.memory', '4G')
conf.set('spark.driver.memory', '4G')


# In[5]:


sc = SparkContext(conf=conf)


# In[ ]:





# In[23]:


def hash_value(x):
    result = set()
    for i in range(n):
        a = random_a[i]
        b = random_b[i]
        result.add(((a * x + b) % p) % m)
    return result


# In[42]:


def transfer(c):
    try:
        return int(binascii.hexlify(c.encode('utf8')), 16)
    except:
        return 0


# In[47]:


def predict_result(s):
    for hs in s:
        if hs not in bloom_filter:
            return '0'
    return "1"


# In[18]:


cities = sc.textFile(first_json_path).map(lambda x:json.loads(x)).map(lambda d: d['city']).distinct()     .filter(lambda c: c != '').map(lambda c: int(binascii.hexlify(c.encode('utf8')), 16)).cache()


# In[30]:


m = 9973
p = 99991
n = 10
random_a = random.sample(range(1, 2000), n)
random_b = random.sample(range(1, 2000), n)


# In[33]:


bloom_filter = cities.map(hash_value).reduce(lambda a,b: a|b)


# In[48]:


result = sc.textFile(second_json_path).map(lambda x:json.loads(x)).map(lambda d: d['city'])     .map(transfer).map(hash_value).map(predict_result).collect()


# In[ ]:





# In[49]:


with open(output_file, 'w') as f:
    f.write(' '.join(result))


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


t_end = time.time()
print('Duration:', t_end-t_start)


# In[ ]:




