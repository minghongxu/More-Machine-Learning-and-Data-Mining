#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from pyspark import SparkConf,SparkContext
import sys
import random
import math
import re


# In[ ]:





# In[16]:


train_file = sys.argv[1]
test_file = sys.argv[2]
model_file = sys.argv[3] #task3user.model
output_file = sys.argv[4]
cf_type = sys.argv[5] #item_based/user_based
#input_file = sys.argv[1]
user_avg_file = sys.argv[1][:-17] + 'user_avg.json'
business_avg_file = sys.argv[1][:-17] + 'business_avg.json'
N = 10 #neighbors


# In[3]:


import time
t_start = time.time()


# In[4]:


conf = SparkConf().setAppName("inf553")
conf.set('spark.executor.memory', '4G')
conf.set('spark.driver.memory', '4G')


# In[5]:


sc = SparkContext(conf=conf)


# In[6]:


with open(user_avg_file) as f:
    user_avg = json.load(f)
with open(business_avg_file) as f1:
    business_avg = json.load(f1)


# In[7]:


review = sc.textFile(train_file).map(lambda x:json.loads(x)).map(lambda d: (d['user_id'], d['business_id'], d['stars'])).cache()


# In[8]:


test = sc.textFile(test_file).map(lambda x:json.loads(x))


# In[21]:


model = sc.textFile(model_file).map(lambda x:json.loads(x))


# In[10]:


def item_predict(user_id,business_id):
    try:
        user_rated = user_businesses[user_id]
        weight_list = item_model[business_id]
        weight_list = sorted(weight_list, key=lambda t: t[1], reverse = True)
    except:
        return
    numerator = 0
    denominator = 0
    count = 0
    for t in weight_list:
        if t[0] in user_rated:
            count += 1
            numerator += user_rated[t[0]]*t[1]
            denominator += t[1]
        if count == N:
            break
    if denominator == 0:
        return
    else:
        return numerator/denominator
    


# In[28]:


def user_predict(user_id,business_id):
    try:
        rated_users = business_users[business_id]
        weight_list = user_model[user_id]
        weight_list = sorted(weight_list, key=lambda t: t[1], reverse = True)
    except:
        return
    numerator = 0
    denominator = 0
    count = 0
    for t in weight_list:
        if t[0] in rated_users:
            count += 1
            numerator += (rated_users[t[0]]-user_avg[t[0]])*t[1]
            denominator += t[1]
        if count == N:
            break
    if denominator == 0:
        return
    else:
        return user_avg[user_id] + numerator/denominator


# In[11]:


stars_result = []


# In[29]:


if cf_type == 'item_based':
    user_businesses = dict(review.map(lambda t: (t[0],(t[1],t[2]))).groupByKey().map(lambda t: (t[0],dict(list(t[1])))).collect())
    item_model = dict(model.flatMap(lambda d: ((d['b1'],(d['b2'],d['sim'])),(d['b2'],(d['b1'],d['sim'])))).groupByKey()         .map(lambda t: (t[0],list(t[1]))).collect()) 
    stars_result = test.map(lambda t: (t['user_id'], t['business_id'], item_predict(t['user_id'], t['business_id'])))         .collect()  
elif cf_type == 'user_based':
    business_users = dict(review.map(lambda t: (t[1],(t[0],t[2]))).groupByKey().map(lambda t: (t[0],dict(list(t[1])))).collect())
    user_model = dict(model.flatMap(lambda d: ((d['u1'],(d['u2'],d['sim'])),(d['u2'],(d['u1'],d['sim'])))).groupByKey()         .map(lambda t: (t[0],list(t[1]))).collect())
    stars_result = test.map(lambda t: (t['user_id'], t['business_id'], user_predict(t['user_id'], t['business_id'])))         .collect()  
    


# In[ ]:





# In[ ]:





# In[13]:


with open(output_file, 'w') as f:
    for t in stars_result:
        result = {}
        result['user_id'] = t[0]
        result['business_id'] = t[1]
        result['stars'] = t[2]
        json.dump(result, f)
        f.write('\n')


# In[ ]:





# In[ ]:





# In[14]:


t_end = time.time()
print('Duration:', t_end-t_start)


# In[ ]:




