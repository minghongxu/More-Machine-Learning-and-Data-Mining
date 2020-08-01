#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from pyspark import SparkConf,SparkContext
import sys
import random


# In[2]:


input_file = sys.argv[1]
output_file = sys.argv[2]
#input_file = sys.argv[1]
n = 50
b = 50
r = 1
p = 99991
random_a = random.sample(range(1, 2000), n)
random_c = random.sample(range(1, 2000), n)


# In[3]:


conf = SparkConf().setAppName("inf553")
conf.set('spark.executor.memory', '4G')
conf.set('spark.driver.memory', '4G')


# In[4]:


sc = SparkContext(conf=conf)


# In[ ]:





# In[5]:


def hash_value(row_num, a, c):
    return ((a * row_num + c) % p) % user_num


# In[6]:


def minhash(user_list):
    signature_vector = []
    for i in range(n):
        candidate_sig = []
        for user in user_list:
            candidate_sig.append(hash_value(user,random_a[i],random_c[i]))
        signature_vector.append(min(candidate_sig))
    return signature_vector
    
    


# In[7]:


def LSH(chunk):
    business_id = chunk[0]
    user_list = chunk[1]
    result = []
    for i in range(b):
        band = []
        for j in range(r):
            band.append(user_list[i*r+j])
        result.append(((i,tuple(band)),business_id))
    return result
            


# In[8]:


def find_pairs(candidates):
    result = []
    length = len(candidates)
    for i in range(length):
        for j in range(i+1, length):
            result.append((candidates[i],candidates[j]))
    return result


# In[9]:


def Jaccard(b1, b2):
    sb1 = set(b1)
    sb2 = set(b2)
    return len(sb1&sb2)/len(sb1|sb2)
    


# In[ ]:





# In[10]:


review = sc.textFile(input_file).map(lambda x:json.loads(x))     .map(lambda d: (d['user_id'],d['business_id']))


# In[11]:


users = sorted(review.map(lambda t: t[0]).distinct().collect())


# In[12]:


user_num = len(users)


# In[13]:


user_dic = {}
for i, u in enumerate(users):
    user_dic[u] = i


# In[14]:


business = review.map(lambda t:(t[1],user_dic[t[0]])).groupByKey().map(lambda t:(t[0],list(t[1])))


# In[15]:


business_dict = dict(business.collect())


# In[16]:


signature_matrix = business.map(lambda t:(t[0], minhash(t[1]))).cache()


# In[17]:


candidates = signature_matrix.flatMap(LSH).groupByKey().filter(lambda t: len(t[1])>1)     .map(lambda t: list(t[1])).flatMap(find_pairs).distinct()


# In[18]:


verified_pair = candidates.map(lambda t: ((t[0],t[1]),Jaccard(business_dict[t[0]],business_dict[t[1]])))     .filter(lambda t: t[1] >= 0.05).sortBy(lambda t: (t[0][0],t[0][1])).collect()


# In[ ]:





# In[19]:


with open(output_file, 'w') as f:
    for t in verified_pair:
        result = {}
        result['b1'] = t[0][0]
        result['b2'] = t[0][1]
        result['sim'] = t[1]
        json.dump(result, f)
        f.write('\n')


# In[ ]:





# In[ ]:




