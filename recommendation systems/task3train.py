#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from pyspark import SparkConf,SparkContext
import sys
import random
import math
import re


# In[2]:


train_file = sys.argv[1]
model_file = sys.argv[2] #task3user.model
cf_type = sys.argv[3] #item_based/user_based
#input_file = sys.argv[1]
user_avg_file = sys.argv[1][:-17] + 'user_avg.json'
business_avg_file = sys.argv[1][:-17] + 'business_avg.json'



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


review = sc.textFile(train_file).map(lambda x:json.loads(x)).map(lambda d: (d['user_id'], d['business_id'], d['stars'])).cache()


# In[7]:


business_avg = dict(review.map(lambda t: (t[1], t[2])).groupByKey().map(lambda t: (t[0], sum(list(t[1]))/len(t[1]))).collect())


# In[8]:


#with open(user_avg_file) as f:
#    user_avg = json.load(f)
#with open(business_avg_file) as f1:
#    business_avg = json.load(f1)


# In[ ]:





# In[9]:


n = 25
b = 25
r = 1
p = 99991
random_a = random.sample(range(1, 2000), n)
random_c = random.sample(range(1, 2000), n)


# In[10]:


def hash_value(row_num, a, c):
    return ((a * row_num + c) % p) % business_num


# In[11]:


def minhash(business_list):
    signature_vector = []
    for i in range(n):
        candidate_sig = []
        for business in business_list:
            candidate_sig.append(hash_value(business,random_a[i],random_c[i]))
        signature_vector.append(min(candidate_sig))
    return signature_vector
    
    


# In[12]:


def LSH(chunk):
    user_id = chunk[0]
    business_list = chunk[1]
    result = []
    for i in range(b):
        band = []
        for j in range(r):
            band.append(business_list[i*r+j])
        result.append(((i,tuple(band)),user_id))
    return result
            


# In[13]:


def lshfind_pairs(candidates):
    result = []
    sort_candi = sorted(candidates)
    length = len(sort_candi)
    for i in range(length):
        for j in range(i+1, length):
            result.append((sort_candi[i],sort_candi[j]))
    return result


# In[14]:


def Jaccard(b1, b2):
    sb1 = set(b1)
    sb2 = set(b2)
    return len(sb1&sb2)/len(sb1|sb2)
    


# In[ ]:





# In[ ]:





# In[15]:


def find_pairs(chunk):
    commonid = chunk[0]
    candidates = sorted(list(chunk[1]))
    length = len(candidates)
    result = []
    for i in range(length):
        for j in range(i+1, length):
            result.append(((candidates[i],candidates[j]),commonid))
    return result


# In[16]:


def Pearson_item(chunk):
    itemi, itemj = chunk[0]
    users = chunk[1]
    numerator = 0
    denominator1 = 0
    denominator2 = 0
    ri_sum = 0
    rj_sum = 0
    for u in users:
        ri_sum += business_users[itemi][u]
        rj_sum += business_users[itemj][u]
    ri_avg = ri_sum/len(users)
    rj_avg = rj_sum/len(users)
    for u in users:
        rui = business_users[itemi][u]
        ruj = business_users[itemj][u]
        numerator += (rui-ri_avg)*(ruj-rj_avg)
        denominator1 += (rui-ri_avg)**2
        denominator2 += (ruj-rj_avg)**2
    if denominator1*denominator2 == 0:
        return [(itemi,itemj,0)]
    else:
        wij = numerator/(denominator1**0.5*denominator2**0.5)
        return [(itemi,itemj,wij)]
    
    


# In[17]:


def Pearson_user(tup):
    useru, userv = tup
    items = set(user_businesses[useru])&set(user_businesses[userv])
    numerator = 0
    denominator1 = 0
    denominator2 = 0
    uu_sum = 0
    uv_sum = 0
    for i in items:
        uu_sum += user_businesses[useru][i]
        uv_sum += user_businesses[userv][i]
    uu_avg = uu_sum/len(items)
    uv_avg = uv_sum/len(items)
    for i in items:
        rui = user_businesses[useru][i]
        rvi = user_businesses[userv][i]
        numerator += (rui-uu_avg)*(rvi-uv_avg)
        denominator1 += (rui-uu_avg)**2
        denominator2 += (rvi-uv_avg)**2
    if denominator1*denominator2 == 0:
        return [(useru,userv,0)]
    else:
        wuv = numerator/(denominator1**0.5*denominator2**0.5)
        return [(useru,userv,wuv)]    
    
    
    
    


# In[ ]:





# In[18]:


if cf_type == 'item_based':
    business_pair = review.map(lambda t: (t[0],t[1])).groupByKey().flatMap(find_pairs).groupByKey()         .filter(lambda t: len(t[1]) >= 3).map(lambda t: (t[0],set(t[1])))
    business_users = dict(review.map(lambda t: (t[1],(t[0],t[2]))).groupByKey().map(lambda t: (t[0],dict(list(t[1])))).collect())
    itembased = business_pair.flatMap(Pearson_item).filter(lambda t: t[2] > 0).collect()
    with open(model_file, 'w') as f:
        for t in itembased:
            result = {}
            result['b1'] = t[0]
            result['b2'] = t[1]
            result['sim'] = t[2]
            json.dump(result, f)
            f.write('\n')
elif cf_type == 'user_based':
    user_businesses = dict(review.map(lambda t: (t[0],(t[1],t[2]))).groupByKey().map(lambda t: (t[0],dict(list(t[1])))).collect())
    businesses = sorted(review.map(lambda t: t[1]).distinct().collect())
    business_num = len(businesses)
    business_dic = {}
    for i, u in enumerate(businesses):
        business_dic[u] = i
    user = review.map(lambda t:(t[0],business_dic[t[1]])).groupByKey().map(lambda t:(t[0],list(t[1])))
    user_dict = dict(user.collect())

    signature_matrix = user.map(lambda t:(t[0], minhash(t[1]))).cache()
    candidates = signature_matrix.flatMap(LSH).groupByKey().filter(lambda t: len(t[1])>1)         .map(lambda t: list(t[1])).flatMap(lshfind_pairs).distinct()
    verified_pair = candidates.map(lambda t: ((t[0],t[1]),Jaccard(user_dict[t[0]],user_dict[t[1]])))         .filter(lambda t: t[1] >= 0.01).map(lambda t: t[0])
    user_based = verified_pair.flatMap(Pearson_user).filter(lambda t: t[2] > 0).collect()
    with open(model_file, 'w') as f:
        for t in user_based:
            result = {}
            result['u1'] = t[0]
            result['u2'] = t[1]
            result['sim'] = t[2]
            json.dump(result, f)
            f.write('\n')


# In[ ]:





# In[ ]:





# In[ ]:





# In[19]:


t_end = time.time()
print('Duration:', t_end-t_start)


# In[ ]:




