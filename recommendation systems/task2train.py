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


test_file = sys.argv[1]
model_file = sys.argv[2]
stopwords = sys.argv[3]
#input_file = sys.argv[1]


# In[3]:

import time
t_start = time.time()


conf = SparkConf().setAppName("inf553")
conf.set('spark.executor.memory', '4G')
conf.set('spark.driver.memory', '4G')


# In[4]:


sc = SparkContext(conf=conf)


# In[5]:


review = sc.textFile(test_file).map(lambda x:json.loads(x))


# In[ ]:





# In[6]:


stoplist = []
f = open(stopwords, 'r')
for line in f:
    stoplist.append(line.strip('\n'))


# In[7]:


punc = ['(', '[', ',', '.', '!', '?', ':', ';', ']', ')', ' ', '', '\n', '-', '&']
for i in punc:
    stoplist.append(i)


# In[8]:


def words_frequent(chunk):
    text_dic = {}
    business_id = chunk[0]
    text_list = chunk[1]
    for i in text_list:
        i = i.strip('([,.!?:;]) \n\'\"-')
        i = i.lower()
        try:
            i = int(i)
        except:
            if i not in stoplist:
                if i in text_dic:
                    text_dic[i] += 1
                else:
                    text_dic[i] = 1
    return (business_id, text_dic)


# In[9]:


def TFIDF(chunk):
    business_id = chunk[0]
    text_dic = chunk[1]
    maxfkj = sorted(text_dic.items(), key=lambda t: t[1], reverse = True)[0][1]
    result = {}
    for key in text_dic:
        TF = text_dic[key]/maxfkj
        IDF = math.log(N/doc_frequency[key] ,2)
        result[key] = TF*IDF
    business_profile = sorted(result.items(), key=lambda t: t[1], reverse = True)[:200]
    return (business_id, business_profile)
    


# In[10]:


def find_user_profile(chunk):
    user_id = chunk[0]
    word_list = chunk[1]
    if len(word_list) == 200:
        return (user_id, word_list)
    result = {}
    for word in word_list:
        if word not in result:
            result[word] = 1
        else:
            result[word] += 1
    user_profile = sorted(result.items(), key=lambda t: t[1], reverse = True)[:200]
    return (user_id, list(dict(user_profile)))
    


# In[ ]:





# In[11]:


business_text = review.map(lambda d: (d['business_id'],d['text'])).reduceByKey(lambda a,b: a+' '+b)     .map(lambda t: (t[0],re.split(' |,|\n|\.',t[1]))).cache()


# In[12]:


N = business_text.count()


# In[13]:


#total = business_text.map(lambda t: len(t[1])).reduce(lambda a,b: a+b)


# In[14]:


business_frequent = business_text.map(words_frequent).cache()


# In[15]:


doc_frequency = dict(business_frequent.flatMap(lambda t: t[1]).map(lambda w: (w,1)).reduceByKey(lambda a,b: a+b).collect())


# In[16]:


business_profile = dict(business_frequent.map(TFIDF).map(lambda t: (t[0], list(set(dict(t[1]))))).collect())


# In[ ]:





# In[17]:


#user_profile = dict(review.map(lambda d: (d['user_id'],d['business_id'])).groupByKey() \
#    .map(lambda t: (t[0], list(set(t[1])))).collect())


# In[18]:


user_profile = dict(review.map(lambda d: (d['user_id'],d['business_id'])).map(lambda t:(t[0], business_profile[t[1]]))     .reduceByKey(lambda a,b: a+b).map(find_user_profile).collect())


# In[ ]:





# In[ ]:





# In[19]:


result = {}
result['business_profile'] = business_profile
result['user_profile'] = user_profile


# In[20]:


with open(model_file, 'w') as f:
    json.dump(result, f)


# In[ ]:


t_end = time.time()
print('Duration:', t_end-t_start)


# In[ ]:




