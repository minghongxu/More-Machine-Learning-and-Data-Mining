#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from pyspark import SparkConf,SparkContext
import sys


# In[2]:


import time
t_start = time.time()


# In[17]:


filter_threshold = int(sys.argv[1])
support = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]
#input_file = sys.argv[1]


# In[4]:


conf = SparkConf().setAppName("inf553")
conf.set('spark.executor.memory', '4G')
conf.set('spark.driver.memory', '4G')


# In[5]:


sc = SparkContext(conf=conf)


# In[6]:





# In[6]:


def APriori(chunk):
    basketslist = list(chunk)
    ps = support * (len(basketslist)/basket_num)
    candidate = []
    candidatek = set() #init
    k = 3
    
    #singletons
    count_fisrt = {}
    for s in basketslist:
        for i in s:
            frozeni = frozenset({i})
            if frozeni in count_fisrt:
                count_fisrt[frozeni] += 1
            else:
                count_fisrt[frozeni] = 1
    for key in count_fisrt:
        if count_fisrt[key] >= ps:
            candidate.append(key)
    
    first_len = len(candidate)
    for i in range(first_len):
        for j in range(i+1, first_len):
            candidatek.add(candidate[i]|candidate[j])

    while True:
        count = {}
        candidatetemp = []
        for i in candidatek:
            for basket in basketslist:
                if i<= basket:
                    frozeni = frozenset(i)
                    if frozeni in count:
                        count[frozeni] += 1
                    else:
                        count[frozeni] = 1
        candidatek = set() #reset
        for key in count:
            if count[key] >= ps:
                candidate.append(key)
                candidatetemp.append(key)
                
        length = len(candidatetemp)
        for s1 in range(length):
            for s2 in range(s1+1,length):
                combo = candidatetemp[s1]|candidatetemp[s2]
                if len(combo) == k:
                    candidatek.add(combo)
        k += 1
        if len(candidatek) == 0:
            break
    return candidate
                    
    


# In[7]:


def reduce2(item):
    itemset = set(item)
    count = 0
    for basket in entire_basket:
        if itemset <= basket:
            count += 1
    return count >= support


# In[8]:


def create_output(itemsets):
    output = ''
    length = 1
    for i in itemsets:
        if len(i) == length:
            if len(i) == 1:
                output = output + str(i)[0:-2] + '),'
            else:
                output = output + str(i) + ','
        else:
            length += 1
            output = output[0:-1] + '\n\n' + str(i) + ','
    return output[0:-1]


# In[ ]:





# In[9]:


user_business = sc.textFile(input_file)
first = user_business.first()
simulated = user_business.filter(lambda x: x != first).map(lambda s: s.split(', '))


# In[25]:


#new version
baskets = simulated.map(lambda l: (l[0], l[1])).groupByKey().map(lambda t: set(t[1]))     .filter(lambda t: len(t) > filter_threshold)


# In[ ]:





# In[26]:


entire_basket = baskets.collect()
basket_num = len(entire_basket)


# In[ ]:





# In[20]:


candidate = baskets.mapPartitions(APriori).map(lambda s: (tuple(sorted(s)),1)).reduceByKey(lambda a, b: 1).keys()


# In[21]:


candidatesorted = candidate.sortBy(lambda x:(len(x),x)).collect()


# In[ ]:





# In[22]:


frequent_output = candidate.filter(reduce2).sortBy(lambda x:(len(x),x)).collect()


# In[ ]:





# In[23]:


with open(output_file, "w") as f:
    f.write("Candidates:\n" + create_output(candidatesorted) +             "\n\nFrequent Itemsets:\n" + create_output(frequent_output))


# In[ ]:





# In[16]:


t_end = time.time()
print('Duration:', t_end-t_start)


# In[ ]:




