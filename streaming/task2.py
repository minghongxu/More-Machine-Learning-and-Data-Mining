#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from pyspark import SparkConf,SparkContext
import sys
import time
import random


# In[13]:


import binascii
from pyspark.streaming import StreamingContext
import datetime


# In[2]:


port = int(sys.argv[1])
output_file = sys.argv[2]
#input_file = sys.argv[1]


# In[3]:


conf = SparkConf().setAppName("inf553")
conf.set('spark.executor.memory', '4G')
conf.set('spark.driver.memory', '4G')


# In[4]:


sc = SparkContext(conf=conf)


# In[8]:


ssc=StreamingContext(sc , 5)


# In[ ]:





# In[12]:


with open(output_file, 'w') as f:
    f.write('Time,Ground Truth,Estimation' + '\n')


# In[10]:


cities = ssc.socketTextStream('localhost', port).map(lambda x:json.loads(x)).map(lambda d: d['city'])     .filter(lambda c: c != '').map(lambda c: int(binascii.hexlify(c.encode('utf8')), 16))


# In[32]:


m = 307
p = 997
n = 24
random_a = random.sample(range(1, 500), n)
random_b = random.sample(range(1, 2000), n)


# In[ ]:





# In[31]:


def Flajolet_Martin(cityrdd):
    citylist = cityrdd.collect()
    time = str(datetime.datetime.now())[:-7]
    ground_truth = len(set(citylist))
    
    est_list = []
    for i in range(n):
        max_zeros = 0
        for x in citylist:
            a = random_a[i]
            b = random_b[i]
            binhash = bin(((a * x + b) % p) % m)
            count = 0
            strbin = str(binhash)
            for num in range(len(strbin), 2, -1):
                if strbin[num-1] == '0':
                    count += 1
                else:
                    break
            if count >= max_zeros:
                max_zeros = count
        est_list.append(2**max_zeros)
    estimation = sum(sorted(est_list)[8:-8])/(n/3)
    
    with open(output_file, 'a') as f:
        f.write(time + ',' + str(ground_truth) + ',' + str(estimation) + '\n')
    


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


cities.window(30, 10).foreachRDD(Flajolet_Martin)


# In[ ]:





# In[ ]:


ssc.start()
ssc.awaitTermination()


# In[ ]:




