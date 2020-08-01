#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from pyspark import SparkConf,SparkContext
import sys
import time
import random
import copy


# In[2]:


import os


# In[3]:


t_start = time.time()


# In[4]:


input_path = sys.argv[1]
n_cluster = int(sys.argv[2])
out_file1 = sys.argv[3]
out_file2 = sys.argv[4]
#input_file = sys.argv[1]
file_list = sorted(os.listdir(input_path))


# In[5]:


# os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.6'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.6'


# In[ ]:





# In[6]:


conf = SparkConf().setAppName("inf553")
conf.set('spark.executor.memory', '4G')
conf.set('spark.driver.memory', '4G')


# In[7]:


sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')

# In[ ]:





# In[8]:


def distance(point1:list, point2:list):
    d = len(point1)
    dis = 0
    for i in range(d):
        dis += (point2[i] - point1[i])**2
    return dis**0.5


# In[9]:


def add_point(point1:list, point2:list):
    result = []
    if len(point1) == 0:
        return point2
    elif len(point2) == 0:
        return point1
    for i in range(len(point1)):
        result.append(point2[i] + point1[i])
    return result


# In[10]:


def add_point_square(original:list, point:list):
    result = []
    if len(original) == 0:
        for i in range(len(point)):
            result.append(point[i]**2)
    else:
        for i in range(len(original)):
            result.append(original[i] + point[i]**2)
    return result


# In[11]:


def list_divide(cluster:list, divisor:int):
    result = []
    for i in cluster:
        result.append(i/divisor)
    return result


# In[12]:


def closest(point, data_dict):
    c = 100000
    best_center = 0
    for key in data_dict:
        dis = distance(point, data_dict[key])
        if dis < c:
            c = dis
            best_center = key
    return (best_center,c)


# In[13]:


def find_clusters(data:dict, centers_input:dict):
    result = {}
    for i in centers_input:
        result[i] = []
    for key in data:
        center = closest(data[key], centers_input)[0]
        result[center].append(key)
    return result


# In[14]:


def find_centers(data_dict, clusters:dict):
    result = {}
    for center in clusters:
        total = []
        for index in clusters[center]:
            total = add_point(total, data_dict[index])
        result[center] = list_divide(total,len(clusters[center]))
    return result


# In[15]:


def get_std(N, SUM, SUMSQ):
    result = []
    for i in range(len(SUM)):
        result.append(((SUMSQ[i]/N)-(SUM[i]/N)**2)**0.5)
    return result


# In[16]:


def find_statistics(data_dict, centers:dict):
    result = {}
    for i in centers:
        result[i] = {'N':0, 'SUM':[], 'SUMSQ':[], 'std':[]}
    for key in data_dict:
        center = closest(data_dict[key], centers)[0]
        result[center]['N'] += 1
        result[center]['SUM'] = add_point(result[center]['SUM'], data_dict[key])
        result[center]['SUMSQ'] = add_point_square(result[center]['SUMSQ'], data_dict[key])
    for i in centers:
        result[i]['std'] = get_std(result[i]['N'], result[i]['SUM'], result[i]['SUMSQ'])
    return result


# In[17]:


def kpp_centers(data_dict, k:int):
    init = random.sample(list(data_dict), 1)[0]
    result = {0:data_dict[init]}
    for i in range(1,k):
        max_dis = 0
        key_now = 0
        for key in data_dict:
            dis = closest(data_dict[key], result)[1]
            if dis > max_dis:
                max_dis = dis
                key_now = key
        result[i] = data_dict[key_now]
    return result


# In[18]:


def Mahalanobis_distance(centroid, std, point):
    result = 0
    for i in range(len(centroid)):
        result += ((point[i]-centroid[i])/std[i])**2
    return result**0.5


# In[19]:


#K-means
def kmeans(data_dict,centers):
    kpoints = centers.copy()
    converge_dis = 0.1
    iter_count = 0
    while iter_count < 20:
        clusters = find_clusters(data_dict, kpoints)
        new_centers = find_centers(data_dict, clusters)  #new_centers: dict
        temp_dis = 0
        for key in new_centers:
            temp_dis += distance(new_centers[key], kpoints[key])
            kpoints[key] = copy.deepcopy(new_centers[key])
        if temp_dis < converge_dis:
            break
        iter_count += 1
    return kpoints


# In[20]:


def merge_cs(CS_centers, CS_statistics):
    merge_list = []
    merged = []
    new = {}
    for i in CS_centers:
        for j in CS_centers:
            if i != j and Mahalanobis_distance(CS_centers[j], CS_statistics[j]['std'], CS_centers[i]) < 2*(d**0.5):
                if i not in merged and j not in merged:
                    merge_list.append((i,j))
                    merged.append(i)
                    merged.append(j)
                    break
    for pair in merge_list:             ###########
        CS_centers[pair[0]] = list_divide(add_point(CS_centers[pair[0]],CS_centers[pair[1]]),2)
        del CS_centers[pair[1]]
    count = 0
    for key in CS_centers:
        new[count] = copy.deepcopy(CS_centers[key])
        count += 1
    return new


# In[ ]:





# In[21]:


with open(out_file2, 'w') as f:
    f.write('round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained\n')


# In[ ]:





# In[22]:


DS = {}
CS = {}
RS = {}
DS_centers = []
DS_statistics = {}
CS_centers = []
CS_statistics = {}
output = {}


# In[23]:


for file in range(len(file_list)):
    read = sc.textFile(input_path+'/'+file_list[file])
    inputdata = dict(read.map(lambda s: s.split(',')).map(lambda l: (int(l[0]),[float(i) for i in l[1:]])).collect())

    if file == 0:
        centers = kpp_centers(inputdata, 3*n_cluster) #centers = {0:..., 1:....}
        centers = kmeans(inputdata,centers)
        clusters = find_clusters(inputdata, centers)
        for key in clusters:
            if len(clusters[key]) < 5:
                for index in clusters[key]:
                    RS[index] = inputdata[index]
            else:
                for index in clusters[key]:
                    DS[index] = inputdata[index]
        DS_centers = kpp_centers(DS, n_cluster)
        DS_centers = kmeans(DS,DS_centers)
        DS_clusters = find_clusters(DS, DS_centers)
        for key in DS_clusters:
            for index in DS_clusters[key]:
                output[index] = key
        DS_statistics = find_statistics(DS, DS_centers) 
        
        if len(RS) >= 3*n_cluster:
            RS_cneters = kpp_centers(RS, 3*n_cluster)
            RS_cneters = kmeans(RS,RS_cneters)
            RS_clusters = find_clusters(RS, RS_cneters)
            for key in RS_clusters:
                if len(RS_clusters[key]) > 1:
                    for index in RS_clusters[key]:
                        CS[index] = RS[index]
                        CS_centers[key] = RS_cneters[key]
                    for index in RS_clusters[key]: 
                        del RS[index]
        CS_statistics = find_statistics(CS, CS_centers)
        
    else:
        for index in inputdata:
            d = len(inputdata[index])
            for key in DS_statistics:
                if Mahalanobis_distance(DS_centers[key], DS_statistics[key]['std'], inputdata[index]) < 2*(d**0.5):
                    DS_statistics[key]['N'] += 1
                    DS_statistics[key]['SUM'] = add_point(DS_statistics[key]['SUM'], inputdata[index])
                    DS_statistics[key]['SUMSQ'] = add_point_square(DS_statistics[key]['SUMSQ'], inputdata[index])
                    DS_statistics[key]['std'] = get_std(DS_statistics[key]['N'], DS_statistics[key]['SUM'], DS_statistics[key]['SUMSQ'])
                    DS_centers[key] = list_divide(DS_statistics[key]['SUM'], DS_statistics[key]['N'])
                    output[index] = key
                    break
            else:
                for key in CS_statistics:
                    if Mahalanobis_distance(CS_centers[key], CS_statistics[key]['std'], inputdata[index]) < 2*(d**0.5):
                        CS[index] = inputdata[index]
                        break
                else:
                    RS[index] = inputdata[index]
        
        if len(RS) >= 3*n_cluster:                          ####### finish
            RS_cneters = kpp_centers(RS, 3*n_cluster)
            RS_cneters = kmeans(RS,RS_cneters)
            RS_clusters = find_clusters(RS, RS_cneters)
            for key in RS_clusters:
                if len(RS_clusters[key]) > 1:
                    for index in RS_clusters[key]:
                        CS[index] = RS[index]
                        CS_centers[key] = RS_cneters[key]
                    for index in RS_clusters[key]: 
                        del RS[index]
        CS_statistics = find_statistics(CS, CS_centers)
                        
        CS_centers = merge_cs(CS_centers, CS_statistics) 
        CS_statistics = find_statistics(CS, CS_centers)
    
    with open(out_file2, 'a') as f:
        f.write(str(file+1)+','+str(len(DS_centers))+','+str(len(output))+','+str(len(CS_centers))+','+str(len(CS))+                 ','+str(len(RS))+'\n')
        


# In[ ]:





# In[ ]:





# In[24]:


for index in RS:
    clus_result = closest(RS[index], DS_centers)
    output[index] = clus_result[0]


# In[25]:


CS_clusters = find_clusters(CS, CS_centers)
for key in CS_centers:
    clus_result = closest(CS_centers[key], DS_centers)
    for index in CS_clusters[key]:
        output[index] = clus_result[0]


# In[ ]:





# In[26]:


output = dict(sorted(output.items(),key=lambda x:x[0]))


# In[27]:


with open (out_file1, 'w') as f:
    json.dump(output, f)


# In[ ]:





# In[ ]:





# In[29]:


t_end = time.time()
print('Duration:', t_end-t_start)


# In[ ]:




