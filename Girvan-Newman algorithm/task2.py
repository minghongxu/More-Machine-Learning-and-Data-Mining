#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from pyspark import SparkConf,SparkContext
import sys
import time
import copy


# In[2]:


t_start = time.time()


# In[3]:


threshold = int(sys.argv[1])
input_file = sys.argv[2]
betweenness_file = sys.argv[3]
output_file = sys.argv[4]
#input_file = sys.argv[1]


# In[4]:


conf = SparkConf().setAppName("inf553")
conf.set('spark.executor.memory', '4G')
conf.set('spark.driver.memory', '4G')


# In[5]:


sc = SparkContext(conf=conf)


# In[6]:


def graph_construction(chunk):
    user = chunk[0]
    business = chunk[1]
    result = []
    for key in user_dict:
        if key != user:
            if len(business&user_dict[key]) >= threshold:
                result.append((user,key))
    return result


# In[7]:


def GN(root, graph):
    edge_contribution = {}
    node_credit = {}
    shortest_path = {}
    level = {}
    queue = [root]
    visited = []
    relationship = {}
    
    node_credit[root] = 1
    level[root] = 0
    shortest_path[root] = 1
    
    while len(queue) != 0:
        node = queue.pop(0)
        visited.append(node)
        for subnode in graph[node]:
            if subnode not in level:
                level[subnode] = level[node] + 1
                queue.append(subnode)
                shortest_path[subnode] = 0
            if level[subnode] == level[node] + 1:
                shortest_path[subnode]=shortest_path[subnode]+shortest_path[node]
                if subnode in relationship:
                    relationship[subnode].append(node)
                else:
                    relationship[subnode] = [node]
    
    visited.reverse()
    for n in nodes_list:
        node_credit[n] = 1
    for child in visited[:-1]:
        for parent in relationship[child]:
            share = node_credit[child] * (shortest_path[parent] / shortest_path[child])
            node_credit[parent] += share
            t = tuple(sorted((parent,child)))
            if t in edge_contribution:
                edge_contribution[t] += share
            else:
                edge_contribution[t] = share
        
        
    return edge_contribution
                


# In[8]:


def modularity(communities):
    Q = 0
    for community in communities:
        for i in community:
            for j in community:
                if j in graph[i]:
                    Aij = 1
                else:
                    Aij = 0
                Q += (Aij - (len(graph[i])*len(graph[j]))/(2*m))
    return Q
    


# In[9]:


def find_communities(vertices, graph):
    result = []
    queue = []
    visited = []
    for vertex in vertices:
        if vertex not in visited:
            community = [vertex]
            queue.append(vertex)
            visited.append(vertex)
            while len(queue) != 0:
                node = queue.pop(0)
                for subnode in graph[node]:
                    if subnode not in visited:
                        queue.append(subnode)
                        visited.append(subnode)
                        community.append(subnode)
            community = sorted(community)
            result.append(community)
    return result


# In[ ]:





# In[10]:


input_data = sc.textFile(input_file)
first = input_data.first()
user_business = input_data.filter(lambda x: x != first).map(lambda s: s.split(','))


# In[11]:


user_business_set = user_business.groupByKey().map(lambda t: (t[0], set(t[1]))).cache()


# In[12]:


user_dict = dict(user_business_set.collect())


# In[13]:


connect = user_business_set.flatMap(graph_construction).cache()


# In[14]:


nodes = connect.map(lambda t: t[0]).distinct()


# In[15]:


nodes_list = nodes.collect()


# In[16]:


graph = dict(connect.groupByKey().map(lambda t: (t[0], set(t[1]))).collect())


# In[ ]:





# In[17]:


betweenness = nodes.map(lambda n: GN(n,graph)).flatMap(lambda d: list(d.items())).reduceByKey(lambda a,b: a+b)     .map(lambda t: (t[0],t[1]/2)).sortBy(lambda t:(-t[1],t[0][0])).collect()


# In[18]:


m = connect.count() / 2
max_modularity = -1
graph_res = copy.deepcopy(graph)
communities_res = []


# In[19]:


while m > 0:
    communities = find_communities(nodes_list, graph_res)
    modu = modularity(communities)
    if modu > max_modularity:
        max_modularity = modu
        communities_res = communities.copy()
    betweenness_temp = nodes.map(lambda n: GN(n,graph_res)).flatMap(lambda d: list(d.items())).reduceByKey(lambda a,b: a+b)         .map(lambda t: (t[0],t[1]/2)).sortBy(lambda t:(-t[1],t[0][0])).cache()
    first = betweenness_temp.first()[1]
    remove = betweenness_temp.filter(lambda t: t[1] == first).collect()
    for between in remove:
        m -= 1
        graph_res[between[0][0]].remove(between[0][1])
        graph_res[between[0][1]].remove(between[0][0])
    


# In[20]:


communities_res = sorted(communities_res, key=lambda t: (len(t),t[0]))


# In[ ]:





# In[ ]:





# In[21]:


with open(betweenness_file, "w") as f:
    for value in betweenness:
        f.write(str(value)[1:-1] + '\n')


# In[22]:


with open(output_file, "w") as f1:
    for value in communities_res:
        f1.write(str(value)[1:-1] + '\n')


# In[ ]:





# In[23]:


t_end = time.time()
print('Duration:', t_end-t_start)


# In[ ]:




