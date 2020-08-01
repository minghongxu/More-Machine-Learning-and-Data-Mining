#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import sys
import time
import random


# In[2]:


import tweepy


# In[3]:


port = sys.argv[1]
output_file = sys.argv[2]
#input_file = sys.argv[1]


# In[4]:


API_key = '8QBDbPOclySMUjbfCbfEebQWm'
API_secret_key = '3CWeJ4MiUtGiPTjZG8vmw7he7PiJmoXYjerhq2vwQzYLpEiIHe'
Access_token = '1275610007967379456-L0zERFDffV5f0zmRb6Hv4JHevtPiSo'
Access_token_secret = 'DfFWLZKo6Nh9NUQO1FmY9ZugSqdkAOmJvd5SB3KmQLAKr'


# In[ ]:





# In[5]:


class NewStreamListener(tweepy.StreamListener):
    def __init__(self, output_file):
        tweepy.StreamListener.__init__(self)
        self.output_file = output_file
        self.saved_tweets = {}
        self.sequence = 0
        self.tags = []
        self.tags_count = {}

    def on_status(self, status):
        tag_list = status.entities.get('hashtags')
        self.sequence += 1
        if tag_list is not None:
            if self.sequence <= 100:
                self.tags.append(tag_list)
                for t in tag_list:
                    tag = t['text']
                    if tag in self.tags_count:
                        self.tags_count[tag] += 1
                    else:
                        self.tags_count[tag] = 1
            else:
                if random.random() < 100/self.sequence:
                    out = random.sample(self.tags, 1)[0]
                    self.tags.remove(out)
                    for t in out:
                        tag_out = t['text']
                        if self.tags_count[tag_out] == 1:
                            del self.tags_count[tag_out]
                        else:
                            self.tags_count[tag_out] -= 1
                    
                    self.tags.append(tag_list)
                    for t in tag_list:
                        tag = t['text']
                        if tag in self.tags_count:
                            self.tags_count[tag] += 1
                        else:
                            self.tags_count[tag] = 1 
                    
            
            with open(self.output_file, 'a') as f:
                f.write("The number of tweets with tags from the beginning: " + str(self.sequence)+"\n")
                result = sorted(self.tags_count.items(), key=lambda t: (-t[1],t[0]))
                for t, c in result:
                    f.write(t + ' : ' + str(c) + '\n')
                f.write('\n')
                
        
    
    


# In[ ]:





# In[ ]:





# In[18]:


word_list = ['China', 'COVID19', 'DonaldTrump', 'pandemic', 'Black', 'Lives', 'matter']


# In[12]:


myListener = NewStreamListener(output_file)


# In[15]:


auth = tweepy.OAuthHandler(API_key,API_secret_key)
auth.set_access_token(Access_token, Access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)
api.configuration()


# In[16]:


stream = tweepy.Stream(auth=auth, listener=myListener)


# In[17]:


stream.filter(track = word_list, languages=["en"])


# In[ ]:




