from datetime import datetime
from pymongo import MongoClient
import time
import math
import numpy as np
import os
import sys
import logging
from datetime import date
#from bson import BSON
from os.path import join
from bson.json_util import dumps

# Mongo db version 3.2.10
# Mongo authentication: CR https://stackoverflow.com/questions/29006887/mongodb-cr-authentication-failed
    
#TO DO 
#Create class Tweet with all the attributes in the Message Repository:
#_id,className,oId,createdDate,text,source,fromUser,toUsers,date,language,location,favs,share,sentiment

class Tweet:
    def __init__(self, _id, text, fromUser, date, sentiment):       #mandatory parameters
        self._id = _id
        self.text = text
        self.fromUser = fromUser
        self.date = date
        self.sentiment = sentiment
        
    def setClassName(self, className):
        self.className = className
        
    def setOId(self, oId):
        self.oId = oId
        
    def setCreatedDate(self, createdDate):
        self.createdDate = createdDate
    
    def setSource(self, source):   
        self.source= source
        
    def setToUsers(self, toUsers):   
        self.toUsers = toUsers
        
    def setLanguage(self, language):   
        self.language = language
        
    def setLocation(self, location):   
        self.location = location
        
    def setFavs(self, favs):   
        self.favs = favs
    
    def setShare(self, share):   
        self.share = share
        
    def __str__(self):
        return "_id : " + str(self._id) + ", text : " + str(self.text) + ", sentiment : " + str(self.sentiment)


"""
Input
logger: log handler - Logger
func : function that generates the log - string
param: wrong parameter that causes the error - string
"""
def writeLogs(logger, func, param):
    if(logger is None):
        raise Exception()
    if(func is None):
        raise Exception()
    if(param is None):
        raise Exception()
    logger.setLevel(logging.DEBUG)
    d = {'clientip': '127.0.0.1', 'user': 'root'}   #custom info
    fh = logging.FileHandler('emotionalcity.log')
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    logger.debug("%s [main] ERROR - Wrong or missing parameter %s in function %s", date.today(), param, func, extra=d)
    

"""
Input
tweet : Tweet in the DB - JSON
logger: log handler - Logger

Output
newTweet: - Instance of the class Tweet - Tweet
"""
def createTweet(tweet, logger):
    if(logger is None):
        raise Exception()
    if("_id" not in tweet):
        writeLogs(logger, "createTweet", "_id")
        raise Exception()
    if("text" not in tweet):
        writeLogs(logger, "createTweet", "text")
        raise Exception()
    if("fromUser" not in tweet):
        writeLogs(logger, "createTweet", "fromUser")
        raise Exception()
    if("date" not in tweet):
        writeLogs(logger, "createTweet", "date")
        raise Exception()
    if("sentiment" not in tweet):
        writeLogs(logger, "createTweet", "date")
        raise Exception()
        
    _id = tweet['_id']
    text = tweet['text']
    fromUser = tweet['fromUser']
    date = tweet['date']
    sentiment = tweet['sentiment']
    newTweet = Tweet(_id, text, fromUser, date, sentiment)
    if("classname" in tweet):
        newTweet.setClassName(tweet['className'])
        if("oId" in tweet):
            newTweet.setOId(tweet['oId'])
            if("createdDate" in tweet):
                newTweet.setCreatedDate(tweet['createdDate'])
            if("source" in tweet):
                newTweet.setSource(tweet['source'])
            if("toUsers" in tweet):
                newTweet.setToUsers(tweet['toUsers'])
            if("language" in tweet):
                newTweet.setLanguage(tweet['language'])
            if("location" in tweet):
                newTweet.setLocation(tweet['location'])
            if("favs" in tweet):
                newTweet.setFavs(tweet['favs'])
            if("shares" in tweet):
                newTweet.setShares(tweet['shares'])
                
    return newTweet

"""
Input
db: DB instance - Database
start : Unix timestamp in seconds - int
end : Unix timestamp in seconds - int
time_range : Length of the interval in seconds - int
only_geo : Consider only tweets that are geolocated - boolean
logger: log handler - Logger

Output
[d0,d1,...,dN]

N = (end-start)/time_range

d0 : (i0,i1,count)
i0 : Interval start
i1 : Interval end
If only_geo==False:
count : Integer - Number of tweets gathered in the interval (i0,i1)
Else:
count : Integer - Number of tweets geolocated, gathered in the interval (i0,i1)

WARNING The attribute in the Message Repository that refers to the tweet creation is date that is not a timestamp 
(The attribute created_date instead refers to the date when a tweet is stored in mongodb)
"""
def count_tweets(db, start, end, time_range, only_geo, logger):
    """
    i0 = calendar.timegm(datetime.utcnow().utctimetuple())
    i1 = calendar.timegm(datetime.utcnow().utctimetuple())
    count = 4
    d0 = (i0,i1,count)
    i0 = calendar.timegm(datetime.utcnow().utctimetuple())
    i1 = calendar.timegm(datetime.utcnow().utctimetuple())
    count = 100
    d1 = (i0,i1,count)
    result = [d0,d1]
    return result

    """
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "count_tweets", "db")
        raise Exception()
    if(start is None):
        writeLogs(logger, "count_tweets", "start")
        raise Exception()
    if(end is None):
        writeLogs(logger, "count_tweets", "end")
        raise Exception()
    if(time_range is None):
        writeLogs(logger, "count_tweets", "time_range")
        raise Exception()
    if(start >= end):
        writeLogs(logger, "count_tweets", "start, end")
        raise Exception()
    if(time_range <= 0):
        writeLogs(logger, "count_tweets", "time_range")
        raise Exception()
    if(start < 0):
       writeLogs(logger, "count_tweets", "start")
       raise Exception()
    if(end < 0):
        writeLogs(logger, "count_tweets", "end")
        raise Exception()
        
    if(only_geo == True):
        tweets = list(db.Message.find({"$and": [
            {"date": {              #date filter
                "$gte": int2time(start),
                "$lt": int2time(end)
            }},
            {"location": {"$exists": True} }        #geo filter
        ]}))
    else:
        tweets = list(db.Message.find({"date": {
            "$gte": int2time(start),
            "$lt": int2time(end)
        }} ))
        
    n_ranges = math.ceil((end-start)/time_range)    # no of ranges
    ranges = np.zeros((n_ranges, 3))
    lower = start;
    upper = start + time_range
    sum = 0
    for i in range (0, n_ranges):
        ranges[i][0] = lower
        ranges[i][1] = upper 
        lower = upper 
        upper += time_range
    for i in range(0, len(tweets)):
        ranges[int((time2int(tweets[i]['date'])-start)/time_range)][2] += 1
        sum += 1
    return (ranges, sum)

"""
Input
db: DB instance - Database
start : Unix timestamp - timestamp
end : Unix timestamp - timestamp
time_range : Length of the interval in seconds - int
only_geo : Consider only tweets that are geolocated - boolean
logger: log handler - Logger

Output
[d0,d1,...,dN]

N = (end-start)/time_range

d0 : (i0,i1,count)
i0 : Interval start
i1 : Interval end
If only_geo==False:
count : Integer - Number of tweets gathered in the interval (i0,i1)
Else:
count : Integer - Number of tweets geolocated, gathered in the interval (i0,i1)

WARNING The attribute in the Message Repository that refers to the tweet creation is date that is not a timestamp 
(The attribute created_date instead refers to the date when a tweet is stored in mongodb)
"""
def count_tweets_time(db, start, end, time_range, only_geo, logger):
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "count_tweets_time", "db")
        raise Exception()
    if(start is None):
        writeLogs(logger, "count_tweets_time", "start")
        raise Exception()
    if(end is None):
        writeLogs(logger, "count_tweets_time", "end")
        raise Exception()
    if(time_range is None):
        writeLogs(logger, "count_tweets_time", "time_range")
        raise Exception()
    if(start >= end):
        writeLogs(logger, "count_tweets_time", "start, end")
        raise Exception()
    if(time_range <= 0):
        writeLogs(logger, "count_tweets_time", "time_range")
        raise Exception()
        
    return count_tweets(db, time2int(start), time2int(end), time_range, only_geo, logger)

"""
Input
time: Timestamp - Unix timestamp 
    YYYY-MM-DD HH-MM-DD

Output
seconds: Distance in seconds from the 1970/01/01 - int
      
"""
def time2int(timestamp):
    return int(time.mktime(timestamp.timetuple()))


"""
Input
dist: Distance in seconds from the 1970/01/01 - int
    
Output
time: Timestamp - Unix timestamp
    YYYY-MM-DD HH-MM-DD  
"""
def int2time(seconds):
    return datetime.fromtimestamp(seconds)

"""
Input
date: Date in format YYYY/MM/DD - string
    
Output
seconds: Distance in seconds from the 1970/01/01 - int
"""
def string2int(date):
    return int(time.mktime(datetime.strptime(date, "%Y/%m/%d %H:%M:%S").timetuple()))

"""
Input
db: DB instance - Database
start : Unix timestamp in seconds - int
end : Unix timestamp in seconds - int
time_range : Length of the interval in seconds - int
only_geo : Consider only tweets that are geolocated - boolean
logger: log handler - Logger

Output
[d0,d1,...,dN]

N = (end-start)/time_range

d0 : (i0,i1,sentiment)
i0 : Interval start
i1 : Interval end
If only_geo==False:
sentiment : Float - Mean value of tweets sentiment in the interval (i0,i1)
Else:
sentiment : Float - Mean value of geolocated tweets sentiment in the interval (i0,i1)

WARNING The attribute in the Message repository that refers to the tweet creation is date that is not a timestamp 
(The attribute created_date instead refers to the date when a tweet is stored in mongodb)
"""
def senti_tweets(db, start, end, time_range, only_geo, logger):
    """
    i0 = calendar.timegm(datetime.utcnow().utctimetuple())
    i1 = calendar.timegm(datetime.utcnow().utctimetuple())
    sentiment = 0.8
    d0 = (i0,i1,sentiment)
    i0 = calendar.timegm(datetime.utcnow().utctimetuple())
    i1 = calendar.timegm(datetime.utcnow().utctimetuple())
    sentiment = 0.4
    d1 = (i0,i1,sentiment)
    result = [d0,d1]
    return result
    """    
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "senti_tweets", "db")
        raise Exception()
    if(start == None):
        writeLogs(logger, "senti_tweets", "start")
        raise Exception()
    if(end == None):
        writeLogs(logger, "senti_tweets", "end")
        raise Exception()
    if(time_range == None):
        writeLogs(logger, "senti_tweets", "time_range")
        raise Exception()
    if(start >= end):
        writeLogs(logger, "senti_tweets", "start, end")
        raise Exception()
    if(time_range <= 0):
        writeLogs(logger, "senti_tweets", "time_range")
        raise Exception()
    if(start < 0):
       writeLogs(logger, "senti_tweets", "start")
       raise Exception()
    if(end < 0):
        writeLogs(logger, "senti_tweets", "end")
        raise Exception()
        
    if(only_geo == True):
        tweets = list(db.Message.find({"$and": [
            {"location": {"$exists": True} }, 
            {"date": {
                "$gte": int2time(start),
                "$lt": int2time(end)
            }}, 
        ]}))
    else:
        tweets = list(db.Message.find({"date": {
            "$gte": int2time(start),
            "$lt": int2time(end)
        }}))
        
    n_ranges = math.ceil((end-start)/time_range)    
    ranges = np.zeros((n_ranges, 4))
    lower = start;
    upper = start + time_range
    for i in range (0, n_ranges):
        ranges[i][0] = lower
        ranges[i][1] = upper 
        lower = upper 
        upper += time_range
    for i in range(0, len(tweets)):
        ranges[int((time2int(tweets[i]['date'])-start)/time_range)][2] += 1
        ranges[int((time2int(tweets[i]['date'])-start)/time_range)][3] += tweets[i]['sentiment']
            
    for i in range (0, n_ranges):  
        if(ranges[i][2] > 0):
            ranges[i][3] /= ranges[i][2]
    
    return ranges

"""
Input
db: DB instance - Database
start : Unix timestamp - timestamp
end : Unix timestamp - timestamp
time_range : Length of the interval in seconds - int
only_geo : Consider only tweets that are geolocated - boolean
logger: log handler - Logger

Output
[d0,d1,...,dN]

N = (end-start)/time_range

d0 : (i0,i1,sentiment)
i0 : Interval start
i1 : Interval end
If only_geo==False:
sentiment : Float - Mean value of tweets sentiment in the interval (i0,i1)
Else:
sentiment : Float - Mean value of geolocated tweets sentiment in the interval (i0,i1)

WARNING The attribute in the Message repository that refers to the tweet creation is date that is not a timestamp 
(The attribute created_date instead refers to the date when a tweet is stored in mongodb)
"""
def senti_tweets_time(db, start, end, time_range, only_geo, logger):  
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "senti_tweets_time", "db")
        raise Exception()
    if(start == None):
        writeLogs(logger, "senti_tweets_time", "start")
        raise Exception()
    if(end == None):
        writeLogs(logger, "senti_tweets_time", "end")
        raise Exception()
    if(time_range == None):
        writeLogs(logger, "senti_tweets_time", "time_range")
        raise Exception()
    if(start >= end):
        writeLogs(logger, "senti_tweets_time", "start, end")
        raise Exception()
    if(time_range <= 0):
        writeLogs(logger, "senti_tweets_time", "time_range")
        raise Exception()
        
    return senti_tweets(db, time2int(start), time2int(end), time_range, only_geo, logger)

"""
Input
db: DB instance - Database
time : Unix timestamp in seconds - int
geo : List of couples of two Doubles (latitude,longitude) - example [(90.,1.)] - List<(Double,Double)> 
logger: log handler - Logger 

Output
count - int

count : Number of tweets gathered from the location inside geo and after time
https://docs.mongodb.com/manual/reference/operator/query-geospatial/
The function should filtering those messages created after the UNIX timestamp time.
WARNING The attribute in the Message repository that refers to the tweet creation is date that is not a timestamp 
(The attribute created_date instead refers to the date when a tweet is stored in mongodb)
"""
def geo_count_tweets(db, time, geo, logger):
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "geo_count_tweets", "db")
        raise Exception()
    if(time is None):
        writeLogs(logger, "geo_count_tweets", "time")
        raise Exception()
    if(geo is None):
        writeLogs(logger, "geo_count_tweets", "geo")
        raise Exception()
    if(time < 0):
        writeLogs(logger, "geo_count_tweets", "time")
        raise Exception()
        
    tweets = list(db.Message.find({"$and": [
        {"location": {"$geoWithin": {"$box": geo} } }, 
        {"date": {"$gte": int2time(time)} } 
    ]}))
    return len(tweets)

"""
Input
db: DB instance - Database
time : Unix timestamp in seconds - timestamp
geo : List of couples of two Doubles (latitude,longitude) - example [(90.,1.)] - List<(Double,Double)> 
logger: log handler - Logger 

Output
count - int

count : Number of tweets gathered from the location inside geo and after time
https://docs.mongodb.com/manual/reference/operator/query-geospatial/
The function should filtering those messages created after the UNIX timestamp time.
WARNING The attribute in the Message repository that refers to the tweet creation is date that is not a timestamp 
(The attribute created_date instead refers to the date when a tweet is stored in mongodb)
"""
def geo_count_tweets_time(db, time, geo, logger):
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "geo_count_tweets", "db")
        raise Exception()
    if(time is None):
        writeLogs(logger, "geo_count_tweets", "time")
        raise Exception()
    if(geo is None):
        writeLogs(logger, "geo_count_tweets", "geo")
        raise Exception()
        
    return geo_count_tweets(db, time2int(time), geo, logger)

"""
Input
db: DB instance - Database
time : Unix timestamp in seconds - int
geo : List of couples of two Doubles (latitude,longitude) - example [(90.,1.)] -  List<(Double,Double)> 
logger: log handler - Logger

Output
sentiment : Mean value of tweets sentiment of tweets gathered from a location inside geo and after time - float

https://docs.mongodb.com/manual/reference/operator/query-geospatial/
The function should filtering those messages created after the UNIX timestamp time.
WARNING The attribute in the Message repository that refers to the tweet creation is date that is not a timestamp 
(The attribute created_date instead refers to the date when a tweet is stored in mongodb)
"""
def geo_senti_tweets(db, time, geo_box, logger):
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "geo_senti_tweets", "db")
        raise Exception()
    if(time is None):
        writeLogs(logger, "geo_senti_tweets", "time")
        raise Exception()
    if(geo_box is None):
        writeLogs(logger, "geo_senti_tweets", "geo_box")
        raise Exception()
    if(time < 0):
        writeLogs(logger, "geo_senti_tweets", "geo_box")
        raise Exception()
        
    tweets = list(db.Message.find({"$and": [
        {"location": {"$geoWithin": {"$box": geo_box} } }, 
        {"date": {"$gte": int2time(time)} } 
    ]}))
    
    sentiment = 0
    for i in range(0, len(tweets)):
        sentiment += tweets[i]['sentiment']
    return sentiment/len(tweets) if len(tweets) > 0 else 0 

"""
Input
db: DB instance - Database
time : Unix timestamp - timestamp
geo : List of couples of two Doubles (latitude,longitude) - example [(90.,1.)] -  List<(Double,Double)> 
logger: log handler - Logger

Output
sentiment : Mean value of tweets sentiment of tweets gathered from a location inside geo and after time - float

https://docs.mongodb.com/manual/reference/operator/query-geospatial/
The function should filtering those messages created after the UNIX timestamp time.
WARNING The attribute in the Message repository that refers to the tweet creation is date that is not a timestamp 
(The attribute created_date instead refers to the date when a tweet is stored in mongodb)
"""
def geo_senti_tweets_time(db, time, geo_box, logger):
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "geo_senti_tweets", "db")
        raise Exception()
    if(time is None):
        writeLogs(logger, "geo_senti_tweets", "time")
        raise Exception()
    if(geo_box is None):
        writeLogs(logger, "geo_senti_tweets", "geo_box")
        raise Exception()
    
    return geo_senti_tweets(db, time2int(time), geo_box, logger)

"""
Input
db: DB instance - Database
start : Unix timestamp - int
end : Unix timestamp - int
query : words - List<str>
logger: log handler - Logger

Search in tweets the query tokens

Output
[t0,t1,...,tN] - Tweets gathered in the interval (start,end)

t0 : Instance of class Tweet - Tweet

WARNING IN MONGODB THE ATTRIBUTE TEXT IS INDEXED 
https://docs.mongodb.com/manual/text-search/
WARNING The attribute in the Message repository that refers to the tweet creation is date that is not a timestamp 
(The attribute created_date instead refers to the date when a tweet is stored in mongodb)
"""
#_id,className,oId,createdDate,text,source,fromUser,toUsers,date,language,location,favs,share,sentiment

def search_tweets(db, start, end, query, logger):
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "search_tweets", "db")
        raise Exception()
    if(start is None):
        writeLogs(logger, "search_tweets", "start")
        raise Exception()
    if(end is None):
        writeLogs(logger, "search_tweets", "end")
        raise Exception()
    if(query is None):
        writeLogs(logger, "search_tweets", "query")
        raise Exception()
    if(start >= end):
        writeLogs(logger, "search_tweets", "start, end")
        raise Exception()
    if(start < 0):
       writeLogs(logger, "search_tweets", "start")
       raise Exception()
    if(end < 0):
        writeLogs(logger, "search_tweets", "end")
        raise Exception()
        
    q = ""
    for s in query:
        q += s + " "
    tweets = list(db.Message.find({"$and": [
            {"date": {
                "$gte": int2time(start),
                "$lt": int2time(end)
            }},
            { "$text": { "$search": q } }       # text filter
        ]}))
    
    listTweets = []
    for tweet in tweets:
        newTweet = createTweet(tweet, logger) 
        listTweets.append(newTweet)
    return listTweets

"""
Input
db: DB instance - Database
start : Unix timestamp - timestamp
end : Unix timestamp - timestamp
query : words - List<str>
logger: log handler - Logger

Search in tweets the query tokens

Output
[t0,t1,...,tN] - Tweets gathered in the interval (start,end)

t0 : Instance of class Tweet - Tweet

WARNING IN MONGODB THE ATTRIBUTE TEXT IS INDEXED 
https://docs.mongodb.com/manual/text-search/
WARNING The attribute in the Message repository that refers to the tweet creation is date that is not a timestamp 
(The attribute created_date instead refers to the date when a tweet is stored in mongodb)
"""
#_id,className,oId,createdDate,text,source,fromUser,toUsers,date,language,location,favs,share,sentiment

def search_tweets_time(db, start, end, query, logger):
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "search_tweets_time", "db")
        raise Exception()
    if(start is None):
        writeLogs(logger, "search_tweets_time", "start")
        raise Exception()
    if(end is None):
        writeLogs(logger, "search_tweets_time", "end")
        raise Exception()
    if(query is None):
        writeLogs(logger, "search_tweets_time", "query")
        raise Exception()
    if(start >= end):
        writeLogs(logger, "search_tweets_time", "start, end")
        raise Exception()
        
    return search_tweets(db, time2int(start), time2int(end), query, logger)

"""
Input
db : DB instance - Database
query : words - List<str>
sentiment_min : Minimum sentiment - float
sentiment_max : Maximum sentiment - float
geo : List of couples of two Doubles (latitude,longitude) - example [(90.,1.),(90,0.2),..] - List<(Double,Double)> 
time : Time - int
logger : log handler - Logger

Search in tweets the query tokens

Output
[t0,t1,...,tN] - Tweets gathered in the interval (start,end)

t0 : Tweet (instance of class Tweet)

If sentiment_min is not None 
   If sentiment_max is not None 
      search only where sentiment-max >= tweet.sentiment >= sentiment-min
   Else
      search only where tweet.sentiment >= sentiment-min
Else
   If sentiment_max is not None 
      search only where tweet.sentiment <= sentiment-max

If geo is not None search only where tweet.location is inside geo object.
If time is not None search only where tweet.date < time

WARNING IN MONGODB THE ATTRIBUTE TEXT IS INDEXED 
https://docs.mongodb.com/manual/text-search/
"""
def search_geo_tweets(db, query, sentiment_min, sentiment_max, geo, time, logger):
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "search_geo_tweets", "db")
        raise Exception()
    if(time < 0):
       writeLogs(logger, "search_geo_tweets", "time")
       raise Exception() 
    
    if(query is not None):
        q = ""
        for s in query:
            q += s + " "
    if(sentiment_min is None):
        sentiment_min = -1
    if(sentiment_max is None):
        sentiment_max = 1
        
    if(geo is not None or time is not None or query is not None):
        if(geo is not None and time is not None and query is not None):
            tweets = list(db.Message.find({"$and": [
                {"location": {"$geoWithin": {"$box": geo} } },
                {"date": {"$lt": int2time(time) } },
                {"sentiment": {"$gte": sentiment_min } }, 
                {"sentiment": {"$lte": sentiment_max } },
                { "$text": { "$search": q } }
            ]}))
        else:
            if(geo is not None and time is not None):
               tweets = list(db.Message.find({"$and": [
                    {"location": {"$geoWithin": {"$box": geo} } },
                    {"sentiment": {"$gte": sentiment_min } }, 
                    {"sentiment": {"$lte": sentiment_max } },
                    {"date": {"$lt": int2time(time) } },
                ]})) 
            else:
                if(geo is not None and query is not None):
                    tweets = list(db.Message.find({"$and": [
                        {"location": {"$geoWithin": {"$box": geo} } },
                        {"sentiment": {"$gte": sentiment_min } }, 
                        {"sentiment": {"$lte": sentiment_max } },
                        { "$text": { "$search": q } }
                    ]}))
                else:
                    if(time is not None and query is not None):
                       tweets = list(db.Message.find({"$and": [
                            {"date": {"$lt": int2time(time) } },
                            {"sentiment": {"$gte": sentiment_min } }, 
                            {"sentiment": {"$lte": sentiment_max } },
                            { "$text": { "$search": q } }
                        ]}))
                    else:
                        if(geo is not None):
                            tweets = list(db.Message.find({"$and": [
                                {"location": {"$geoWithin": {"$box": geo} } },
                                {"sentiment": {"$gte": sentiment_min } }, 
                                {"sentiment": {"$lte": sentiment_max } }
                            ]}))
                        else:
                            if(time is not None):
                              tweets = list(db.Message.find({"$and": [
                                    {"date": {"$lt": int2time(time) } },
                                    {"sentiment": {"$gte": sentiment_min } }, 
                                    {"sentiment": {"$lte": sentiment_max } }
                                ]})) 
                            else:
                                if(query is not None):
                                    tweets = list(db.Message.find({"$and": [
                                        {"sentiment": {"$gte": sentiment_min } }, 
                                        {"sentiment": {"$lte": sentiment_max } },
                                        { "$text": { "$search": q } }
                                    ]}))
    else:
        tweets = list(db.Message.find({"$and": [
            {"sentiment": {"$gte": sentiment_min } }, 
            {"sentiment": {"$lte": sentiment_max } }
        ]}))
    
    listTweets = []
    for tweet in tweets:
        newTweet = createTweet(tweet, logger) 
        listTweets.append(newTweet)
            
    return listTweets

"""
Input
db : DB instance - Database
query : words - List<str>
sentiment_min : Minimum sentiment - float
sentiment_max : Maximum sentiment - float
geo : List of couples of two Doubles (latitude,longitude) - example [(90.,1.),(90,0.2),..] - List<(Double,Double)> 
time : Time - timestamp
logger : log handler - Logger

Search in tweets the query tokens

Output
[t0,t1,...,tN] - Tweets gathered in the interval (start,end)

t0 : Tweet (instance of class Tweet)

If sentiment_min is not None 
   If sentiment_max is not None 
      search only where sentiment-max >= tweet.sentiment >= sentiment-min
   Else
      search only where tweet.sentiment >= sentiment-min
Else
   If sentiment_max is not None 
      search only where tweet.sentiment <= sentiment-max

If geo is not None search only where tweet.location is inside geo object.
If time is not None search only where tweet.date < time

WARNING IN MONGODB THE ATTRIBUTE TEXT IS INDEXED 
https://docs.mongodb.com/manual/text-search/
"""
def search_geo_tweets_time(db, query, sentiment_min, sentiment_max, geo, time, logger):
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "search_geo_tweets", "db")
        raise Exception()
    
    return search_geo_tweets(db, query, sentiment_min, sentiment_max, geo, time2int(time), logger)

"""
Input
db : DB instance - Database
time : Unix timestamp in seconds - int
logger : log handler - Logger

Use mongodump for backups, the function should filtering those messages created after the UNIX timestamp time.
WARNING!! 
In this case use CREATED_DATE
in the Message Repository use the attribute created_date, the attribute is UNIX timestamp EXPRESSED IN MILLISECONDS.

Backup save path
emc-backups/year/month/day/ - Extract this information by the time parameter. 

"""

def backup(db, time, logger):
    #Configuration
    CLIENT_ID = ''
    TENANT_ID = ''
    AUTHORITY_URL = 'https://login.microsoftonline.com/{}'.format(TENANT_ID)
    RESOURCE_URL = 'https://graph.microsoft.com/'
    API_VERSION = 'v1.0'
    SCOPES = ['Sites.ReadWrite.All','Files.ReadWrite.All'] 
    sys.path.append('.')
    from onedrive import upload
    from onedrive import check_hash
    from onedrive import get_headers
   
   
    if(logger is None):
        raise Exception()
    if(db is None):
        writeLogs(logger, "backup", "db")
        raise Exception()
    if(time is None):
        writeLogs(logger, "backup", "time")
        raise Exception()
         
    timestamp = str(int2time(int(time/1000)))   #createdDate is in milliseconds
    year = timestamp[:4]
    month = timestamp[5:7]
    day = timestamp[8:10]
    
    '''
    EXPORT BSON
    tweets = db.Message.find({ "createdDate": { "$gte": time*1000} } )
    
    with open('backups/Message.bson', 'wb+') as f:
        for doc in tweets:
            f.write(BSON.encode(doc))
    '''    
    tweets = db.Message.find({"createdDate" : {"$gt": time*1000}})  #createdDate is in milliseconds
    
    collection = tweets
    jsonpath = "Message_" + str(year) + "-" + str(month) + "-" + str(day) + ".json"
    jsonpath = join("backups/", jsonpath)
    with open(jsonpath, 'wb') as jsonfile:
        jsonfile.write(dumps(collection).encode())
    
    onedrive_destination = '{}/{}/me/drive/root:/emc-backups'.format(RESOURCE_URL,API_VERSION)
    upload("backups/", onedrive_destination, get_headers(CLIENT_ID, AUTHORITY_URL, SCOPES))
    check_hash("backups/", onedrive_destination, get_headers(CLIENT_ID, AUTHORITY_URL, SCOPES))
    os.remove(jsonpath)


"""
List files in the folder with the name, size and the number of files inside if it is a folder
If year is None:
List years
If month is None:
List months
If day is None:
List days
Else:
List files in emc-backups/year/month/day/

Input:
year : Year - int
day : Month - int
month : Day - int
logger : log handler - Logger
"""
def read_backups(year, month, day, logger):
    from onedrive import list_files_folders
    #Configuration
    CLIENT_ID = ''
    TENANT_ID = ''
    AUTHORITY_URL = 'https://login.microsoftonline.com/{}'.format(TENANT_ID)
    RESOURCE_URL = 'https://graph.microsoft.com/'
    API_VERSION = 'v1.0'
    SCOPES = ['Sites.ReadWrite.All','Files.ReadWrite.All'] 
    sys.path.append('.')
    from onedrive import get_headers
    
    if(logger is None):
        raise Exception()
    if(year == None):
        writeLogs(logger, "read_backups", "year")
        raise Exception()
    if(month == None):
        writeLogs(logger, "read_backups", "month")
        raise Exception()
    if(day == None):
        writeLogs(logger, "read_backups", "day")
        raise Exception()
    
    onedrive_destination = '{}/{}/me/drive/root:/emc-backups'.format(RESOURCE_URL,API_VERSION)
    (files, folders) = list_files_folders(onedrive_destination, get_headers(CLIENT_ID, AUTHORITY_URL, SCOPES))
    listFiles = []
    for file in files:
        if(file[0] == "Message_" + str(year) + "-" + str(month) + "-" + str(day) + ".json"):
          listFiles.append(file)  
    return listFiles      
          
"""
Filters:
date Unix timestamp in seconds
log_type = INFO,WARN,ERROR
component = es. opennlp

Input:
log_path : log file path - string
log_type : Type - string
component : Component - string
logger : log handler - Logger
"""
def get_log(log_path, date, log_type, component, logger):
    if(logger is None):
        raise Exception()
    if(log_path is None):
        writeLogs(logger, "get_log", "log_path")
        raise Exception()
    f = open(log_path, "r")
    file = f.read().split("\n")
    for line in file:
        if(date is None):
            dateString = line[:19]
        else:
            dateString = str(int2time(date))
        if(log_type is None):
            log_type = line[0]        #any substring of line is correct
        if(component is None):
            component = line[0]        #any substring of line is correct    
        if(dateString==line[:19] and log_type in line and component in line):
            print(line)
            
         
f = open("twitter_requests.properties", "r")        #if it is in the same folder
file = f.read().split("\n")
db_url = file[0].split("=")[1]
db_name = file[4].split("=")[1]

client = MongoClient(db_url, unicode_decode_error_handler='ignore')
db = client[db_name]
logger = logging.getLogger('emotionalcity')

print("TEST count_tweets :")
try:
    (ranges, sum) = count_tweets(db, 1607464906, 1607475302, 3600, True, logger)
    for i in range (0, ranges[:,0].size):
        print(str(datetime.fromtimestamp(ranges[i][0])) + " - " + str(datetime.fromtimestamp(ranges[i][1])) + " : " + str(int(ranges[i][2])))
    print("Total tweets : " + str(sum))
except:
    print("Wrong parameters in count_tweets")
    
print("\nTEST count_tweets :")
try:
    (ranges, sum) = count_tweets(db, 1607464906, 0, 3600, True, logger)
    for i in range (0, ranges[:,0].size):
        print(str(datetime.fromtimestamp(ranges[i][0])) + " - " + str(datetime.fromtimestamp(ranges[i][1])) + " : " + str(int(ranges[i][2])))
    print("Total tweets : " + str(sum))
except:
    print("Wrong parameters in count_tweets")
    
print("\nTEST count_tweets_time :")
try:
    (ranges, sum) = count_tweets_time(db, int2time(1607464906), int2time(1607475302), 3600, True, logger)
    for i in range (0, ranges[:,0].size):
        print(str(datetime.fromtimestamp(ranges[i][0])) + " - " + str(datetime.fromtimestamp(ranges[i][1])) + " : " + str(int(ranges[i][2])))
    print("Total tweets : " + str(sum))    
except:
   print("Wrong parameters in count_tweets_time")

print("\nTEST senti_tweets :")
try:
    ranges = senti_tweets(db, 1607464906, 1607475302, 3600, False, logger)
    for i in range (0, ranges[:,0].size):
        print(str(datetime.fromtimestamp(ranges[i][0])) + " - " + str(datetime.fromtimestamp(ranges[i][1])) + " : " + str(int(ranges[i][2])) + " " + str(ranges[i][3]))
except:
    print("Wrong parameters in senti_tweets")
    
print("\nTEST senti_tweets :")
try:
    ranges = senti_tweets(db, 1607464906, 0, 3600, False, logger)
    for i in range (0, ranges[:,0].size):
        print(str(datetime.fromtimestamp(ranges[i][0])) + " - " + str(datetime.fromtimestamp(ranges[i][1])) + " : " + str(int(ranges[i][2])) + " " + str(ranges[i][3]))
except:
    print("Wrong parameters in senti_tweets")
    
print("\nTEST senti_tweets_time :")
try:
    ranges = senti_tweets_time(db, int2time(1607464906), int2time(1607475302), 3600, False, logger)
    for i in range (0, ranges[:,0].size):
        print(str(datetime.fromtimestamp(ranges[i][0])) + " - " + str(datetime.fromtimestamp(ranges[i][1])) + " : " + str(int(ranges[i][2])) + " " + str(ranges[i][3]))
except:
    print("Wrong parameters in senti_tweets_time")

print("\nTEST search_tweets :")
try:
    tweetList = search_tweets(db, 1607464906, 1607475302, ["manchi", "fegato"], logger)
    for x in tweetList:
        print(x.text)
except:
    print("Wrong parameters in search_tweets")
    
print("\nTEST search_tweets :")
try:
    tweetList = search_tweets(db, -1, 1607475302, ["manchi", "fegato"], logger)
    for x in tweetList:
        print(x.text)
except:
    print("Wrong parameters in search_tweets")
    
print("\nTEST search_tweets_time :")
try:
    tweetList = search_tweets_time(db, int2time(1607464906), int2time(1607475302), ["manchi", "fegato"], logger)
    for x in tweetList:
        print(x.text)
except:
    print("Wrong parameters in search_tweets_time")

print("\nTEST geo_count_tweets :")
try:
    count = geo_count_tweets(db, 1607466785, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger)
    print("Count for a threshold time : " + str(count))
    count = geo_count_tweets(db, 9999999999, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger)
    print("Count for a ficticious threshold time : " + str(count))
except:
    print("Wrong parameters in geo_count_tweets")
    
print("\nTEST geo_count_tweets :")
try:
    count = geo_count_tweets(db, -1, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger)
    print("Count for a ficticious threshold time : " + str(count))
except:
    print("Wrong parameters in geo_count_tweets")
    
print("\nTEST geo_count_tweets_time :")
try:
    count = geo_count_tweets_time(db, int2time(1607466785), [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger)
    print("Count for a threshold time : " + str(count))
    count = geo_count_tweets_time(db, int2time(9999999999), [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger)
    print("Count for a ficticious threshold time : " + str(count))
except:
    print("Wrong parameters in geo_count_tweets_time")
    
print("\nTEST geo_senti_tweets : ")
try:
    sentiment = geo_senti_tweets(db, 1607466785, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger)
    print("Sentiment for a threshold time : " + str(sentiment))
    sentiment = geo_senti_tweets(db, 9999999999, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger)
    print("Sentiment for a ficticious threshold time : " + str(sentiment))
except:
    print("Wrong parameters in geo-senti_tweets")

print("\nTEST geo_senti_tweets : ")
try:
    geo_senti_tweets(db, -1, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger)
except:
    print("Wrong parameters in geo-senti_tweets")
  
print("\nTEST geo_senti_tweets_time : ")
try:
    sentiment = geo_senti_tweets_time(db, int2time(1607466785), [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger)
    print("Sentiment for a threshold time : " + str(sentiment))
    sentiment = geo_senti_tweets_time(db, int2time(9999999999), [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger)
    print("Sentiment for a ficticious threshold time : " + str(sentiment))
except:
    print("Wrong parameters in geo-senti_tweets_time")
    
print("\nTEST search_geo_tweets :")
try:
    tweetList = search_geo_tweets(db, ["Buonanotte", "#realjuve"], None, None, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], 1607466785, logger)
    for x in tweetList:
        print(x.text)
    tweetList = search_geo_tweets(db, ["Buonanotte", "#realjuve"], 0, 1, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], 1607466785, logger)
    for x in tweetList:
        print(x.text)
except:
    print("Wrong parameters in search_geo_tweets")
    
print("\nTEST search_geo_tweets :")
try:
    tweetList = search_geo_tweets(db, ["Buonanotte", "#realjuve"], None, None, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], -1, logger)
except:
    print("Wrong parameters in search_geo_tweets")

print("\nTEST search_geo_tweets_time :")
try:
    tweetList = search_geo_tweets_time(db, ["Buonanotte", "#realjuve"], None, None, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], int2time(1607466785), logger)
    for x in tweetList:
        print(x.text)
    tweetList = search_geo_tweets_time(db, ["Buonanotte", "#realjuve"], 0, 1, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], int2time(1607466785), logger)
    for x in tweetList:
        print(x.text)
except:
    print("Wrong parameters in search_geo_tweets_time")
    
'''
print("\nTEST backup :")
try:
    backup(db, 1907466785, logger)
except:
    print("Wrong parameters in backup")
'''

#TEST read_backups
print("\nTEST read_backups : ")
try:
    files = read_backups("1970", "01", "23", logger)
    for file in files:
        print(file[0])
except:
    print("Wrong parameters in read_backups")

print("\nTEST get_log : ")
try:
    get_log("C:\\Users\ddipi\Desktop\emotionalcity\log.log", None, "INFO", "opennlp", logger)
    print("\n")
    get_log("C:\\Users\ddipi\Desktop\emotionalcity\log.log", string2int("2020/12/04 19:49:28"), "ERROR", None, logger)
except:
    print("Wrong parameters in log")

logging.shutdown()







  

