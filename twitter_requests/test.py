# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 16:36:16 2020

@author: ddipi
"""
import unittest
import twitter_requests
from pymongo import MongoClient
import numpy as np
import logging

def assertSequenceEqual(self, it1, it2):
    self.assertEqual(tuple(it1), tuple(it2))
    
class TestTwitterRequests(unittest.TestCase):
    def test_count_tweets(self):
        self.assertTrue((np.array([42., 18., 6.]) == twitter_requests.count_tweets(db, 1607464906, 1607475302, 3600, True, logger)[0]).all()) 
        self.assertRaises(Exception, twitter_requests.count_tweets, db, 1607464906, 0, 3600, True, logger)
    
    def test_count_tweets_time(self):
        self.assertTrue((np.array([42., 18., 6.]) == twitter_requests.count_tweets_time(db, twitter_requests.int2time(1607464906), twitter_requests.int2time(1607475302), 3600, True, logger)[0]).all()) 
        
    def test_senti_tweets(self):
        res = (0.199 + 0.066 + 0.152)/3
        restweet = twitter_requests.senti_tweets(db, 1607464906, 1607475302, 3600, False, logger)
        mean = (restweet[3] + restweet[7] + restweet[11])/3
        self.assertAlmostEqual(mean, res, 2)
        self.assertRaises(Exception, twitter_requests.senti_tweets, db, 1607464906, 0, 3600, False, logger)
    
    def test_senti_tweets_time(self):
        res = (0.199 + 0.066 + 0.152)/3
        restweet = twitter_requests.senti_tweets_time(db, twitter_requests.int2time(1607464906), twitter_requests.int2time(1607475302), 3600, False, logger)
        mean = (restweet[3] + restweet[7] + restweet[11])/3
        self.assertAlmostEqual(mean, res, 2)
        
    def test_search_tweets(self):
        self.assertEqual(len(twitter_requests.search_tweets(db, 1607464906, 1607475302, ["manchi", "fegato"], logger)), 4)
        self.assertRaises(Exception, twitter_requests.search_tweets, db, -1, 1607475302, ["manchi", "fegato"], logger)
    
    def test_search_tweets_time(self):
        self.assertEqual(len(twitter_requests.search_tweets_time(db, twitter_requests.int2time(1607464906), twitter_requests.int2time(1607475302), ["manchi", "fegato"], logger)), 4)

    def test_geo_count_tweets(self):
        self.assertEqual(twitter_requests.geo_count_tweets(db, 1607466785, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger), 35)
        self.assertEqual(twitter_requests.geo_count_tweets(db, 9999999999, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger), 0)
        
    def test_geo_count_tweets_time(self):
        self.assertEqual(twitter_requests.geo_count_tweets_time(db, twitter_requests.int2time(1607466785), [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger), 35)
        self.assertEqual(twitter_requests.geo_count_tweets_time(db, twitter_requests.int2time(9999999999), [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger), 0)
        
    def test_geo_senti_tweets(self):
        self.assertAlmostEqual(twitter_requests.geo_senti_tweets(db, 1607466785, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger), 0.057, 3)
        self.assertEqual(twitter_requests.geo_senti_tweets(db, 9999999999, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger), 0)
        self.assertRaises(Exception, twitter_requests.geo_senti_tweets, db, -1, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger)
        
    def test_geo_senti_tweets_time(self):
        self.assertAlmostEqual(twitter_requests.geo_senti_tweets_time(db, twitter_requests.int2time(1607466785), [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger), 0.057, 3)
        self.assertEqual(twitter_requests.geo_senti_tweets_time(db, twitter_requests.int2time(9999999999), [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], logger), 0)
        
    def test_search_geo_tweets(self):
        self.assertEqual(len(twitter_requests.search_geo_tweets(db, ["Buonanotte", "#realjuve"], None, None, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], 1607466785, logger)), 3)
        self.assertEqual(len(twitter_requests.search_geo_tweets(db, ["Buonanotte", "#realjuve"], 0, None, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], 1607466785, logger)), 3)
        self.assertRaises(Exception, twitter_requests.search_geo_tweets, db, ["Buonanotte", "#realjuve"], None, None, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], -1, logger)
        
    def test_search_geo_tweets_time(self):
        self.assertEqual(len(twitter_requests.search_geo_tweets_time(db, ["Buonanotte", "#realjuve"], None, None, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], twitter_requests.int2time(1607466785), logger)), 3)
        self.assertEqual(len(twitter_requests.search_geo_tweets_time(db, ["Buonanotte", "#realjuve"], 0, 1, [(40.41388645, 18.1876629), (41.092715299999995, 16.87499235)], twitter_requests.int2time(1607466785), logger)), 3)
        
    def test_backup(self):
        self.assertTrue(twitter_requests.backup(configs, db, 1907466785, logger) is not False)
        
    def test_read_backups(self):
        self.assertEqual(len(twitter_requests.read_backups(configs, "1970", "01", "23", logger)), 1)
        
    def test_get_log(self):
        self.assertEqual(len(twitter_requests.get_log("C:\\Users\ddipi\Desktop\emotionalcity\log.log", None, "INFO", "opennlp", logger)), 27)
    
    
def test():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTwitterRequests)
    unittest.TextTestRunner(verbosity=2).run(suite)
    
logger = logging.getLogger('emotionalcity')
try:
    configs = twitter_requests.read_properties("C:\\Users\ddipi\Desktop\Emotionalcity", "twitter_requests.properties", logger)
except:
    print("Wrong parameters in read_properties")
    
client = MongoClient(configs["db_url"], unicode_decode_error_handler='ignore')
db = client[configs["db_name"]]
test()