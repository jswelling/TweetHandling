# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 11:25:35 2015

@author: ColdJB
"""


# Useful links:
#   Working with timelines: https://dev.twitter.com/docs/working-with-timelines
#   Twython info: https://github.com/ryanmcgrath/twython

# Required packages:
#   twython
#       requests_oauthlib
#           requests
#           oauthlib

from twython import Twython
import time, datetime, sys, json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import cPickle as pickle

#import json

pkl_mode = True

## Twitter API keys (ATOD_stream):
consumer_key = 'Nu6zqJMAyvEfuvcWVBCVaL45B'
consumer_secret = 'gVemI189Fmh0MVIAhMzYAcTMj2k6lfn26iJRYjfSLl6osDp6DH'
access_token = '2916021555-rkrT1HNp4ABkYOjhpJLTCSlXge1PZfCpSlYxCoT'
access_token_secret = 'nbdCwzIpLVu48GEZjTifJiL6skJyzdf5p65NzPsZWai84'


# Set directory and use dynamic file names
#directory = 'C:\\Users\\coldjb\\Desktop\\Twython\\PSC_Test\\'
directory = './'
topic = 'ATOD_stream' 
def set_file():
    global outfile
    if pkl_mode:
        outfile = directory+datetime.datetime.today().strftime('%Y%m%d%H%M%S')+'_'+topic+'.pkl' 
    else:
        outfile = directory+datetime.datetime.today().strftime('%Y%m%d%H%M%S')+'_'+topic+'.json' 
    global errlog    
    errlog = directory+datetime.datetime.today().strftime('%Y%m%d%H%M%S')+'_errors.txt'
    global oldtime
    oldtime = datetime.datetime.today().strftime('%d') 
set_file()

# Search terms: (separated with commas; spaces = OR)
filter_file = directory+'filters.txt'
get_filters = open(filter_file,"r")
lines = get_filters.readlines()
get_filters.close()
filters = [l.replace('\n', '') for l in lines]
#filters = ''.join(lines).replace('\n',',')

# This is used for exponential backoff
err_count = 0
def back_off():
    global err_count 
    err_count += 1
    time.sleep(2**err_count*30)
def err_clr():
    global err_count
    if err_count > 0:
        err_msg='@ColditzJB Back-off count in recent data: '+str(err_count)+' \n\nSome data were lost.\n'+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))
        tweet.update_status(status=err_msg)
        err_count = 0

class StdOutListener(StreamListener):
    def on_data(self, data):
        if datetime.datetime.today().strftime('%d') != oldtime:        
            err_clr()
            set_file()
        out_file = open(outfile,"a")
        decoded = json.loads(data)
        if pkl_mode:
            pickle.dump(decoded, out_file, pickle.HIGHEST_PROTOCOL)
        else:
            json.dump(decoded,out_file, indent=0)

    def on_error(self, status_code):
        #err_msg='@ColditzJB ('+str(status_code)+') Twitter error.\n\n\n'+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))
        #tweet.update_status(status=err_msg)
        err_str=str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))+','+str(status_code)+',Twitter error\n'
        log_err = open(errlog,"a")
        log_err.write(err_str)
        log_err.close()
        print status_code, ' Twitter error'
        if status_code in ['104', '420']: 
            back_off()
        return True

#stream = MyStreamer(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
tweet = Twython(consumer_key, consumer_secret, access_token, access_token_secret)
#
while True:
    try:
        l = StdOutListener()
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)        
        stream = Stream(auth, l)        
        stream.filter(track=filters, languages=['en']) #This is a limited search based on filters.txt file
        #stream.statuses.sample() #This is a 1% sample of all Twitter activity
    except:
        err = str(sys.exc_info()[1])
        err_str=str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))+',000,'+err+'\n'
        log_err = open(errlog,"a")
        log_err.write(err_str)
        log_err.close()
        if 'Max retries exceeded' in err:
            err_msg='@ColditzJB CONNECTION REFUSED!\n\nTime-out.\n'+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))
            tweet.update_status(status=err_msg)
            back_off()
        elif 'IncompleteRead' in err:
            #err_msg='@ColditzJB Experiencing some latency.\n\nQueue dumped.\n'+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))
            #tweet.update_status(status=err_msg)
            pass
        elif 'decode byte' in err:
            #err_msg='@ColditzJB Can not decode byte.\n\nTweet dropped.\n'+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))
            #tweet.update_status(status=err_msg)
            pass
        elif 'has no attribute' in err:
            #err_msg='@ColditzJB NoneType object found.\n\nMoving on...\n'+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))
            #tweet.update_status(status=err_msg)
            pass
        else:
            err_msg='@ColditzJB '+err[:80]+'\n\nTime-out.\n'+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))
            try: tweet.update_status(status=err_msg)
            except: pass
            back_off()
        print err
        continue
    break






#if __name__ == '__main__':
#    l = StdOutListener()
#    auth = OAuthHandler(consumer_key, consumer_secret)
#    auth.set_access_token(access_token, access_token_secret)
#
#    stream = Stream(auth, l)
#    stream.filter(track=['lol'])
