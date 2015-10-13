#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 12 11:54:12 2015

@author: ColdJB
"""

import sys
import cPickle as pickle
import re
import os.path
import datetime
import types
from collections import defaultdict

# directory = '\\Primack\\Primack Projects\\01 Projects\\2015_hpi_twitter\\World Vape Day\\2015-05-10\\'
# directory = 'C:\\Users\\coldjb\\Desktop\\Data Transfer\\'
directory = '/home/welling/git/TweetHandling/TweetHandling/data'

tweet_count = 0  # Start counting at n-1 (usually 0)
#skipper = 100    # Set this variable to save every n-th tweet (all tweets: 1)
skipper = 1    # Set this variable to save every n-th tweet (all tweets: 1)

"""The following string determines the format of the output records"""
rowString = ("{0[u_id]:s},{0[user]:s},{0[utc_off]:s},{0[created]:s},{0[faves]:s},{0[followers]:s},"
             + "{0[tweets]:s},{0[date]:s},{0[t_id]:s},{0[text]:s},{0[day]:s},{0[yyyy]:s},"
             + "{0[month]:s},{0[dd]:s},{0[hh]:s},{0[mm]:s},{0[ss]:s}\n")

emojiRe = re.compile(r"((\\U[0-9a-f]{8})|(\\u[0-9a-f]{4}))")

replacementStringPairs = [(re.compile('"'), ' ["] '),
                          (re.compile(r'\\n'), ' [RETURN] '),
                          (re.compile(r'\\r'), ' [RETURN] '),
                          (re.compile(','), ' [COMMA] '),
                          (re.compile('&amp;'), '&')
                          ]


class EmojiDict(defaultdict):
    def __missing__(self, key):
        if key in self:
            return self[key]
        else:
            v = '\\%s' % key
            self[key] = v
            print ('<Unknown code %s>' % v),
            return v


### A relic of decoding JSON files - saved for posterity
#def fixjson(directory, fn): 
#    f = open(directory+fn, 'rb')
#    data = f.read()         #? json.loads(f)
#    data = data.replace('}{','},{')
#    data2 = '{\"tweet\": ['+data+']}'
#    record = json.loads(data2)
#    f.close()
#    jsontocsv(record)


def getpkl(directory, fn):
    """
    This function returns a generator which iterates through the data entries in the
    given pkl file.
    """
    with open(os.path.join(directory, fn), 'rb') as f:
#     f = file(directory+fn, 'rb')
#    data = f.read() #json.loads(f) #This relates to JSON files
        while True:
            try:
                line = pickle.load(f)
                yield line
            except EOFError:  # EOFError is normal at end of .pkl file
                print '. End of File'
                break
            except Exception, e:  # Other errors are printed and the process endures
                print 'Ignoring exception: %s' % str(e)
                continue


def pkltocsv(data, saveFile):
    #    for data in line: #This relates to JSON files

    # Collect some data about the user:
    # All available fields: https://dev.twitter.com/overview/api/tweets
    rec = {}
    for fld, key in [('u_id', 'id'),
                     ('t_id', 'id_str'),
                     ('user', 'screen_name'),
                     ('utc_off', 'utc_offset'),
                     ('created', 'created_at'),
                     ('faves', 'favourites_count'),
                     ('followers', 'followers_count'),
                     ('following', 'friends_count'),
                     ('tweets', 'statuses_count')
                     ]:
        try:
            val = data['user'][key]
            if isinstance(val, types.UnicodeType):
                rec[fld] = val.encode('utf-8')
            else:
                if key == 'utc_offset':
                    val /= 3600
                rec[fld] = str(val)
        except:
            if key in ['id', 'id_str']:
                rec[fld] = '0'
            else:
                rec[fld] = ''

### Collect data about the tweet:
### All possible fields: https://dev.twitter.com/overview/api/users
    try:
        date = data['created_at']
        day = date[:3]
        yyyy = date[-4:]
        month = date[:7][-3:]
        dd = date[:10][-2:]
        hh = date[:13][-2:]
        mm = date[:16][-2:]
        ss = date[:19][-2:]
    except:
        date = day = yyyy = month = dd = hh = mm = ss = ''
    try:
        uText = data['text']
        text = uText.encode('unicode-escape')
        for compRe, newStr in replacementStringPairs:
            text = compRe.sub(newStr, text)

        try:
            words = re.split(emojiRe, text)
            if len(words) > 1:
                words = [w for w in words if w is not None]
                text = ''.join([(emojiDict[w[1:]] if emojiRe.match(w) else w) for w in words])
        except Exception, e:
            print 'Exception: %s' % e
            print 'unicode text: %s' % uText
            print 'words: %s' % str(words)
            raise
    except:
        text = ''

    lclVars = locals()
    for fld in ['date', 'day', 'yyyy', 'month', 'dd', 'hh', 'mm', 'ss', 'text']:
        rec[fld] = lclVars[fld]

    # Append the row of data to the CSV file
    saveFile.write(rowString.format(rec))


# Load the emoji codes
emojiDict = EmojiDict()
with open('emojilist3.csv') as f:
    for line in f:
        k, v = line.split(',')
        emojiDict[k] = v.strip()

### This iterates through any .pkl files in the current directory Note:
### Files must match the naming conventions of our twitter streamer output.
with open(os.path.join(directory, 'Data.csv'), 'a') as saveFile:

    for fname in os.listdir(directory):
        print fname
        if fname[-4:] == '.pkl':
    #        try: # Exception handler removed from this top-level process.
            if int(fname[:8]) < int(datetime.datetime.today().strftime('%Y%m%d')):
                print '\n\n Running total:'+str(tweet_count)+'\n'
                print 'Now reading: '+str(fname)
                for data in getpkl(directory, fname):
                    tweet_count += 1
                    if tweet_count % 10000 == 0:  # Progress indicator
                        print '.', tweet_count,
                        sys.stdout.flush()

                    if tweet_count % skipper == 0:  # See global "skipper" for description
                        pkltocsv(data, saveFile)
            else:
                print 'Bad date: '+str(fname)
    #        except: 
    #            print 'Invalid: '+str(f)
        else:
            print 'skipping non-pkl file %s' % fname

print '\n\n'+str(tweet_count)+' records reviewed (some may be blank).'
