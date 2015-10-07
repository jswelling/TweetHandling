# -*- coding: utf-8 -*-
"""
Created on Thu Feb 12 11:54:12 2015

@author: ColdJB
"""

import cPickle as pickle
import re
import os.path
import datetime
import types


# directory = '\\Primack\\Primack Projects\\01 Projects\\2015_hpi_twitter\\World Vape Day\\2015-05-10\\'
directory = 'C:\\Users\\coldjb\\Desktop\\Data Transfer\\'

tweet_count = 0  # Start counting at n-1 (usually 0)
skipper = 100    # Set this variable to save every n-th tweet (all tweets: 1)

"""The following string determines the format of the output records"""
rowString = ("%(u_id)s,%(user)s,%(utc_off)s,%(created)s,%(faves)s,%(followers)s,"
             + "%(tweets)s,%(date)s,%(t_id)s,%(text)s,%(day)s,%(yyyy)s,%(month)s,"
             + "%(dd)s,%(hh)s,%(mm)s,%(ss)s\n")


# def globs():
#     global tweet_count, skipper
#     tweet_count=0 #Start counting at n-1 (usually 0)
#     skipper=100 #Set this variable to save every n-th tweet (all tweets: 1)
# globs()


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


### Emoji conversion process removed (too needy)
# Load the emoji conversion file 
#with open(directory+'emojilist3.csv', 'rb') as f:
#    reader = csv.reader(f)
#    emoji = list(reader)


def pkltocsv(data, saveFile):
    #    for data in line: #This relates to JSON files

    # Collect some data about the user:
    # All available fields: https://dev.twitter.com/overview/api/tweets
    rec = {}
    for fld, key in [('u_id', 'id'),
                     ('t_id', 'id_str'),
                     ('user', 'screen_name'),
                     ('utc_off', 'utc_offset')
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
        text = data['text'].encode('unicode-escape')
        text = re.sub('"', ' ["] ', text)
        text = re.sub(r'\\n', ' [RETURN] ', text)
        text = re.sub(r'\\r', ' [RETURN] ', text)
        text = re.sub(',', ' [COMMA]', text)
        text = re.sub('&amp;', '&', text)
    ### Emoji conversion process removed (too needy)
    #    for key, val in emoji:
    #        text = re.sub(key, val, text)
    #    text = re.sub(r'\\ \[',' [',text)
    except:
        text = ''

    lclVars = locals()
    for fld in ['date', 'day', 'yyyy', 'month', 'dd', 'hh', 'mm', 'ss', 'text']:
        rec[fld] = lclVars[fld]

    # Append the row of data to the CSV file
    saveFile.write(rowString.format(rec))


### This iterates through any .pkl files in the current directory Note:
### Files must match the naming conventions of our twitter streamer output.
with open(os.path.join(directory, 'Data.csv'), 'a') as saveFile:

    for fname in os.listdir(directory):
        if fname[-4:] == '.pkl':
    #        try: # Exception handler removed from this top-level process.
            if int(fname[:8]) < int(datetime.datetime.today().strftime('%Y%m%d')):
                print '\n\n Running total:'+str(tweet_count)+'\n'
                print 'Now reading: '+str(fname)
                for data in getpkl(directory, fname):
                    tweet_count += 1
                    if tweet_count % 10000 == 0:  # Progress indicator
                        print '.', tweet_count,

                    if tweet_count % skipper == 0:  # See global "skipper" for description
                        pkltocsv(data, saveFile)
            else:
                print 'Bad date: '+str(fname)
    #        except: 
    #            print 'Invalid: '+str(f)
        else:
            print 'skipping non-pkl file %s' % fname

print '\n\n'+str(tweet_count)+' records reviewed (some may be blank).'
