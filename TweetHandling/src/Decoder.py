# -*- coding: utf-8 -*-
"""
Created on Thu Feb 12 11:54:12 2015

@author: ColdJB
"""



import pickle, re, os, datetime, csv, sys

def globs():
    global tweet_count, skipper
    tweet_count=0 #Start counting at n-1 (usually 0)
    skipper=100 #Set this variable to save every n-th tweet (all tweets: 1)
globs()

#directory = '\\Primack\\Primack Projects\\01 Projects\\2015_hpi_twitter\\World Vape Day\\2015-05-10\\'
directory = 'C:\\Users\\coldjb\\Desktop\\Data Transfer\\'
files = os.listdir(directory)


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
    f = file(directory+fn, 'rb')
#    data = f.read() #json.loads(f) #This relates to JSON files
    while True:
        global tweet_count, skipper
        try:
            line = pickle.load(f)
            pkltocsv(line)
        except EOFError: # EOFError is normal at end of .pkl file
            print '. End of File'
            f.close()
            break
        except: #Other errors are printed and the process endures
            print str(sys.exc_info()[0]),
            continue
 



### Emoji conversion process removed (too needy)
# Load the emoji conversion file 
#with open(directory+'emojilist3.csv', 'rb') as f:
#    reader = csv.reader(f)
#    emoji = list(reader)


def pkltocsv(data):
#    for data in line: #This relates to JSON files
    global tweet_count, skipper
    tweet_count += 1
    if tweet_count%10000 == 0: #Progress indicator
            print '.',tweet_count,

    if tweet_count%skipper == 0: #See global "skipper" for description
        outfile = directory+'Data.csv' # Save data from the tweets here

### Collect some data about the user:
### All available fields: https://dev.twitter.com/overview/api/tweets
        try: u_id = str(data['user']['id'])
        except: u_id = '0'
        try: user = data['user']['screen_name'].encode('utf-8')
        except: user = ''
        try: utc_off = str(int(data['user']['utc_offset'])/3600)
        except: utc_off = ''
        try: created = data['user']['created_at']
        except: created = ''
        try: faves = str(data['user']['favourites_count'])
        except: faves = ''
        try: followers = str(data['user']['followers_count'])
        except: followers = ''
        try: following = str(data['user']['friends_count'])
        except: following = ''
        try: tweets = str(data['user']['statuses_count'])
        except: tweets = ''
    
### Collect data about the tweet:
### All possible fields: https://dev.twitter.com/overview/api/users
        try: t_id = data['id_str']
        except: t_id = '0'
        try: 
            date = data['created_at']
            day = date[:3]
            yyyy = date[-4:]
            month = date[:7][-3:]
            dd = date[:10][-2:]
            hh = date[:13][-2:]
            mm = date[:16][-2:]
            ss = date[:19][-2:]
        except: date = day = yyyy = month = dd = hh = mm = ss = '' 
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
        except: text = ''
    	    
# Append the row of data to the CSV file 
        saveFile = open(outfile,'a')
        saveFile.write(u_id) 
        saveFile.write(',')
        saveFile.write(user) 
        saveFile.write(',')
        saveFile.write(utc_off) 
        saveFile.write(',')
        saveFile.write(created) 
        saveFile.write(',')
        saveFile.write(faves) 
        saveFile.write(',')
        saveFile.write(followers) 
        saveFile.write(',')
        saveFile.write(following) 
        saveFile.write(',')
        saveFile.write(tweets) 
        saveFile.write(',')
        saveFile.write(date) 
        saveFile.write(',')
        saveFile.write(t_id)
        saveFile.write(',')
        saveFile.write(text)
        saveFile.write(',')
        saveFile.write(day)
        saveFile.write(',')
        saveFile.write(yyyy)
        saveFile.write(',')
        saveFile.write(month)
        saveFile.write(',')
        saveFile.write(dd)
        saveFile.write(',')
        saveFile.write(hh)
        saveFile.write(',')
        saveFile.write(mm)
        saveFile.write(',')
        saveFile.write(ss)
        saveFile.write('\n')
        saveFile.close()




### This iterates through any .pkl files in the current directory Note:
### Files must match the naming conventions of our twitter streamer output.
for f in files:
    if f[-4:] =='.pkl':
#        try: # Exception handler removed from this top-level process.
        if int(f[:8]) < int(datetime.datetime.today().strftime('%Y%m%d')):
            print '\n\n Running total:'+str(tweet_count)+'\n'
            print 'Now reading: '+str(f)
            fn = f
            getpkl(directory, fn)
        else: print 'Bad date: '+str(f)
#        except: 
#            print 'Invalid: '+str(f)

print '\n\n'+str(tweet_count)+' records reviewed (some may be blank).'

