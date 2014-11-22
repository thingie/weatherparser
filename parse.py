#!/usr/bin/python

from bs4 import BeautifulSoup
import urllib2
import sys
import re
import datetime
import sqlite3

# config (kind of)
url = 'http://pr-asv.chmi.cz/synopy-map/pocasina.php?indstanice=11519'
dbfile = '/home/pony/weather.db'

htmltext = None
try:
    data = urllib2.urlopen(url)
    htmltext = data.read()
except Exception, e:
    print 'no data read'
    sys.exit(1)

if htmltext is not None:
    soup = BeautifulSoup(htmltext)
else:
    print 'cannot read text'
    sys.exit(1)

recordDate = None
    
# variables to read
temperature = None
dewPoint = None
relativeHumidity = None
airPressure = None
windSpeed = None
windDirection = None
clouds = None # just string that we cannot process now
description = None
rainfall = None

try:
    dateString = soup.find_all("center", class_="zviraz")[1].text.split('\n')[0]
    dm = re.search('([0-9]{2})\.([0-9]{2})\.([0-9]{4})', dateString)
    hour = int(re.search('([0-9]{2}) UTC', dateString).group(1))

    recordDate = datetime.datetime(int(dm.group(3)), int(dm.group(2)), int(dm.group(1)), hour)
except Exception, e:
    print dateString
    print 'date parse failed'
    sys.exit(1)
    
try:
    clouds = soup.find_all('td')[5].text
except Exception, e:
    print 'cloud parsing failed'
    clouds = None  # we can live without clouds :)

try:
    windMatch = re.search(' (.+)\\xa0-\\xa0([1-9][0-9]*) m/s', soup.find_all('td')[7].text)
    windSpeed = int(windMatch.group(2))
    windDirection = windMatch.group(1)
except Exception, e:
    print 'failed to read wind: %s' % e
    windSpeed = None
    windDirection = None

try:
    airPressure = float(re.search('\\xa0([1-9][0-9]*\.[0-9]+) hPa', soup.find_all('td')[11].text).group(1))
except Exception, e:
    print 'failed to read air pressure'
    airPressure = None

try:
    temperature = float(re.search('\\xa0(-*[1-9][0-9]*\.?[0-9]*)\\xb0', soup.find_all('td')[15].text).group(1))
except Exception, e:
    print 'failed to read temperature'
    sys.exit(1) # cannot recover

try:
    dewPoint = float(re.search('\\xa0([1-9][0-9]*\.?[0-9]*)\\xb0', soup.find_all('td')[17].text).group(1))
except Exception, e:
    print 'failed to read dewPoint'
    dewPoint = None

try:
    relativeHumidity = int(re.search('\\xa0([1-9][0-9]*) \%', soup.find_all('td')[19].text).group(1))
except Exception, e:
    print 'failed to read relativeHumidity'
    relativeHumidity = None

try:
    rainfall = re.search('\\xa0(.*)\\xa0', soup.find_all('td')[27].text).group(1)
except Exception, e:
    print 'failed to parse rainfall'
    rainfall = None

try:
    sqldb = sqlite3.connect(dbfile)
    c = sqldb.cursor()

    safeText = None
    try:
        safeText = htmltext.decode('cp1250')
    except:
        pass

    #sqldb.text_factory = str
    c.execute('''INSERT INTO records (date, windDirection, windSpeed, airPressure, temperature, dewPoint,
                 relativeHumidity, rainfall, originalData) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',  (recordDate, windDirection,
                                                                                   windSpeed, airPressure, temperature, dewPoint, relativeHumidity, rainfall, safeText))

    sqldb.commit()
    sqldb.close()
except Exception, e:
    print 'sql failed: %s' % e
    sys.exit(1)
