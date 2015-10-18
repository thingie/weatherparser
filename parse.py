#!/usr/bin/python

from bs4 import BeautifulSoup
import urllib2
import sys
import re
import datetime
import sqlite3
from hashlib import sha256

weatherStations = {
    'praha-karlov': 'http://pr-asv.chmi.cz/synopy-map/pocasina.php?indstanice=11519',
    'praha-libus': 'http://pr-asv.chmi.cz/synopy-map/pocasina.php?indstanice=11520',
    'praha-ruzyne': 'http://pr-asv.chmi.cz/synopy-map/pocasina.php?indstanice=11518',
    'praha-kbely': 'http://pr-asv.chmi.cz/synopy-map/pocasinawin.php?indstanice=11567',
    'ostrava-mosnov': 'http://pr-asv.chmi.cz/synopy-map/pocasina.php?indstanice=11782',
}

# config (kind of)
dbfile = '/home/pony/weather2.db'

class weatherRecord(object):
    recordDate = None
    temperature = None
    dewPoint = None
    relativeHumidity = None
    airPressure = None
    windSpeed = None
    windDirection = None
    clouds = None
    description = None
    rainfall = None
    plaintext = None
    station = None

    def saveToDb(self, dbConnection):
        try:
            c = dbConnection.cursor()
            c.execute('''INSERT INTO records (date, windDirection, windSpeed, airPressure, temperature, dewPoint,
                         relativeHumidity, rainfall, station, originalData) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (self.recordDate, self.windDirection, self.windSpeed, self.airPressure, self.temperature,
                       self.dewPoint, self.relativeHumidity, self.rainfall, self.station, self.safeText))
            dbConnection.commit()
        except Exception, e:
            print 'failed to write record of %s for %s' % (self.recordDate, self.station)
            print 'sql failed: %s' % e

"""
Dump raw data in case of a fatal error during parsing

non-fatal errors are recoverable from safeText in the regular record
binds global sqldb (TODO, not very cool)
"""
def fatalErrDump(errText):
    chsum = sha256(errText).hexdigest()
    try:
        c = sqlDb.cursor()
        c.execute('''INSERT INTO errors (date, checksum, badtext) VALUES (?, ?, ?)''',
                  ("date('now')", chsum, errText))
        sqlDb.commit()
    except Exception, e: 
        print 'there was a fatal error and the error log failed as well'
        print 'well, what now?'

def getData(stationUrl, stationName):
    htmltext = None
    try:
        data = urllib2.urlopen(stationUrl, None, 30)
        htmltext = data.read()
    except Exception, e:
        print 'no data read for station ', stationName
        raise e

    if htmltext is not None:
        soup = BeautifulSoup(htmltext)
    else:
        print 'cannot read text'
        raise Exception('Could not read text')

    r = weatherRecord()
    r.station = stationName

    try:
        dateString = soup.find_all("center", class_="zviraz")[1].text.split('\n')[0]
        dm = re.search('([0-9]{2})\.([0-9]{2})\.([0-9]{4})', dateString)
        hour = int(re.search('([0-9]{2}) UTC', dateString).group(1))

        r.recordDate = datetime.datetime(int(dm.group(3)), int(dm.group(2)), int(dm.group(1)), hour)

    except Exception, e:
        print dateString
        print 'date parse failed'
        fatalErrDump(htmltext)
        raise Exception('Could not get recordDate')
    
    try:
        r.clouds = soup.find_all('td')[5].text
    except Exception, e:
        print 'cloud parsing failed'
        r.clouds = None  # we can live without clouds :)

    try:
        windIndex = 7 if r.station == "praha-kbely" else 9
        windMatch = re.search(' (.+)\\xa0-\\xa0([1-9][0-9]*) m/s', soup.find_all('td')[windIndex].text)
        r.windSpeed = int(windMatch.group(2))
        r.windDirection = windMatch.group(1)
    except Exception, e:
        print 'failed to read wind: %s' % e
        r.windSpeed = None
        r.windDirection = None

    try:
        airIndex = 11 if r.station == "praha-kbely" else 13
        r.airPressure = float(re.search('\\xa0([1-9][0-9]*\.[0-9]+) hPa', soup.find_all('td')[airIndex].text).group(1))
    except Exception, e:
        print 'failed to read air pressure'
        r.airPressure = None

    try:
        tempIndex = 15 if r.station == "praha-kbely" else 17
        r.temperature = float(re.search('\\xa0(-*[1-9][0-9]*\.?[0-9]*)\\xb0', soup.find_all('td')[tempIndex].text).group(1))
    except Exception, e:
        print 'failed to read temperature'
        fatalErrDump(htmltext)
        raise Exception('No temperature')

    try:
        dewIndex = 17 if r.station == "praha-kbely" else 19
        r.dewPoint = float(re.search('\\xa0([1-9][0-9]*\.?[0-9]*)\\xb0', soup.find_all('td')[dewIndex].text).group(1))
    except Exception, e:
        print 'failed to read dewPoint'
        r.dewPoint = None

    try:
        humidityIndex = 19 if r.station == "praha-kbely" else 21
        r.relativeHumidity = int(re.search('\\xa0([1-9][0-9]*) \%', soup.find_all('td')[humidityIndex].text).group(1))
    except Exception, e:
        print 'failed to read relativeHumidity'
        r.relativeHumidity = None

    try:
        rainfallIndex  = 27 if r.station == "praha-kbely" else 29
        r.rainfall = re.search('\\xa0(.*)\\xa0', soup.find_all('td')[rainfallIndex].text).group(1)
    except Exception, e:
        print 'failed to parse rainfall'
        r.rainfall = None

    r.safeText = None
    try:
        r.safeText = htmltext.decode('cp1250')
    except:
        pass

    return r

sqldb = sqlite3.connect(dbfile)

try:
    karlov = getData(weatherStations['praha-karlov'], 'praha-karlov').saveToDb(sqldb)
except Exception, e:
    print 'failed to write station karlov'
    print e

try:
    libus = getData(weatherStations['praha-libus'], 'praha-libus').saveToDb(sqldb)
except Exception, e:
    print 'failed to write station libus'
    print e

try:
    kbely = getData(weatherStations['praha-kbely'], 'praha-kbely').saveToDb(sqldb)
except Exception, e:
    print 'failed to write station kbely'
    print e

try:
    letiste = getData(weatherStations['praha-ruzyne'], 'praha-ruzyne').saveToDb(sqldb)
except Exception, e:
    print 'failed to write station letiste'
    print e

try:
    mosnov = getData(weatherStations['ostrava-mosnov'], 'ostrava-mosnov').saveToDb(sqldb)
except Exception, e:
    print 'failed to write station mosnov'
    print e

sqldb.close()
