#!/usr/bin/python

import urllib2
import sys
import json
import sqlite3
import datetime

configuration = {
    'apiKey': None,
    'longitude': '50.0755',
    'latitude': '14.4378',
    'dbfile': '/home/pony/weather2.db',
    'stationName': 'praha-karlov'
}

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
    safeText = None

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

apiKey = None
try:
    af = open('/home/pony/.dsapikey') #.read().strip()
    configuration['apiKey'] = af.read().strip()
    af.close()
except Exception, e:
    print e
    sys.exit(-1)

url = 'https://api.darksky.net/forecast/%s/%s,%s?units=si' % (configuration['apiKey'],
                                                              configuration['longitude'],
                                                              configuration['latitude'])
jsonData = None
try:
    data = urllib2.urlopen(url, None, 30)
    jsonData = json.loads(data.read())
except Exception, e:
    print e
    sys.exit(-1)
    
r = weatherRecord()

#get the time
d = datetime.datetime.now()
d = d - datetime.timedelta(minutes=d.minute % 5,
                           seconds=d.second,
                           microseconds=d.microsecond)
r.recordDate = d
r.station = configuration['stationName']
r.temperature = jsonData['currently']['temperature']
r.dewPoint = jsonData['currently']['dewPoint']
r.relativeHumidity = jsonData['currently']['humidity']
r.airPressure = jsonData['currently']['pressure']
r.windSpeed = jsonData['currently']['windSpeed']
r.windDirection = str(jsonData['currently']['windBearing'])
r.rainfall = jsonData['currently']['precipIntensity']

sqldb = sqlite3.connect(configuration['dbfile'])
try:
    r.saveToDb(sqldb)
except Exception, e:
    print e
