__author__ = 'Kyle Kirkby'

import urllib.request
import json
import datetime
import sqlite3
import time
import sys


class EarthQuakeData:

    def __init__(self):

        self.url = "http://apis.is/earthquake/is"
        self.jsonData = self.collectData()

        self.formattedQuakes = self.parseData(self.jsonData)


    def collectData(self):

        request = urllib.request.urlopen(self.url)
        data = request.read().decode("utf-8")
        jsonData = json.loads(data)

        return jsonData


    def parseData(self, data):

        earthQuakes = []
        
        for earthquake in data['results']:
            earthQuakes.append(earthquake)
     

        return earthQuakes


if __name__ == "__main__":

    earthquakes = EarthQuakeData()

    data = earthquakes.formattedQuakes

    largestQuake = 0.0

    for each in data:
        print(str(each["size"]) + "\tb" + str(each["humanReadableLocation"]) + "\tb" + str(each["latitude"]))
        if each["size"] > largestQuake:
            largestQuake = each["size"]


    print("Largest Quake:" + str(largestQuake))
