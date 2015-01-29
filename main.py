__author__ = 'Kyle'

import urllib.request
import json
import datetime
import sqlite3
import time
import sys
from PyQt4.QtCore import *
from PyQt4.QtGui import *


class EarthquakeThread(QThread):

    newEarthquake = pyqtSignal(int)

    """ This class processes the data from the api and emits a signal when a new earthquake is detected.
    """
    def __init__(self):

        super().__init__()
        self.url = "http://apis.is/earthquake/is"
        self.earthquakeTable = """

            CREATE TABLE Earthquakes(
            EarthquakeID integer,
            EarthquakeTime text,
            EarthquakeDepth real,
            EarthquakeLocation text,
            EarthquakeLat text,
            EarthquakeLng text,
            EarthquakeSize real,
            EarthquakeQuality real,
            Primary Key(EarthquakeID));

        """

        self.create_table("backend.db","Earthquakes",self.earthquakeTable)

    def run(self):
        while True:
            jsonData = self.getData()
            with sqlite3.connect("backend.db") as db:
                for each in jsonData['results']:
                    quality = each["quality"]
                    depth = each["depth"]
                    size = each["size"]
                    lat = each["latitude"]
                    lng = each["longitude"]
                    location = each["humanReadableLocation"]
                    dateTime = datetime.datetime.strptime(each['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")

                    cursor = db.cursor()

                    sql = "SELECT * FROM Earthquakes WHERE EarthquakeTime = ?"
                    cursor.execute(sql,(dateTime,))
                    results = cursor.fetchall()
                    if len(results) == 0:
                        sql = """ INSERT INTO Earthquakes(EarthquakeTime,EarthquakeDepth,EarthquakeLocation,EarthquakeLat,EarthquakeLng,EarthquakeSize,EarthquakeQuality)
                        VALUES(?,?,?,?,?,?,?);
                        """
                        values = (dateTime,depth,location,lat,lng,size,quality)
                        cursor.execute(sql,values)
                        print("Earthquake Added")
                        earthquakeId = cursor.lastrowid
                        self.newEarthquake.emit(earthquakeId)
                        db.commit()
            time.sleep(30)

    def getData(self):

        data = urllib.request.urlopen(self.url)
        rawData = data.read().decode("utf-8")
        jsonData = json.loads(rawData)

        return jsonData

    def create_table(self,db_name,table_name,sql):
        with sqlite3.connect(db_name) as db:
            cursor = db.cursor()
            cursor.execute("select name from sqlite_master where name=?",(table_name,))
            result = cursor.fetchall()
            if len(result) == 1:
                pass
            else:
                cursor.execute(sql)
                db.commit()



earthquakeTable = """
        CREATE TABLE Earthquakes(
    EarthquakeID integer,
    EarthquakeTime text,
    EarthquakeDepth real,
    EarthquakeLocation text,
    EarthquakeLat text,
    EarthquakeLng text,
    EarthquakeSize real,
    EarthquakeQuality real,
    Primary Key(EarthquakeID));

    """

def create_database():

    sql = """
        CREATE TABLE Earthquakes(
    EarthquakeID integer,
    EarthquakeTime text,
    EarthquakeDepth real,
    EarthquakeLocation text,
    EarthquakeLat text,
    EarthquakeLng text,
    EarthquakeSize real,
    EarthquakeQuality real,
    Primary Key(EarthquakeID));

    """

    with sqlite3.connect("backend.db") as db:
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()


def create_table(db_name,table_name,sql):
    with sqlite3.connect(db_name) as db:
        cursor = db.cursor()
        cursor.execute("select name from sqlite_master where name=?",(table_name,))
        result = cursor.fetchall()
        if len(result) == 1:
            pass
        else:
            cursor.execute(sql)
            db.commit()




def getEarthquakes():
    with sqlite3.connect("backend.db") as db:
        for each in jsonData['results']:
            quality = each["quality"]
            depth = each["depth"]
            size = each["size"]
            lat = each["latitude"]
            lng = each["longitude"]
            location = each["humanReadableLocation"]
            dateTime = datetime.datetime.strptime(each['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")

            cursor = db.cursor()

            sql = "SELECT * FROM Earthquakes WHERE EarthquakeTime = ?"
            cursor.execute(sql,(dateTime,))
            results = cursor.fetchall()
            if len(results) == 0:
                sql = """ INSERT INTO Earthquakes(EarthquakeTime,EarthquakeDepth,EarthquakeLocation,EarthquakeLat,EarthquakeLng,EarthquakeSize,EarthquakeQuality)
                VALUES(?,?,?,?,?,?,?);
                """
                values = (dateTime,depth,location,lat,lng,size,quality)
                cursor.execute(sql,values)
                print("Earthquake Added")
                db.commit()



if __name__ == "__main__":
    def showNew(eId):
        print("New",eId)
    app = QCoreApplication([])
    thread = EarthquakeThread()
    thread.finished.connect(app.exit)
    thread.start()
    thread.newEarthquake.connect(showNew)
    sys.exit(app.exec_())