from locust import Locust, TaskSet, task
from locust import events
from random import randint, random

import time
import json

FILE = open("/tmp/locust-%s.log" % (time.ctime()), 'w', 1)
HEADER= "querytime, numFeatures, totalServerTime, responseTime\n"


class Status:
    def __init__(self):
        self.hatch_complete = False

    def set_hatch_complete(self):
        self.hatch_complete = True

STATUS = Status()


def on_hatch_complete(num):
    STATUS.set_hatch_complete()
    FILE.write("num. users: " + str(num) + "\n")
    FILE.write(HEADER)

def write_to_file(msg):
    if (STATUS.hatch_complete):
        FILE.write(msg)

class WebsiteTasks(TaskSet):

    def genCoordinateX(self):
        minX = 0
        maxX = 200000
        return minX + random() * (maxX - minX)

    def genCoordinateY(self):
        minY = 0
        maxY = 200000
        return minY + random() * (maxY - minY)


    def genLength(self):
        return randint(1000, 20000)

    def generateQuery(self):
        l = self.genLength()
        llX = self.genCoordinateX()
        llY = self.genCoordinateY()
        bbox = "%f,%f,%f,%f" % (llX, llY, llX + l, llY + l)
        return bbox

    @task
    def query(self):
        start = time.time()
        resp = self.client.get("/api/databases/test/nstest/query", name="query", params={"bbox": self.generateQuery()})
        if (resp.status_code == 200):
            end = time.time()
            js  = resp.json
            msg = "%d, %d, %d, %f\n" % (js["query-time"], js["total"], js["totalTime"], (end - start) * 1000)
            #print(msg)
            write_to_file(msg)


    def exit_handler(self):
        self.file.close()



events.quitting += FILE.close
events.hatch_complete += on_hatch_complete

class WebsiteUser(Locust):
    task_set = WebsiteTasks
    min_wait = 1000
    max_wait = 5000
