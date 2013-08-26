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
        self.count = 0

    def set_hatch_complete(self):
        self.start_time = time.time()
        self.hatch_complete = True
        self.count = 0

    def increment(self):
        self.count += 1

    def stop(self):
        self.total_time = (time.time() - self.start_time)


STATUS = Status()


def on_hatch_complete(num):
    STATUS.set_hatch_complete()
    FILE.write("num. users: " + str(num) + "\n")
    FILE.write(HEADER)

def write_to_file(msg):
    if (STATUS.hatch_complete):
        FILE.write(msg)

def update_status():
    if (STATUS.hatch_complete):
        STATUS.increment()

def on_quitting():
       STATUS.stop()
       FILE.write("throughput:" + str(float(STATUS.count)/float(STATUS.total_time)) + " \n")
       FILE.close


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
            write_to_file(msg)
            update_status()


    def exit_handler(self):
        self.file.close()



events.quitting += on_quitting
events.hatch_complete += on_hatch_complete

class WebsiteUser(Locust):
    task_set = WebsiteTasks
    min_wait = 250
    max_wait = 2500
