from locust import Locust, TaskSet, task
from locust import events
from random import randint, random

import time
import json

FILE = open("/tmp/locust-pgsql-%s.log" % (time.ctime()), 'w', 1)
HEADER= "numFeatures, responseTime\n"
FILE.write(HEADER)

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
        resp = self.client.get("/featureserver/rest/tables/public.nstest.json", name="query", params={"bbox": self.generateQuery()})
        if (resp.status_code == 200):
            end = time.time()
            js  = resp.json
            msg = "%d, %f" % (js["total"], (end - start) * 1000)
            print(msg)
            FILE.write(msg)
            FILE.write("\n")

    def exit_handler(self):
        self.file.close()

events.quitting += FILE.close

class WebsiteUser(Locust):
    task_set = WebsiteTasks
    min_wait = 1000
    max_wait = 5000
