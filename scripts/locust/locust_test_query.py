from locust import Locust, TaskSet, task

from random import randint, random

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
        return randint(1000, 10000)

    def generateQuery(self):
        l = self.genLength()
        llX = self.genCoordinateX()
        llY = self.genCoordinateY()
        bbox = "%f,%f,%f,%f" % (llX, llY, llX + l, llY + l)
        return bbox

    @task
    def query(self):
        self.client.get("api/databases/test/nstest/query", name="query", params={"bbox": self.generateQuery()})


class WebsiteUser(Locust):
    task_set = WebsiteTasks
    min_wait = 5000
    max_wait = 15000
