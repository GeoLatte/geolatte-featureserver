
from locust import Locust, TaskSet, task
from locust import events
from random import randint, random
import gevent

ID = 1

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

    @task(1)
    def query1(self):
        print(self.locust.id)        
        resp = self.client.get("/api/databases/test/nstest/query", params={"bbox": "100,100,5000,5000"})         

    # @task(1)
    # def query2(self):
    #    print(self.locust.id)
    #    resp = self.client.get("/api/databases/test/nstest/query", params={"bbox": "10000,10000,15000,15000"})

    # @task(1)
    # def query3(self):
    #     print(self.locust.id)        
    #     resp = self.client.get("/api/databases/test/nstest")


class WebsiteUser(Locust):
    task_set = WebsiteTasks
    min_wait = 100
    max_wait = 100
    
    def __init__(self):
        global ID     
        Locust.__init__(self)   
        self.id = ID
        ID += 1 
