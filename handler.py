# -*- coding:utf-8 -*-
import tornado.web
import motor


class WebHandler(tornado.web.RequestHandler):

    def __init__(self, mongo_url, mongo_database, mongo_collections):
        self.db = motor.MotorClient(mongo_url)[mongo_database][mongo_collections]

    def get(self):
        self.write("Hello, world")