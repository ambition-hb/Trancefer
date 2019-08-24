#-*- coding:utf-8 –*-

from pymongo import MongoClient

class Mongo1:

    def __init__(self):
        # uri = 'mongodb://dev:ChenShi2017@58.206.96.135:20250/?authSource=PM&authMechanism=SCRAM-SHA-1'
        self.client = MongoClient('58.206.96.135:20250',
                                  username='dev',
                                  password='Chenshi2017',
                                  authMechanism='SCRAM-SHA-1')
        self.db = self.client.EL_shi