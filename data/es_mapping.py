import pymongo
from pymongo.errors import ConnectionFailure
import argparse


# get a connection to MongoDB
def connect_mongodb(host):
    try:
        mongodb_conn = pymongo.MongoClient(host, 27017)
        return mongodb_conn
    except ConnectionFailure as e:
        print "Unable to connect to MongoDB instance\n{0}\n".format(str(e))


arg_parser = argparse.ArgumentParser(description='Process ElasticSearch Mapped Indices')
arg_parser.add_argument('--mongo', dest='mongohost', default='127.0.0.1')
args = arg_parser.parse_args()

mongo_client = connect_mongodb(args.mongohost)
db = mongo_client.plans

# get a cursor to work through all the plans
cursor = db.plans.distinct("plan_id", {},{"marketing_name":1, })

