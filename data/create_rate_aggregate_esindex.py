from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
import pymongo
from pymongo.errors import ConnectionFailure
import psycopg2
import re
import argparse


arg_parser = argparse.ArgumentParser(description='Process aggregate plan data into elastic search')
arg_parser.add_argument('--mongo', dest='mongohost', default='127.0.0.1')
arg_parser.add_argument('--pghost', dest='postgreshost', default='w210.cxihwctrc5di.us-west-1.rds.amazonaws.com')
arg_parser.add_argument('--eshost', dest='eshost',
                        default='https://search-acaproject-yayvqakrnkdvdfd5m6kyqonp5a.us-west-1.es.amazonaws.com/')

args = arg_parser.parse_args()


# get a connection to the database
def connect_db(host):
    conn = psycopg2.connect(user="acaproject",
                            database="acaproject",
                            password="test1234",
                            host=host,
                            port="5432")
    return conn


def connect_mongodb(host):
    try:
        client = pymongo.MongoClient(host, 27017)
        return client
    except ConnectionFailure as e:
        print "Unable to connect to MongoDB instance\n{0}\n".format(str(e))


#  query mongodb formulary db for all the drugs in the drug collection under this plan id.
def fetch_drugs_for_plan(client, plan_id):
    db = client.formularies
    cursor = db.drugs.find({"plans.plan_id": plan_id },{"_id":0, "drug_name" : 1, "rxnorm_id" : 1})
    drugs = []
    for drug in cursor:
        drugs.append(drug)
    return drugs


# query mongodb provider db for all the providers in the provider collection under this plan id
# TODO: do same for facilities


#  db.providers.aggregate([{"$match": {"plans.plan_id":"10191NJ0030001" }}, {"$unwind": "$addresses"},
#  {"$project": {"_id":0, "npi" :1, alias:{"$concat":["$name.first"," ","$name.last"]} , "addresses" : 1,
#  "speciality" : 1}}, {"$group" : {"_id":{"npi":"$npi","name":"$alias"}, "address":{"$first":"$addresses"}}},
#  {$project: {"_id.npi":1, "_id.name":1,"address":1 }}])

def fetch_providers_for_plan(client, plan_id):
    db = client.providers
    cursor = db.providers.find( {"plans.plan_id" : plan_id },
                                { "_id": 0, "npi": 1,  "$concat": ["$name.first", " ", "$name.last"],
                                  "addresses": {"$slice":1}, "speciality": 1} )
    providers = []
    for provider in cursor:
        # combine first name and last name
        providers.append(provider)

    return providers


# TODO: implement
def fetch_conditions_for_plan(plan_id):
    pass


db_conn = connect_db(args.postgreshost)
cur = db_conn.cursor()

es = Elasticsearch(args.eshost)
ic = IndicesClient(es)

mongo_client = connect_mongodb(args.mongohost)


# regular expression to pick off the number at the end of the Rating Area string
re_rating_area_number = re.compile(r'([0-9]+)$')

column = ['issuer',
          'issuerid',
          'issuerName',
          'planid',
          'standardcomponentid',
          'planname',
          'plantype',
          'metallevel',
          'url',
          'age',
          'ratingareaid',
          'tobacco',
          'individualrate',
          'individualtobaccorate',
          'primarysubscriberandonedependent',
          'primarysubscriberandtwodependents',
          'primarysubscriberandthreeormoredependents',
          'couple',
          'coupleandonedependent',
          'coupleandtwodependents',
          'coupleandthreeormoredependents']

parameters = {'statecode' : 'AK' }

cur.execute( 'SELECT '
             'hi."ISSR_LGL_NAME" AS issuer,'
             'hi."HIOS_ISSUER_ID" as issuerid,'
             'hi."MarketingName" as issuerName,'
             'pa.planid,'
             'pa.standardcomponentid,'
             'pa.planmarketingname,'
             'pa.plantype,'
             'pa.metallevel,'
             'pa.planbrochure,'
             'r.age,'
             'r.ratingareaid,'
             'r.tobacco,'
             'r.individualrate,'
             'r.individualtobaccorate,'
             'r.primarysubscriberandonedependent,'
             'r.primarysubscriberandtwodependents,'
             'r.primarysubscriberandthreeormoredependents,'
             'r.couple,'
             'r.coupleandonedependent,'
             'r.coupleandtwodependents,'
             'r.coupleandthreeormoredependents '
             'FROM plan_attributes pa,'
             'hios_issuer hi,'
             'rates r '
             'WHERE pa.issuerid = hi."HIOS_ISSUER_ID" '
             'AND r.planid = pa.standardcomponentid '
             'AND pa.statecode = %(statecode)s AND pa.dentalonlyplan=\'No\' '
             'AND r.rateexpirationdate > CURRENT_DATE '
             'AND r.rateeffectivedate <= CURRENT_DATE '
             'GROUP BY '
             'pa.standardcomponentid, pa.planid, hi."ISSR_LGL_NAME", hi."HIOS_ISSUER_ID", '
             'hi."MarketingName", pa.planmarketingname, pa.plantype, '
             'pa.metallevel, pa.planbrochure, r.age, r.ratingareaid, r.individualrate, '
             'r.individualtobaccorate, r.primarysubscriberandonedependent, '
             'r.primarysubscriberandtwodependents, r.primarysubscriberandthreeormoredependents,'
             'r.couple, r.coupleandonedependent, r.coupleandtwodependents, '
             'r.coupleandthreeormoredependents, r.tobacco', parameters)

plan_id = None
plan_name = None
issuer_name = None
plan_type = None
metal_level = None
url = None

premiums = []
es_doc = {}
for row in cur:
    # NOTE: the plan_id and standardcomponentid are not quite the same thing, but the plan_id has multiple meanings
    # that overlap with the standard component id. In these cases we are using the term plan_id even though for
    # plan_attributes and benefits_and_cost_sharing it is equivalent to the standardcomponentid. The
    # standardcomponentid looks like the plan_id but with a suffix of -01 or similar.
    if plan_id != row[column.index('standardcomponentid')]:
        # new plan id, start a new aggregation
        if len(premiums) > 0:
            # finalize the document
            es_doc = { 'plan_name': row[column.index('planname')],
                       'issuer' : row[column.index('issuerName')],
                       'plan_type': row[column.index('plantype')],
                       'level': row[column.index('metallevel')],
                       'url': row[column.index('url')],
                       'premium' : premiums,
                       'drugs' : fetch_drugs_for_plan(mongo_client, plan_id),
                       'providers': fetch_providers_for_plan(mongo_client, plan_id),
                       'conditions': fetch_conditions_for_plan(plan_id)
                       }

            # write the document to elastic search
            # TODO: choose the correct index name and document type
            es.index(index='myindex', doc_type='plan', id=plan_id, body=es_doc)
            # clear the premiums list and set the current plan_id
            premiums = []
            plan_id = row[column.index('standardcomponentid')]
            plan_type = row[column.index('plantype')]
            metal_level = row[column.index('metallevel')]
        else:
            if not plan_id:
                plan_id = row[column.index('standardcomponentid')]
                plan_type = row[column.index('plantype')]
                metal_level = row[column.index('metallevel')]

    m = re_rating_area_number.search(row[column.index('ratingareaid')])
    areaId = m.group()
    premium_label = 'age_' + row[column.index('age')] + '_areaID_' + areaId
    if row[column.index('individualtobaccorate')]:
        premiums.append({premium_label + '_individualtobacco': row[column.index('individualtobaccorate')]})
    if row[column.index('individualrate')]:
        premiums.append({premium_label + '_individual': row[column.index('individualrate')]})

    for index in range (column.index('primarysubscriberandonedependent'),
                        column.index('coupleandthreeormoredependents')):
        if row[index]:
            premiums.append({premium_label + '_' + column[index]: row[index]})

    # this is for double checking as we go along
    if metal_level != row[column.index('metallevel')]:
        print "Plan {0} AreaId {1} ID {2} Metal Level {3} also has Metal Level {4}"\
                .format(plan_id, areaId, row[column.index('planid')],
                        metal_level, row[column.index('metallevel')])
