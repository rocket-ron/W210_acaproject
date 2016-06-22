from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import IndicesClient
import boto3
import botocore
import json
import os
import psycopg2

# download from S3 bucket into local temp file
def xfer_from_s3(key, bucket):
  filename='tmp.json'
  # remove the temp file if it exists
  try:
    os.remove(filename)
  except OSError:
    pass
  s3 = boto3.client('s3')
  response = s3.download_file(bucket, key, filename)
  return filename

# get a connection to the database
def connect_db():
  DB_NAME = 'acaproject'
  conn=psycopg2.connect(user="acaproject",
                        database="acaproject",
                        password="test1234",
                        host="w210.cxihwctrc5di.us-west-1.rds.amazonaws.com",
                        port="5432")
  return conn

# prepare the JSON documents for bulk load into elasticsearch
def process_formulary_into_es(fname, es):
  status = False
  with open(fname, 'r') as infile:
    data=infile.read().replace('\n', '')
  try:
    docs = json.loads(data)
    actions = []
    for doc in docs:
      action = {
          "_index": "data",
          "_type": "drug",
          "_source": doc
      }
      actions.append(action)
    if len(actions) > 0:
        helpers.bulk(es, actions)
    status = True
  except KeyboardInterrupt, SystemExit:
    conn.rollback()
    raise
  except UnicodeDecodeError:
    pass
  return status

# prepare the JSON docuements for bulk load into elasticsearch
def process_plan_into_es(fname, es):
  status = False
  with open(fname, 'r') as infile:
    data=infile.read().replace('\n', '')
  try:
    docs = json.loads(data)
    actions = []
    for doc in docs:
      action = {
          "_index": "data",
          "_type": "plan",
          "_source": doc
      }
      actions.append(action)
    if len(actions) > 0:
      helpers.bulk(es, actions)
    status = True
  except KeyboardInterrupt, SystemExit:
    conn.rollback()
    raise
  except UnicodeDecodeError:
    pass
  return status

# prepare the JSON documents for bulk load into elasticsearch
def process_provider_into_es(fname, es):
  status = False
  with open(fname, 'r') as infile:
    data=infile.read().replace('\n', '')
  try:
    docs = json.loads(data)
    actions = []
    for doc in docs:
      if doc['type'] == 'INDIVIDUAL':
        action = {
            "_index": "data",
            "_type": "provider",
            "_source": doc
        }
      else:
        action = {
            "_index": "data",
            "_type": "facility",
            "_source": doc
        }
        actions.append(action)
    if len(actions) > 0:
      helpers.bulk(es, actions)
    status = True
  except KeyboardInterrupt, SystemExit:
    conn.rollback()
    raise
  except UnicodeDecodeError:
    pass
  return status

db_conn = connect_db()
cur = db_conn.cursor()

es = Elasticsearch("https://search-acaproject-yayvqakrnkdvdfd5m6kyqonp5a.us-west-1.es.amazonaws.com/")
ic = IndicesClient(es)

# Get the formulary documents
count = 0;
cur.execute("SELECT id,s3key FROM jsonurls WHERE es_index is FALSE AND type=3 AND s3key is not null")
for id,key in cur.fetchall():
  fname = xfer_from_s3('json/'+key, 'w210')
  if process_formulary_into_es(fname, es):
    update_cursor = db_conn.cursor()
    update_cursor.execute("UPDATE jsonurls SET es_index=TRUE WHERE id=%(id)s", {'id': id})
    db_conn.commit()
    update_cursor.close()
  else:
    count += 1

print "{0} formularies failed to upload".format(count)

# Get the plan documents
count = 0
cur.execute("SELECT id,s3key FROM jsonurls WHERE es_index is FALSE AND type=2 AND s3key is not null")
for id,key in cur.fetchall():
  fname = xfer_from_s3('json/'+key, 'w210')
  if process_plan_into_es(fname, es):
    update_cursor = db_conn.cursor()
    update_cursor.execute("UPDATE jsonurls SET es_index=TRUE WHERE id=%(id)s", {'id': id})
    db_conn.commit()
    update_cursor.close()
  else:
    count += 1

print "{0} plans failed to upload".format(count)

# Get the provider and facility documents
count = 0
cur.execute("SELECT id,s3key FROM jsonurls WHERE es_index is FALSE AND type=1 AND s3key is not null")
for id,key in cur.fetchall():
  fname = xfer_from_s3('json/'+key, 'w210')
  if process_provider_into_es(fname, es):
    update_cursor = db_conn.cursor()
    update_cursor.execute("UPDATE jsonurls SET es_index=TRUE WHERE id=%(id)s", {'id': id})
    db_conn.commit()
    update_cursor.close()
  else:
    count += 1

print "{0} providers failed to upload".format(count)

# close all database connections
cur.close()
db_conn.close()
