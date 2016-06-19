import boto3
import botocore
from boto3.s3.transfer import S3Transfer
import csv
import json
import requests
from requests.exceptions import SSLError
from urlparse import urlparse
import os
import hashlib
import psycopg2

# Download to a local file
def download_file(_url):
  h = hashlib.md5(_url).hexdigest()
  local_file = h + '.tmp'
  r = requests.get(_url, stream=True)
  with open(local_file, 'wb') as f:
    for chunk in r.iter_content(chunk_size=1024*64):
      if chunk:
        f.write(chunk)
  return local_file

# Upload to S3 bucket
def xfer_to_S3(file_name, bucket, key):
  client = boto3.client('s3', 'us-west-1')
  transfer = S3Transfer(client)
  transfer.upload_file(file_name, bucket, key)

# download to a local file and then transfer to S3
# using the hashed URL as the S3 key
def process_url(_url, bucket_name, prefix):
  print "Processing {0}".format(_url)
  hashed_url = hashlib.md5(_url).hexdigest()
  f = download_file(_url)
  xfer_to_S3(f, bucket_name, prefix + str(hashed_url))
  # os.remove(f)
  return hashed_url

def connect_db():
  DB_NAME = 'acaproject'
  conn=psycopg2.connect(user="acaproject",
                        database="acaproject",
                        password="test1234",
                        host="w210.cxihwctrc5di.us-west-1.rds.amazonaws.com",
                        port="5432")
  return conn

######################################################################
#
# MAIN SCRIPT STARTS HERE
#
######################################################################

db_conn = connect_db()
cur = db_conn.cursor()
update_cur = db_conn.cursor()
cur.execute("SELECT url FROM jsonurls WHERE s3key is NULL")
urls = cur.fetchall()

for u in urls:
  _url = {}
  # results are tuples where the first item is the url in this case
  _url['url'] = u[0]
  try:
    _url['s3key'] = process_url(_url['url'], 'w210', 'json/')
    _url['status'] = 'PROCESSED'
    update_cur.execute("UPDATE jsonurl SET "
                       "status = (SELECT id FROM retrieval_status WHERE status=%(status)s), "
                       "s3key = %(s3key)s "
                       "WHERE url = %(url)s" ,
                       _url
      )
  except Exception as ex:
    _url['status'] = 'ERROR'
    update_cur.execute("UPDATE jsonurl SET "
                       "status = (SELECT id FROM retrieval_status WHERE status=%(status)s) "
                       "WHERE url = %(url)s" ,
                       _url
      )
  conn.commit()
cur.close()
update_cur.close()
conn.close()



