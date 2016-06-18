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

# Download to a local file
def download_file(url):
  h = hashlib.md5(item['url']).hexdigest()
  local_file = h + '.tmp'
  r = requests.get(url, stream=True)
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
  hashed_url = hashlib.md5(item['url']).hexdigest()
  f = download_file(_url)
  xfer_to_S3(f, bucket_name, prefix + str(hashed_url))
  # os.remove(f)
  return hashed_url

# reconstitute the dictionary from the file on disk
def load_urls(urlfile):
  urls = []
  with open(urlfile, 'r') as infile:
    for line in infile.readlines():
      urls.append(json.loads(line.strip()))
  return urls

######################################################################
#
# MAIN SCRIPT STARTS HERE
#
######################################################################
for fname in ['provider-urls.txt','plan-urls.txt','formulary-urls.txt']:
  urls = load_urls(fname)
  for _url in urls:
    if _url['status'] == 'NEW':
      try:
        _url['s3key'] = process_url(_url['url'], 'w210', 'json/')
        _url['status'] = 'PROCESSED'
      except Exception as ex:
        _url['status'] = 'ERROR'

  with open(fname, 'w') as outfile:
    for _url in urls:
      outfile.write("{0}\n".format(json.dumps(_url)))
