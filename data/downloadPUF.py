import boto3
import botocore
from boto3.s3.transfer import S3Transfer
import csv
import json
import requests
from urlparse import urlparse
import os

"""
Walk through the Machine Readable PUF and follow the links for each insurer.
Download the JSON files to S3 storage and create a map that maps the PUF
to the files in S3. 

"""

# Download to a local file
def download_file(url):
    local_file = str(hash(url)) + '.tmp'
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

# process the PUF file, keeping track of which URLs have been processed
# and writing the URL - hash map to a file in JSON format
def process_machine_readable_puf(csv_filename, s3, bucket_name, prefix):
    url_hashmap = {}
    url_map_fname = 'puf-url-json-map.json'
    
    try:
        with open(url_map_fname, 'r') as map_file:
            url_hashmap = json.load(map_file)
            # print url_hashmap
    except IOError:
        pass
    
    try:
        with open(csv_filename, 'r') as urlfile:
            urls = csv.DictReader(urlfile)
            for row in urls:
                _url = row['URL Submitted']
                url_parseresult = urlparse(_url)

                # minimal check to make sure the url begins with scheme:// and is not empty
                if url_parseresult.scheme:
                    process_puf_url(_url, bucket_name, prefix, url_hashmap)

                    # write the updated dictionary to disk
                    with open(url_map_fname, 'w') as map_file:
                        map_file.write(json.dumps(url_hashmap))
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)

def process_puf_url(puf_url, bucket_name, prefix, url_map):
    
    print "Processing {0}...".format(puf_url)
    response = requests.get(puf_url)

    links = json.loads(response.content)
    print ("\nProvider URLS:")
    print ("==================================")
    for provider_url in links['provider_urls']:
        if provider_url not in url_map:
            url_map[provider_url] = process_url(provider_url, bucket_name, prefix)
    
    print ("\nFormulary URLS:")
    print ("==================================")    
    for formulary_url in links['formulary_urls']:
        if provider_url not in url_map:
            url_map[formulary_url] = process_url(formulary_url, bucket_name, prefix)
        
    print ("\nPlan URLS:")
    print ("==================================")        
    for plan_url in links['plan_urls']:
        if provider_url not in url_map:
            url_map[plan_url] = process_url(plan_url, bucket_name, prefix)

# download to a local file and then transfer to S3
# using the hashed URL as the S3 key
def process_url(_url, bucket_name, prefix):
    print "Processing {0}".format(_url)
    hashed_url = hash(_url)
    f = download_file(_url)
    xfer_to_S3(f, bucket_name, prefix + str(hashed_url))
    # os.remove(f)  
    return hashed_url

s3 = boto3.resource('s3')
process_machine_readable_puf('machine-readable-url-puf.csv', s3, 'w210', 'json/')

