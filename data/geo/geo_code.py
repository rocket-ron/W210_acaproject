import pymongo
from pymongo.errors import ConnectionFailure
import json
import argparse

arg_parser = argparse.ArgumentParser(description='Process aggregate plan data into elastic search')
arg_parser.add_argument('--mongo', dest='mongohost', default='ec2-52-53-173-200.us-west-1.compute.amazonaws.com')

args = arg_parser.parse_args()


def connect_mongodb(host):
    try:
        connection_string = 'mongodb://tgthomas:mongo112358@{0}:27017/providers?authMechanism=MONGODB-CR'.format(host)
        client = pymongo.MongoClient(connection_string)
        return client
    except ConnectionFailure as e:
        print "Unable to connect to MongoDB instance\n{0}\n".format(str(e))


def fetch_providers_for_state(client, state):
    db = client.providers

    # find all the addresses for each provider and aggregate the components to form a list of "complete" addresses
    cursor = db.providers.aggregate([{"$match": {"addresses.state": state}},
                                    {"$unwind": "$addresses"},
                                    {"$project": {"npi": 1, "addr": {
                                        "$concat": [
                                            "$addresses.address", " ",
                                            "$addresses.address_2", " ",
                                            "$addresses.city", " ",
                                            "$addresses.state", " ",
                                            "$addresses.zip"]
                                    }}}])


    _addresses = []
    for address in cursor:
        _addresses.append(address)

    # filter out null addresses
    _addresses = [x for x in _addresses if x['addr']]

    # remove repeated addresses
    a = []
    for item in _addresses:
        if item['addr'] not in a:
            a.append(item['addr'])
        else:
            _addresses.remove(item)

    return _addresses


def load_addresses():
    mongo_client = connect_mongodb(args.mongohost)

    # all the states that are in the ACA data
    all_states = ['PA', 'AZ', 'FL', 'LA', 'MT', 'NM', 'AK', 'NC', 'OR', 'MS', 'AR', 'MO', 'IL', 'IN', 'HI', 'WY', 'UT',
                  'MI', 'KS', 'GA', 'WI', 'NE', 'OH', 'NV', 'OK', 'AL', 'ND', 'DE', 'WV', 'ME', 'TN', 'VA', 'SD', 'NH',
                  'IA', 'SC', 'TX', 'NJ']

    address_list = {}
    for state in ['NJ']:
        address_list[state] = []
        addresses = fetch_providers_for_state(mongo_client, state)
        for item in addresses:
            address = item['addr']
            if address not in address_list[state]:
                address_list[state] = {'address': address, 'npi': [item['npi']]}
            else:
                address_list[state]['npi'].append(item['npi'])

        with open(state+'address.json', 'w') as f:
            json.dump(address_list)

#url = 'https://maps.googleapis.com/maps/api/geocode/json'
#params = {'address': 'data',
#          'key': 'mykey'}
#response = get(url, data=params)


if __name__ == '__main__':
    load_addresses()
