from ijson import common
from ijson.backends import yajl2
from itertools import imap


def floaten(event):
    if event[1] == 'number':
        return (event[0], event[1], float(event[2]))
    else:
        return event

with open('tmp.json','r') as f:
    event = imap(floaten, yajl2.parse(f))
    data = common.items(event, 'item')
    for d in data:
        pass



