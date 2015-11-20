import bson
import datetime
import pytz
import random
import string
import sys

from bson.json_util import dumps, loads

NUM_DOCS = 10000

# Random types
def rand_key():
    return ''.join(random.choice(string.ascii_letters) for _ in range(8))
def rand_int32():
    return random.randint(-100, 100)
def rand_int64():
    return {'$numberLong': random.randint(sys.maxint, 2*sys.maxint)}
def rand_double():
    return random.random() * sys.maxint - 1
def rand_bool():
    return bool(random.getrandbits(1))
def rand_string():
    return ''.join(random.choice(string.ascii_letters) for _ in range(80))
def rand_array():
    return [random.randint(1, 10) for _ in range(random.randint(1, 10))]
def rand_bindata():
    return bson.Binary(rand_string())  # won't work for python 3
def rand_date():
    start_date = datetime.datetime(1992, 6, 30, tzinfo=pytz.UTC).toordinal()
    end_date = datetime.datetime.now(tz=pytz.UTC).toordinal()
    return datetime.datetime.fromordinal(random.randint(start_date, end_date))
def rand_regex():
    return bson.Regex(rand_key())
def rand_js():
    return bson.Code(rand_string())
def rand_js_context():
    context = {rand_key(): rand_int32() for _ in range(random.randint(1, 10))}
    return bson.Code(rand_string(), scope=context)
def rand_ts():
    return bson.Timestamp(rand_date(), 1)
def false():
    return False
def true():
    return True
def rand_subdoc(document, depth):
    if depth >= 6:
        return rand_key()
    document['left'] = rand_subdoc({}, depth + 1)
    document['right'] = rand_subdoc({}, depth + 1)
    return document

def flat_bson():
    random_funcs = [rand_string, rand_int32, rand_int64, rand_double,
                    rand_bool, rand_string]
    with open('flat_bson.json', 'w') as fle:
        for _ in range(NUM_DOCS):
            doc = {rand_key(): gen_value() for gen_value in random_funcs
                   for _ in range(24)}
            doc['_id'] = bson.ObjectId()
            fle.write(dumps(doc) + '\n')

def deep_bson():
    with open('deep_bson.json', 'w') as fle:
        for _ in range(NUM_DOCS):
            fle.write(dumps(rand_subdoc({}, 0)) + '\n')

def full_bson():
    random_funcs = [rand_string, rand_double, rand_int64, rand_int32,
                    true, false, bson.MinKey, bson.MaxKey, rand_array,
                    rand_bindata, rand_date, rand_regex, rand_js,
                    rand_js_context, rand_ts]
    with open('full_bson.json', 'w') as fle:
        for _ in range(NUM_DOCS):
            doc = {rand_key(): gen_value() for gen_value in random_funcs
                   for _ in range(6)}
            doc['_id'] = bson.ObjectId()
            fle.write(dumps(doc) + '\n')

flat_bson()
print "done with flat_bson"
deep_bson()
print "done with deep_bson"
full_bson()
print "done with full_bson"
