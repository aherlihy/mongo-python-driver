# Copyright 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the MongoDB Driver Performance Benchmarking Spec."""

import json
import multiprocessing as mp
import os
import shutil
import sys
import warnings

sys.path[0:0] = [""]

from bson import BSON, CodecOptions
from bson.json_util import loads
from bson.objectid import ObjectId
from gridfs import GridFSBucket
from pymongo import MongoClient
from pymongo.monotonic import time
from pymongo.operations import InsertOne
from test import client_context, host, port, unittest

NUM_ITERATIONS = 100
MAX_ITERATION_TIME = 300
NUM_CYCLES = 10000  # Number of times to iterate inside do_task

TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('performance_testdata'))


class Timer(object):
    def __enter__(self):
        self.start = time()
        return self

    def __exit__(self, *args):
        self.end = time()
        self.interval = self.end - self.start


class PerformanceTest(object):
    def setUp(self):
        pass

    def tearDown(self):
        print('Running %s. MEDIAN=%s' % (self.__class__.__name__,
                                         self.percentile(50)))

    def before(self):
        pass

    def after(self):
        pass

    def percentile(self, percentile):
        if hasattr(self, 'results'):
            sorted_results = sorted(self.results[:self.max_iterations])
            percentile_index = int(self.max_iterations * percentile / 100) - 1
            return sorted_results[percentile_index]
        else:
            self.fail('Test execution failed')

    def runTest(self):
        results = [0 for _ in range(NUM_ITERATIONS)]
        start = time()
        self.max_iterations = NUM_ITERATIONS
        for i in range(NUM_ITERATIONS):
            if time() - start > MAX_ITERATION_TIME:
                self.max_iterations = i
                warnings.warn('Test timed out, completed %s iterations.'
                              % self.max_iterations)
                break
            self.before()
            with Timer() as timer:
                self.do_task()
            self.after()
            results[i] = timer.interval

        self.results = results


class BsonEncodingTest(PerformanceTest):
    def setUp(self):
        # Location of test data.
        with open(
            os.path.join(TEST_PATH,
                         os.path.join('extended_bson', self.dataset))) as data:
            self.document = loads(data.read())

    def do_task(self):
        for _ in range(NUM_CYCLES):
            BSON.encode(self.document)


class BsonDecodingTest(PerformanceTest):
    def setUp(self):
        # Location of test data.
        with open(
            os.path.join(TEST_PATH,
                         os.path.join('extended_bson', self.dataset))) as data:
            self.document = BSON.encode(json.loads(data.read()))

    def do_task(self):
        for _ in range(NUM_CYCLES):
            self.document.decode(codec_options=CodecOptions(tz_aware=True))


class TestFlatEncoding(BsonEncodingTest, unittest.TestCase):
    dataset = 'flat_bson.json'


class TestFlatDecoding(BsonDecodingTest, unittest.TestCase):
    dataset = 'flat_bson.json'


class TestNestedEncoding(BsonEncodingTest, unittest.TestCase):
    dataset = 'deep_bson.json'


class TestNestedDecoding(BsonDecodingTest, unittest.TestCase):
    dataset = 'deep_bson.json'


class TestAllBsonTypesEncoding(BsonEncodingTest, unittest.TestCase):
    dataset = 'full_bson.json'


class TestAllBsonTypesDecoding(BsonDecodingTest, unittest.TestCase):
    dataset = 'full_bson.json'


class TestRunCommand(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

    def before(self):
        self.isMaster = {'isMaster': True}

    def do_task(self):
        for _ in range(NUM_CYCLES):
            self.client.perftest.command(self.isMaster)


class TestOneDocument(PerformanceTest):
    def setUp(self):
        # Location of test data.
        with open(os.path.join(
            TEST_PATH, os.path.join(
                'single_document', self.dataset)), 'r') as data:
            self.document = json.loads(data.read())

        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

    def tearDown(self):
        super(TestOneDocument, self).tearDown()
        self.client.drop_database('perftest')

    def before(self):
        self.corpus = self.client.perftest.corpus

    def after(self):
        self.client.perftest.drop_collection('corpus')


class TestFindOneByID(TestOneDocument, unittest.TestCase):
    def setUp(self):
        self.dataset = 'TWEET.json'
        super(TestFindOneByID, self).setUp()

        documents = [self.document.copy() for _ in range(NUM_CYCLES)]
        result = self.client.perftest.corpus.insert_many(documents)
        self.inserted_ids = result.inserted_ids

    def do_task(self):
        for i in self.inserted_ids:
            self.corpus.find_one({'_id': i})

    def after(self):
        pass


class TestSmallDocInsertOne(TestOneDocument, unittest.TestCase):
    def setUp(self):
        self.dataset = 'SMALL_DOC.json'
        super(TestSmallDocInsertOne, self).setUp()

    def do_task(self):
        for _ in range(NUM_CYCLES):
            self.corpus.insert_one(self.document)
            self.document.pop('_id')  # Hack! Ugh!


class TestLargeDocInsertOne(TestOneDocument, unittest.TestCase):
    def setUp(self):
        self.dataset = 'LARGE_DOC.json'
        super(TestLargeDocInsertOne, self).setUp()

    def do_task(self):
        for _ in range(10):
            self.corpus.insert_one(self.document)
            self.document.pop('_id')  # Hack! Ugh!


class TestFindManyAndEmptyCursor(TestOneDocument, unittest.TestCase):
    def setUp(self):
        self.dataset = 'TWEET.json'
        super(TestFindManyAndEmptyCursor, self).setUp()

        for _ in range(100):
            self.client.perftest.command(
                'insert', 'corpus',
                documents=[self.document for _ in range(1000)])

    def do_task(self):
        len(list(self.corpus.find({})))

    def after(self):
        pass


class TestSmallDocBulkInsert(TestOneDocument, unittest.TestCase):
    def setUp(self):
        self.dataset = 'SMALL_DOC.json'
        super(TestSmallDocBulkInsert, self).setUp()

    def before(self):
        self.corpus = self.client.perftest.corpus
        self.documents = [InsertOne(
            self.document.copy()) for _ in range(NUM_CYCLES)]

    def do_task(self):
        self.corpus.bulk_write(self.documents, ordered=True)


class TestLargeDocBulkInsert(TestOneDocument, unittest.TestCase):
    def setUp(self):
        self.dataset = 'LARGE_DOC.json'
        super(TestLargeDocBulkInsert, self).setUp()
        self.documents = [InsertOne(self.document.copy()) for _ in range(10)]

    def before(self):
        self.corpus = self.client.perftest.corpus

    def do_task(self):
        self.corpus.bulk_write(self.documents, ordered=True)


class TestGridFsUpload(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

        gridfs_path = os.path.join(
            TEST_PATH, os.path.join('single_document', 'GRIDFS_LARGE'))
        with open(gridfs_path, 'rb') as data:
            self.document = data.read()

    def tearDown(self):
        super(TestGridFsUpload, self).tearDown()
        self.client.drop_database('perftest')

    def before(self):
        self.client.perftest.drop_collection('fs.files')
        self.client.perftest.drop_collection('fs.chunks')
        self.bucket = GridFSBucket(self.client.perftest)
        self.bucket.upload_from_stream('init', b'x')

    def do_task(self):
        self.bucket.upload_from_stream('gridfstest', self.document)


class TestGridFsDownload(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

        gridfs_path = os.path.join(
            TEST_PATH, os.path.join('single_document', 'GRIDFS_LARGE'))

        bucket = GridFSBucket(self.client.perftest)
        with open(gridfs_path, 'rb') as gfile:
            self.uploaded_id = bucket.upload_from_stream('gridfstest', gfile)

    def tearDown(self):
        super(TestGridFsDownload, self).tearDown()
        self.client.drop_database('perftest')

    def before(self):
        self.bucket = GridFSBucket(self.client.perftest)

    def do_task(self):
        self.bucket.open_download_stream(self.uploaded_id).read()


def mp_map(map_func, length):
    pool = mp.Pool(mp.cpu_count())
    pool.map(map_func, length)
    pool.close()
    pool.join()


def insert_json_file(i):
    client = MongoClient(host, port)

    ldjson_path = os.path.join(
        TEST_PATH, os.path.join(
            'parallel', os.path.join('LDJSON_MULTI', 'LDJSON%03d.txt' % i)))

    documents = [0 for _ in range(NUM_CYCLES)]
    with open(ldjson_path, 'r') as data:
        for j in range(NUM_CYCLES):
            documents[j] = json.loads(data.readline())

    client.perftest.corpus.insert_many(documents)


def insert_json_file_with_file_id(i):
    client = MongoClient(host, port)

    ldjson_path = os.path.join(
        TEST_PATH, os.path.join(
            'parallel', os.path.join('LDJSON_MULTI', 'LDJSON%03d.txt' % i)))

    documents = [0 for _ in range(NUM_CYCLES)]
    with open(ldjson_path, 'r') as data:
        for j in range(NUM_CYCLES):
            documents[j] = json.loads(data.readline())
            documents[j]['file'] = i

    client.perftest.corpus.insert_many(documents)


class TestJsonMultiImport(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

    def before(self):
        self.corpus = self.client.perftest.corpus
        self.client.perftest.drop_collection('corpus')

    def do_task(self):
        mp_map(insert_json_file, range(100))

    def tearDown(self):
        super(TestJsonMultiImport, self).tearDown()
        self.client.drop_database('perftest')


def read_json_file(i):
    client = MongoClient(host, port)
    temp_path = os.path.join(
        TEST_PATH, os.path.join(
            'parallel', os.path.join('json_temp', 'LDJSON%03d.txt' % i)))
    with open(temp_path, 'w') as data:
        for doc in client.perftest.corpus.find({'file': i}):
            data.write(str(doc) + '\n')


class TestJsonMultiExport(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')
        self.client.perfest.corpus.create_index('file')

        mp_map(insert_json_file_with_file_id, range(100))

    def before(self):
        self.directory = os.path.join(TEST_PATH, os.path.join(
            'parallel', 'json_temp'))
        if os.path.exists(self.directory):
            shutil.rmtree(self.directory)
        os.makedirs(self.directory)

    def do_task(self):
        mp_map(read_json_file, range(100))

    def after(self):
        shutil.rmtree(self.directory)

    def tearDown(self):
        super(TestJsonMultiExport, self).tearDown()
        self.client.drop_database('perftest')


def insert_gridfs_file(i):
    client = MongoClient(host, port)
    bucket = GridFSBucket(client.perftest)

    filename = 'file%s.txt' % i
    gridfs_path = os.path.join(
        TEST_PATH, os.path.join(
            'parallel', os.path.join('GRIDFS_MULTI', filename)))

    with open(gridfs_path, 'rb') as gfile:
        bucket.upload_from_stream(filename, gfile)


class TestGridFsMultiFileUpload(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

    def before(self):
        self.client.perftest.drop_collection('fs.files')
        self.client.perftest.drop_collection('fs.chunks')

        self.bucket = GridFSBucket(self.client.perftest)

    def do_task(self):
        mp_map(insert_gridfs_file, range(50))

    def tearDown(self):
        super(TestGridFsMultiFileUpload, self).tearDown()
        self.client.drop_database('perftest')


def read_gridfs_file(i):
    client = MongoClient(host, port)
    bucket = GridFSBucket(client.perftest)

    filename = 'file%s.txt' % i
    temp_path = os.path.join(
        TEST_PATH, os.path.join(
            'parallel', os.path.join('gridfs_temp', filename)))
    with open(temp_path, 'wb') as gfile:
        bucket.download_to_stream_by_name(filename, gfile)


class TestGridFsMultiFileDownload(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

        bucket = GridFSBucket(self.client.perftest)

        gridfs_path = os.path.join(
            TEST_PATH, os.path.join('parallel', 'GRIDFS_MULTI'))
        for i in range(50):
            filename = 'file%s.txt' % i
            with open(os.path.join(gridfs_path, filename), 'rb') as gfile:
                bucket.upload_from_stream(filename, gfile)

    def before(self):
        self.directory = os.path.join(
            TEST_PATH, os.path.join('parallel', 'gridfs_temp'))
        if os.path.exists(self.directory):
            shutil.rmtree(self.directory)
        os.makedirs(self.directory)

    def do_task(self):
        mp_map(read_gridfs_file, range(50))

    def after(self):
        shutil.rmtree(self.directory)

    def tearDown(self):
        super(TestGridFsMultiFileDownload, self).tearDown()
        self.client.drop_database('perftest')

if __name__ == "__main__":
    unittest.main()
