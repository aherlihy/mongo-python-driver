# Copyright 2009-2015 MongoDB, Inc.
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

"""Tests for the performance spec."""

import multiprocessing as mp
import os
import shutil
import sys
import ujson as json
import warnings

sys.path[0:0] = [""]

from bson import BSON, CodecOptions
from bson.json_util import loads
from gridfs import GridFSBucket
from pymongo import MongoClient
from pymongo.monotonic import time
from pymongo.operations import InsertOne
from test import client_context, host, port, unittest

NUM_ITERATIONS = 100
MAX_ITERATION_TIME = 300

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
        print '\n', self.percentile(50)

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
            self.assertTrue(False, 'Test execution failed')

    def runTest(self):
        results = [0 for _ in range(NUM_ITERATIONS)]
        start = time()
        self.max_iterations = NUM_ITERATIONS
        for i in range(NUM_ITERATIONS):
            if time() - start > MAX_ITERATION_TIME:
                self.max_iterations = i
                warnings.warn("Test timed out, completed %s iterations."
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
                         os.path.join('featherweight', self.dataset))) as data:
            self.documents = loads(data.read())

    def do_task(self):
        for doc in self.documents:
            BSON.encode(doc)


class BsonDecodingTest(PerformanceTest):
    def setUp(self):
        # Location of test data.
        with open(
            os.path.join(TEST_PATH,
                         os.path.join('featherweight', self.dataset))) as data:
            self.documents = [BSON.encode(d) for d in loads(data.read())]

    def do_task(self):
        for doc in self.documents:
            doc.decode(codec_options=CodecOptions(tz_aware=True))


class TestFlatEncoding(BsonEncodingTest, unittest.TestCase):
    def setUp(self):
        self.dataset = 'flat_bson.json'
        super(TestFlatEncoding, self).setUp()


class TestFlatDecoding(BsonDecodingTest, unittest.TestCase):
    def setUp(self):
        self.dataset = 'flat_bson.json'
        super(TestFlatDecoding, self).setUp()


class TestNestedEncoding(BsonEncodingTest, unittest.TestCase):
    def setUp(self):
        self.dataset = 'deep_bson.json'
        super(TestNestedEncoding, self).setUp()


class TestNestedDecoding(BsonDecodingTest, unittest.TestCase):
    def setUp(self):
        self.dataset = 'deep_bson.json'
        super(TestNestedDecoding, self).setUp()


class TestAllBsonTypesEncoding(BsonEncodingTest, unittest.TestCase):
    def setUp(self):
        self.dataset = 'full_bson.json'
        super(TestAllBsonTypesEncoding, self).setUp()


class TestAllBsonTypesDecoding(BsonDecodingTest, unittest.TestCase):
    def setUp(self):
        self.dataset = 'full_bson.json'
        super(TestAllBsonTypesDecoding, self).setUp()


class TestDocuments(PerformanceTest):
    def setUp(self):
        # Location of test data.
        self.documents = [0 for _ in range(self.num_docs)]
        start = time()
        with open(os.path.join(
            TEST_PATH, os.path.join(
                'lightweight', self.dataset)), 'r') as data:
            # Since read only first 10k for Twitter dataset
            for i in range(self.num_docs):
                self.documents[i] = json.loads(data.readline())

        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

    def tearDown(self):
        super(TestDocuments, self).tearDown()
        self.client.drop_database('perftest')

    def before(self):
        self.corpus = self.client.perftest.corpus

    def after(self):
        self.client.perftest.drop_collection('corpus')


class TestRunCommand(TestDocuments, unittest.TestCase):
    def setUp(self):
        self.dataset = 'empty.json'
        self.num_docs = 0  # Don't need to read anything in
        super(TestRunCommand, self).setUp()

    def before(self):
        self.isMaster = {'isMaster': 1}

    def do_task(self):
        for _ in range(10000):
            self.client.perftest.command(self.isMaster)

    def after(self):
        pass


class TestFindOneByID(TestDocuments, unittest.TestCase):
    def setUp(self):
        self.dataset = 'TWITTER.json'
        self.num_docs = 10000
        super(TestFindOneByID, self).setUp()

        result = self.client.perftest.corpus.insert_many(
            self.documents[:self.num_docs])
        self.inserted_ids = result.inserted_ids

    def do_task(self):
        for i in self.inserted_ids:
            self.corpus.find_one({"_id": i})

    def after(self):
        pass


class TestSmallDocInsertOne(TestDocuments, unittest.TestCase):
    def setUp(self):
        self.dataset = 'SMALL_DOC.json'
        self.num_docs = 10000
        super(TestSmallDocInsertOne, self).setUp()

    def do_task(self):
        for doc in self.documents:
            self.corpus.insert_one(doc)


class TestLargeDocInsertOne(TestDocuments, unittest.TestCase):
    def setUp(self):
        self.dataset = 'LARGE_DOC.json'
        self.num_docs = 1000
        super(TestLargeDocInsertOne, self).setUp()

    def do_task(self):
        for doc in self.documents:
            self.corpus.insert_one(doc)


class TestFindManyAndEmptyCursor(TestDocuments, unittest.TestCase):
    def setUp(self):
        self.dataset = 'TWITTER.json'
        self.num_docs = 10000
        super(TestFindManyAndEmptyCursor, self).setUp()

        self.client.perftest.corpus.insert_many(
            self.documents[:self.num_docs])

    def do_task(self):
        len(list(self.corpus.find({})))

    def after(self):
        pass


class TestSmallDocBulkInsert(TestDocuments, unittest.TestCase):
    def setUp(self):
        self.dataset = 'SMALL_DOC.json'
        self.num_docs = 10000
        super(TestSmallDocBulkInsert, self).setUp()

    def do_task(self):
        self.corpus.bulk_write([InsertOne(doc) for doc in self.documents],
                               ordered=True)


class TestLargeDocBulkInsert(TestDocuments, unittest.TestCase):
    def setUp(self):
        self.dataset = 'LARGE_DOC.json'
        self.num_docs = 1000
        super(TestLargeDocBulkInsert, self).setUp()

    def do_task(self):
        self.corpus.bulk_write([InsertOne(doc) for doc in self.documents],
                               ordered=True)


class TestGridFsUpload(PerformanceTest, unittest.TestCase):
    def setUp(self):
        # Location of test data.
        with open(
            os.path.join(TEST_PATH,
                         os.path.join('gridfs', 'file0.txt')), 'r') as data:
            self.documents = data.read()

        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

    def tearDown(self):
        super(TestGridFsUpload, self).tearDown()
        self.client.drop_database('perftest')

    def before(self):
        self.client.perftest.drop_collection('fs.files')
        self.client.perftest.drop_collection('fs.chunks')
        self.bucket = GridFSBucket(self.client.perftest)

    def do_task(self):
        for i in range(100):
            self.bucket.upload_from_stream(
                'gridfstest%03d' % i, self.documents)


class TestGridFsDownload(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

        self.uploaded_id = GridFSBucket(
            self.client.perftest).upload_from_stream(
                "gridfstest",
                open(os.path.join(TEST_PATH,
                                  os.path.join('gridfs', 'file0.txt'))))

    def tearDown(self):
        super(TestGridFsDownload, self).tearDown()
        self.client.drop_database('perftest')

    def before(self):
        self.bucket = GridFSBucket(self.client.perftest)

    def do_task(self):
        for _ in range(100):
            self.bucket.open_download_stream(self.uploaded_id).read()


def import_json_file(i):
    documents = [0 for _ in range(10000)]
    with open(os.path.join(
        TEST_PATH, os.path.join(
            'heavyweight', 'LDJSON%03d.txt' % i)), 'r') as data:
        for j in range(10000):
            documents[j] = json.loads(data.readline())

    client = MongoClient(host, port)
    client.perftest.corpus.insert_many(documents)


def import_json_file_with_file_id(i):
    documents = [0 for _ in range(10000)]
    with open(os.path.join(
        TEST_PATH, os.path.join('heavyweight',
                                'LDJSON%03d.txt' % i)), 'r') as data:
        for j in range(10000):
            doc = json.loads(data.readline())
            doc['file'] = i
            documents[j] = doc

    client = MongoClient(host, port)
    client.perftest.corpus.insert_many(documents)


class TestJsonMultiImport(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

    def before(self):
        self.corpus = self.client.perftest.corpus
        self.client.perftest.drop_collection('corpus')

    def do_task(self):
        pool = mp.Pool(mp.cpu_count())
        pool.map(import_json_file, range(1, 101))
        pool.close()
        pool.join()

    def tearDown(self):
        super(TestJsonMultiImport, self).tearDown()
        self.client.drop_database('perftest')


def export_json_file(i):
    client = MongoClient(host, port)
    with open(os.path.join(
        TEST_PATH, os.path.join(
            'json_temp', 'LDJSON%03d' % i)), 'w') as data:
        for doc in client.perftest.corpus.find({"file": i}):
            data.write(str(doc) + '\n')


class TestJsonMultiExport(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')
        self.client.perfest.corpus.create_index('file')

        pool = mp.Pool(mp.cpu_count())
        pool.map(import_json_file_with_file_id, range(1, 101))
        pool.close()
        pool.join()

    def before(self):
        self.directory = os.path.join(TEST_PATH, os.path.join('json_temp'))
        if os.path.exists(self.directory):
            shutil.rmtree(self.directory)
        os.makedirs(self.directory)

    def do_task(self):
        pool = mp.Pool(mp.cpu_count())
        pool.map(export_json_file, range(1, 101))
        pool.close()
        pool.join()

    def after(self):
        shutil.rmtree(self.directory)

    def tearDown(self):
        super(TestJsonMultiExport, self).tearDown()
        self.client.drop_database('perftest')


def import_gridfs_file(i):
    client = MongoClient(host, port)
    bucket = GridFSBucket(client.perftest)

    filename = 'file%s.txt' % i
    gridfsfile = open(os.path.join(TEST_PATH,
                                   os.path.join('gridfs', filename)))
    bucket.upload_from_stream(filename, gridfsfile)


class TestGridFsMultiFileUpload(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

    def before(self):
        self.client.perftest.drop_collection('fs.files')
        self.client.perftest.drop_collection('fs.chunks')

        self.bucket = GridFSBucket(self.client.perftest)

    def do_task(self):
        pool = mp.Pool(mp.cpu_count())
        pool.map(import_gridfs_file, range(100))
        pool.close()
        pool.join()

    def tearDown(self):
        super(TestGridFsMultiFileUpload, self).tearDown()
        self.client.drop_database('perftest')


def export_gridfs_file(i):
    client = MongoClient(host, port)
    bucket = GridFSBucket(client.perftest)

    filename = 'file%s.txt' % i
    gridfsfile = open(os.path.join(TEST_PATH,
                                   os.path.join('gridfs_temp', filename)), 'w')
    bucket.download_to_stream_by_name(
        filename,
        gridfsfile)


class TestGridFsMultiFileDownload(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

        bucket = GridFSBucket(self.client.perftest)
        for i in range(101):
            bucket.upload_from_stream(
                "file%s.txt" % i, open(
                    os.path.join(TEST_PATH, os.path.join(
                        'gridfs', 'file%s.txt' % i)), 'r'))

    def before(self):
        self.directory = os.path.join(TEST_PATH, os.path.join('gridfs_temp'))
        if os.path.exists(self.directory):
            shutil.rmtree(self.directory)
        os.makedirs(self.directory)

    def do_task(self):
        pool = mp.Pool(mp.cpu_count())
        pool.map(export_gridfs_file, range(101))
        pool.close()
        pool.join()

    def after(self):
        shutil.rmtree(self.directory)

    def tearDown(self):
        super(TestGridFsMultiFileDownload, self).tearDown()
        self.client.drop_database('perftest')

if __name__ == "__main__":
    unittest.main()
