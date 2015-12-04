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

import json
import multiprocessing as mp
import os
import shutil
import sys
import warnings

sys.path[0:0] = [""]

from bson.json_util import loads
from bson import BSON, CodecOptions
from gridfs import GridFSBucket
from pymongo import MongoClient
from pymongo.monotonic import time
from pymongo.operations import InsertOne
from test import client_context, host, port, unittest

NUM_ITERATIONS = 1
MAX_ITERATION_TIME = 100

TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('performance_testdata'))
print "TEST_PATH", TEST_PATH

class Timer:
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
        print "Median:", self.percentile(50)

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
            self.before()
            with Timer() as t:
                self.do_task()
            self.after()
            results[i] = t.interval
            if time() - start > MAX_ITERATION_TIME:
                self.max_iterations = i
                warnings.warn(Warning("Test timed out, completed %s iterations." % (self.max_iterations + 1)))
                break

        self.results = results


class Featherweight(PerformanceTest):
    def setUp(self):
        # Location of test data.
        with open(os.path.join(
                TEST_PATH, os.path.join('featherweight', self.dataset))) as f:
            self.documents = loads(f.read())

    def do_task(self):
        for doc in self.documents:
            BSON.encode(doc).decode(codec_options=CodecOptions(tz_aware=True))


class CommonFlatBSON(Featherweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'flat_bson.json'
        super(CommonFlatBSON, self).setUp()


class CommonNestedBSON(Featherweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'deep_bson.json'
        super(CommonNestedBSON, self).setUp()


class AllBSONTypes(Featherweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'full_bson.json'
        super(AllBSONTypes, self).setUp()


class LightAndMiddleweight(PerformanceTest):
    def setUp(self):
        # Location of test data.
        self.documents = [0 for _ in range(self.num_docs)]
        with open(os.path.join(
                TEST_PATH, os.path.join('lightweight', self.dataset)), 'r') as f:
            # Since read only first 10k for Twitter dataset
            for i in range(self.num_docs):
                self.documents[i] = json.loads(f.readline())

        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

    def tearDown(self):
        super(LightAndMiddleweight, self).tearDown()
        self.client.drop_database('perftest')

    def before(self):
        self.corpus = self.client.perftest.corpus

    def after(self):
        self.client.perftest.drop_collection('corpus')


class RunCommand(LightAndMiddleweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'empty.json'
        self.num_docs = 0
        super(RunCommand, self).setUp()

    def before(self):
        self.isMaster = {'isMaster': 1}

    def do_task(self):
        for _ in range(10000):
            self.client.perftest.command(self.isMaster)

    def tearDown(self):
        print "Median:", self.percentile(50)

    def after(self):
        pass


class FindOneByID(LightAndMiddleweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'TWITTER.json'
        self.num_docs = 10000
        super(FindOneByID, self).setUp()

        result = self.client.perftest.corpus.insert_many(
            self.documents[:self.num_docs])
        self.inserted_ids = result.inserted_ids

    def do_task(self):
        for i in self.inserted_ids:
            self.corpus.find_one({"_id": i})

    def after(self):
        pass


class SmallDocInsertOne(LightAndMiddleweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'SMALL_DOC.json'
        self.num_docs = 10000
        super(SmallDocInsertOne, self).setUp()

    def do_task(self):
        for doc in self.documents:
            self.corpus.insert_one(doc)


class LargeDocInsertOne(LightAndMiddleweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'LARGE_DOC.json'
        self.num_docs = 1#000
        super(LargeDocInsertOne, self).setUp()

    def do_task(self):
        for doc in self.documents:
            self.corpus.insert_one(doc)


class FindManyAndEmptyCursor(LightAndMiddleweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'TWITTER.json'
        self.num_docs = 10000
        super(FindManyAndEmptyCursor, self).setUp()

        self.client.perftest.corpus.insert_many(
            self.documents[:self.num_docs])

    def do_task(self):
        len(list(self.corpus.find({})))

    def after(self):
        pass

class SmallDocBulkInsert(LightAndMiddleweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'SMALL_DOC.json'
        self.num_docs = 10000
        super(SmallDocBulkInsert, self).setUp()

    def do_task(self):
        self.corpus.bulk_write([InsertOne(doc) for doc in self.documents],
                               ordered=True)

class LargeDocBulkInsert(LightAndMiddleweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'LARGE_DOC.json'
        self.num_docs = 1#000
        super(LargeDocBulkInsert, self).setUp()

    def do_task(self):
        self.corpus.bulk_write([InsertOne(doc) for doc in self.documents],
                               ordered=True)


class GridFsUpload(PerformanceTest, unittest.TestCase):
    def setUp(self):
        # Location of test data.
        with open(os.path.join(
                TEST_PATH, os.path.join('gridfs', 'file0.txt')), 'r') as f:
            self.documents = f.read()

        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

    def tearDown(self):
        super(GridFsUpload, self).tearDown()
        self.client.drop_database('perftest')

    def before(self):
        self.client.perftest.drop_collection('fs.files')
        self.client.perftest.drop_collection('fs.chunks')
        self.bucket = GridFSBucket(self.client.perftest)

    def do_task(self):
        for i in range(10):# 100
            grid_in = self.bucket.open_upload_stream("gridfstest%s" % i)
            grid_in.write(self.documents)
            grid_in.close()


class GridFsDownload(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

        self.uploaded_id = GridFSBucket(
            self.client.perftest).upload_from_stream(
                "gridfstest",
                os.path.join(TEST_PATH, os.path.join('gridfs', 'file0.txt')))

    def tearDown(self):
        super(GridFsDownload, self).tearDown()
        self.client.drop_database('perftest')

    def before(self):
        self.bucket = GridFSBucket(self.client.perftest)

    def do_task(self):
        for _ in range(100):
            self.bucket.open_download_stream(self.uploaded_id).read()


def import_json_file(i):
    documents = [0 for _ in range(10000)]
    with open(os.path.join( TEST_PATH, os.path.join(
            'heavyweight', 'LDJSON%03d.txt' % i)), 'r') as f:
        for j in range(10000):
            documents[j] = json.loads(f.readline())

    client = MongoClient(host, port)
    client.perftest.corpus.insert_many(documents)


def import_json_file_with_file_id(i):
    documents = [0 for _ in range(10000)]
    with open(os.path.join( TEST_PATH, os.path.join(
            'heavyweight', 'LDJSON%03d.txt' % i)), 'r') as f:
        for j in range(10000):
            doc = json.loads(f.readline())
            doc['file'] = i
            documents[j] = doc

    client = MongoClient(host, port)
    client.perftest.corpus.insert_many(documents)


class JSONMultiImport(PerformanceTest, unittest.TestCase):
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
        super(JSONMultiImport, self).tearDown()
        self.client.drop_database('perftest')


def export_json_file(i):
    client = MongoClient(host, port)
    with open(os.path.join(TEST_PATH, os.path.join(
            'json_temp', 'LDJSON%03d' % i)), 'w') as f:
        for doc in client.perftest.corpus.find({"file": i}):
            f.write(str(doc) + '\n')


class JSONMultiExport(PerformanceTest, unittest.TestCase):
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
        super(JSONMultiExport, self).tearDown()
        self.client.drop_database('perftest')

def import_gridfs_file(i):
    client = MongoClient(host, port)
    bucket = GridFSBucket(client.perftest)

    filename = 'file%s.txt' % i
    bucket.upload_from_stream(filename, os.path.join(
        TEST_PATH, os.path.join('gridfs', filename)))


class GridFsMultiFileUpload(PerformanceTest, unittest.TestCase):
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
        super(GridFsMultiFileUpload, self).tearDown()
        self.client.drop_database('perftest')

def export_gridfs_file(i):
    client = MongoClient(host, port)
    bucket = GridFSBucket(client.perftest)

    filename = 'file%s.txt' % i
    file = open(os.path.join(TEST_PATH, os.path.join('gridfs_temp', filename)), 'w')
    bucket.download_to_stream_by_name(
        filename,
        file)


class GridFsMultiFileDownload(PerformanceTest, unittest.TestCase):
    def setUp(self):
        self.client = client_context.rs_or_standalone_client
        self.client.drop_database('perftest')

        self.directory = os.path.join(TEST_PATH, os.path.join('gridfs_temp'))
        if os.path.exists(self.directory):
            shutil.rmtree(self.directory)
        os.makedirs(self.directory)

        bucket = GridFSBucket(self.client.perftest)
        for i in range(101):
            bucket.upload_from_stream(
                "file%s.txt" % i, open(
                    os.path.join(TEST_PATH, os.path.join(
                        'gridfs', 'file%s.txt' % i)), 'r'))

    def do_task(self):
        pool = mp.Pool(mp.cpu_count())
        pool.map(export_gridfs_file, range(101))
        pool.close()
        pool.join()

    # def after(self):
    #     shutil.rmtree(self.directory)

    def tearDown(self):
        super(GridFsMultiFileDownload, self).tearDown()
        # self.client.drop_database('perftest')









if __name__ == "__main__":
    unittest.main()
