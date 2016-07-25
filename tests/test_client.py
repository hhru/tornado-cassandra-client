# coding=utf-8

import unittest

from tassandra.cluster import Cluster
from cassandra.protocol import QueryMessage
from cassandra import ConsistencyLevel
from tornado.ioloop import IOLoop

import ccmlib.cluster


class TestClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ccm_cluster = ccmlib.cluster.Cluster('.', 'test', cassandra_version='2.1.14')
        node = cls.ccm_cluster.populate(1).start()[0][0].network_interfaces['binary']
        cls.cluster = Cluster(contact_points=[node[0]], port=node[1])

    @classmethod
    def tearDownClass(cls):
        cls.ccm_cluster.remove()

    def test_simple(self):
        def create_keyspace():
            future = self.cluster.execute(
                QueryMessage(
                    "CREATE KEYSPACE test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };",
                    ConsistencyLevel.ONE
                )
            )

            future.add_done_callback(create_table)

        def create_table(keyspace_future):
            result = keyspace_future.result()

            self.assertEqual(result['keyspace'], 'test')
            self.assertEqual(result['change_type'], 'CREATED')

            future = self.cluster.execute(
                QueryMessage(
                    "CREATE TABLE test.settings(key_name text, value text, primary key((key_name)));",
                    ConsistencyLevel.ONE
                )
            )

            future.add_done_callback(insert)

        def insert(table_future):
            result = table_future.result()

            self.assertEqual(result['keyspace'], 'test')
            self.assertEqual(result['table'], 'settings')
            self.assertEqual(result['change_type'], 'CREATED')

            future = self.cluster.execute(
                QueryMessage(
                    "INSERT INTO test.settings (key_name, value) VALUES ('setting_name', 'false');",
                    ConsistencyLevel.ONE
                )
            )

            future.add_done_callback(select)

        def select(insert_future):
            future = self.cluster.execute(
                QueryMessage(
                    "SELECT * FROM test.settings;",
                    ConsistencyLevel.ONE
                )
            )

            future.add_done_callback(check)

        def check(select_future):
            result = select_future.result()

            self.assertEqual(result[0].key_name, 'setting_name')
            self.assertEqual(result[0].value, 'false')

            IOLoop.instance().stop()

        IOLoop.instance().add_callback(create_keyspace)
        IOLoop.instance().start()
