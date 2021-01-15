import unittest

from tassandra.cluster import Cluster
from cassandra.protocol import QueryMessage
from cassandra import ConsistencyLevel
from tornado.ioloop import IOLoop

import ccmlib.cluster


class TestClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ccm_cluster = ccmlib.cluster.Cluster('.', 'test', cassandra_version='2.2.6')
        node = cls.ccm_cluster.populate(1).start()[0][0].network_interfaces['binary']
        cls.cluster = Cluster(contact_points=[node[0]], port=node[1])

    @classmethod
    def tearDownClass(cls):
        cls.ccm_cluster.stop(wait=False, gently=False)
        cls.ccm_cluster.remove_dir_with_retry(cls.ccm_cluster.get_path())

    def test_simple(self):
        async def create_and_select():
            await self.cluster.init()
            keyspace_result = await self.cluster.execute(
                QueryMessage(
                    "CREATE KEYSPACE test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };",
                    ConsistencyLevel.ONE
                )
            )

            self.assertEqual(keyspace_result['keyspace'], 'test')
            self.assertEqual(keyspace_result['change_type'], 'CREATED')

            table_result = await self.cluster.execute(
                QueryMessage(
                    "CREATE TABLE test.settings(key_name text, value text, primary key((key_name)));",
                    ConsistencyLevel.ONE
                )
            )

            self.assertEqual(table_result['keyspace'], 'test')
            self.assertEqual(table_result['table'], 'settings')
            self.assertEqual(table_result['change_type'], 'CREATED')

            await self.cluster.execute(
                QueryMessage(
                    "INSERT INTO test.settings (key_name, value) VALUES ('setting_name', 'false');",
                    ConsistencyLevel.ONE
                )
            )

            select_result = await self.cluster.execute(
                QueryMessage(
                    "SELECT * FROM test.settings;",
                    ConsistencyLevel.ONE
                )
            )

            self.assertEqual(select_result[0].key_name, 'setting_name')
            self.assertEqual(select_result[0].value, 'false')

        IOLoop.current().run_sync(create_and_select)
