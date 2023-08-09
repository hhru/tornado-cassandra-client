import os
import shutil

from tassandra.cluster import Cluster
from cassandra.protocol import QueryMessage
from cassandra import ConsistencyLevel
import ccmlib.cluster

from tests import PROJECT_ROOT


class TestClient:
    @classmethod
    def setup_class(cls):
        cassandra_dir = '.cassandra'
        if os.path.exists(PROJECT_ROOT + '/' + cassandra_dir):
            shutil.rmtree(PROJECT_ROOT + '/' + cassandra_dir, ignore_errors=True)
        cls.ccm_cluster = ccmlib.cluster.Cluster(f'{PROJECT_ROOT}', '.cassandra', cassandra_version='2.2.6')
        node = cls.ccm_cluster.populate(1).start()[0][0].network_interfaces['binary']
        cls.cluster = Cluster(contact_points=[node[0]], port=node[1])

    @classmethod
    def teardown_class(cls):
        cls.ccm_cluster.stop(wait=False, gently=False)
        cls.ccm_cluster.remove_dir_with_retry(cls.ccm_cluster.get_path())

    async def test_create_and_select(self):
        await self.cluster.init()
        keyspace_result = await self.cluster.execute(
            QueryMessage(
                "CREATE KEYSPACE test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };",
                ConsistencyLevel.ONE
            )
        )

        assert keyspace_result['keyspace'] == 'test'
        assert keyspace_result['change_type'] == 'CREATED'

        table_result = await self.cluster.execute(
            QueryMessage(
                "CREATE TABLE test.settings(key_name text, value text, primary key((key_name)));",
                ConsistencyLevel.ONE
            )
        )

        assert table_result['keyspace'] == 'test'
        assert table_result['table'] == 'settings'
        assert table_result['change_type'] == 'CREATED'

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

        assert select_result[0].key_name == 'setting_name'
        assert select_result[0].value == 'false'
