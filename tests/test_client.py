import collections
import unittest

from cassandra import ConsistencyLevel
from cassandra.protocol import QueryMessage
from tornado.ioloop import IOLoop

from tassandra.cluster import Cluster

setattr(collections, "Iterable", collections.abc.Iterable)


class TestClient(unittest.TestCase):
    def test_simple(self):
        async def create_and_select():
            self.cluster = Cluster(contact_points=["10.208.30.96"])
            await self.cluster.init()

            select_result = await self.cluster.execute(
                QueryMessage(
                    "SELECT * FROM settings.setting;",
                    ConsistencyLevel.ONE
                )
            )
            pass


        IOLoop.current().run_sync(create_and_select)
