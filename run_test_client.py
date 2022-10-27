import collections

from cassandra import ConsistencyLevel
from cassandra.protocol import QueryMessage
from tornado.ioloop import IOLoop

from tassandra.cluster import Cluster

setattr(collections, "Iterable", collections.abc.Iterable)


def main():
    async def create_and_select():
        cluster = Cluster(contact_points=["10.208.30.96"])
        await cluster.init()

        select_result = await cluster.execute(
            QueryMessage(
                "SELECT * FROM settings.setting;",
                ConsistencyLevel.ONE
            )
        )
        pass

    IOLoop.current().run_sync(create_and_select)

if __name__ == '__main__':
    main()
