import json

from cassandra.cluster import Cluster
from cassandra.query import named_tuple_factory
from sseclient import SSEClient


class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)
        self.session.row_factory = named_tuple_factory

    def execute(self, query):
        return self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def write_data(self, time_created, domain, user_id, is_bot, page_title, page_id, page_url):
        query1 = "INSERT INTO all_primary_on_times_and_bots (user_id, time_created, domain, is_bot, page_title, page_id, page_url)" \
                 " VALUES (%d, '%s', '%s', %d, '%s', %d, '%s')" % (int(user_id), time_created, domain, is_bot, page_title, int(page_id), page_url)

        query2 = "INSERT INTO pages_by_users (user_id, page_id, page_title, page_url) VALUES (%d, %d, '%s', '%s')" \
                 % (int(user_id), page_id, page_title, page_url)
        query3 = "INSERT INTO pages_by_domains (domain, page_id, page_title, page_url) VALUES ('%s', %d, '%s', '%s')" \
                 % (domain, int(page_id), page_title, page_url)
        query4 = "INSERT INTO pages_by_page_id (page_id, page_title, page_url) VALUES (%d, '%s', '%s')" % (int(page_id), page_title, page_url)
        self.execute(query1)
        self.execute(query2)
        self.execute(query3)
        self.execute(query4)


if __name__ == "__main__":
    host = 'cassandra-node' # for local run: 'localhost'
    port = 9042
    keyspace = 'pavelko_dobrovolskyi_keyspace'
    cassandra_client = CassandraClient(host, port, keyspace)
    cassandra_client.connect()

    url = 'https://stream.wikimedia.org/v2/stream/page-create'

    messages = SSEClient(url)

    for event in messages:
        if event.event == 'message':
            page_data = "[" + event.data + "]"
            json_dict = json.loads(page_data)
            if json_dict:
                try:
                    all_data = json_dict[0]
                    # time_created, domain, user_id, is_bot, page_title, page_id
                    time_created = all_data['meta']['dt']
                    domain = all_data['meta']['domain']
                    user_id = all_data['performer']['user_id']
                    page_id = all_data['page_id']
                    page_title = all_data['page_title'].replace("'", "''")
                    page_url = all_data["meta"]['uri']

                    is_bot = 1 if all_data['performer']['user_is_bot'] == 'true' else 0
                except KeyError:
                    continue

                cassandra_client.write_data(time_created, domain, user_id, is_bot, page_title, page_id, page_url)
                # print("written successfully")
                # break
        else:
            print("No event")
