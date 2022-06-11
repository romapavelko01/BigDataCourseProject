import json
import pytz
import time
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from flask import jsonify, request, Flask

class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def category_a_query_1(self, start, end):
        query = "SELECT domain FROM all_primary_on_times_and_bots WHERE time_created >= '%s' AND time_created <= '%s'" \
                " ALLOW FILTERING" % (start, end)
        return self.session.execute(query)

    def category_a_query_2(self, start, end):
        query = "SELECT domain FROM all_primary_on_times_and_bots WHERE is_bot=1 AND time_created >= '%s'" \
                " AND time_created <= '%s' ALLOW FILTERING" % (start, end)
        return self.session.execute(query)

    def category_a_query_3(self, start, end):
        query = "SELECT user_id, page_title FROM all_primary_on_times_and_bots WHERE time_created >= '%s' AND" \
                " time_created <= '%s' ALLOW FILTERING" % (start, end)
        return self.session.execute(query)

    def category_b_query_1(self):
        query = "SELECT domain FROM all_primary_on_times_and_bots"
        return self.session.execute(query)

    def category_b_query_2(self, user_id):
        query = "SELECT page_url FROM pages_by_users WHERE user_id=%s" % user_id
        return self.session.execute(query)

    def category_b_query_3(self, domain):
        query = "SELECT COUNT(*) FROM pages_by_domains WHERE domain = '%s'" % domain
        return self.session.execute(query)

    def category_b_query_4(self, page_id):
        query = "SELECT page_url FROM pages_by_page_id WHERE page_id=%s" % page_id
        return self.session.execute(query)

    def category_b_query_5(self, start, end):
        query = "SELECT user_id FROM all_primary_on_times_and_bots WHERE time_created >= '%s' AND" \
                " time_created <= '%s' ALLOW FILTERING" % (start, end)
        return self.session.execute(query)


class FlaskAPI:
    def __init__(self, host_, port_, keyspace_):
        self.app = Flask(__name__)
        # self.api = Api(self.app)

        self.client = CassandraClient(host_, port_, keyspace_)
        self.client.connect()

        @self.app.route('/a_query_1', methods=['GET'])
        def a_query_1():
            """
            curl -iX GET 'http://127.0.0.1:8080/a_query_1'
            """
            if request.method == 'GET':
                return jsonify(f"{self.__a_query_1()}")

        @self.app.route('/a_query_2', methods=['GET'])
        def a_query_2():
            """
            curl -iX GET 'http://127.0.0.1:8080/a_query_2'
            """
            if request.method == 'GET':
                return jsonify(f"{self.__a_query_2()}")

        @self.app.route('/a_query_3', methods=['GET'])
        def a_query_3():
            """
            curl -iX GET 'http://127.0.0.1:8080/a_query_3'
            """
            if request.method == 'GET':
                return jsonify(f"{self.__a_query_3()}")

        @self.app.route('/b_query_1', methods=['GET'])
        def b_query_1():
            """
            curl -iX GET 'http://127.0.0.1:8080/b_query_1'
            """
            if request.method == 'GET':
                return jsonify(f"{self.__b_query_1()}")

        @self.app.route('/b_query_2', methods=['POST'])
        def b_query_2():
            """
            curl -iX POST -H "Content-Type: application/json" \
            -d '{"user_id": 11260960}' \
            'http://127.0.0.1:8080/b_query_2'
            """
            if request.method == 'POST':
                data = json.loads(json.dumps(request.get_json(force=True)))
                return jsonify(f"{self.__b_query_2(data)}")

        @self.app.route('/b_query_3', methods=['POST'])
        def b_query_3():
            """
            curl -iX POST -H "Content-Type: application/json" \
            -d '{"domain": "pl.wikipedia.org"}' \
            'http://127.0.0.1:8080/b_query_3'
            """
            if request.method == 'POST':
                data = json.loads(json.dumps(request.get_json(force=True)))
                return jsonify(f"{self.__b_query_3(data)}")

        @self.app.route('/b_query_4', methods=['POST'])
        def b_query_4():
            """
            curl -iX POST -H "Content-Type: application/json" \
            -d '{"page_id": "119170884"}' \
            'http://127.0.0.1:8080/b_query_4'
            """
            if request.method == 'POST':
                data = json.loads(json.dumps(request.get_json(force=True)))
                return jsonify(f"{self.__b_query_4(data)}")

        @self.app.route('/b_query_5', methods=['POST'])
        def b_query_5():
            """
            curl -iX POST -H "Content-Type: application/json" \
            -d '{"start": "2022-06-11 10:33:00", "end": "2022-06-11 12:43:00"}' \
            'http://127.0.0.1:8080/b_query_5'
            """
            if request.method == 'POST':
                data = json.loads(json.dumps(request.get_json(force=True)))
                return jsonify(f"{self.__b_query_5(data)}")

    def __a_query_1(self):

        time_now = datetime.now(pytz.timezone('Europe/Kiev'))

        end_time = time_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        start_time = time_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)

        result = {"aggregated_statistics": []}

        while start_time != end_time:
            rows = self.client.category_a_query_1(
                start_time.strftime("%Y-%m-%d %H:%M:%S+0000"),
                (start_time + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S+0000"))

            statistics = {
                "time_start": start_time.strftime("%H:%M"),
                "time_end": (start_time + timedelta(hours=1)).strftime("%H:%M"),
                "statistics": []
            }

            domains = {}
            for row in rows:
                if str(row.domain) not in domains.keys():
                    domains[str(row.domain)] = 1
                else:
                    domains[str(row.domain)] += 1

            for domain in domains.keys():
                statistics["statistics"].append({str(domain): domains[domain]})

            result["aggregated_statistics"].append(statistics)

            start_time = start_time + timedelta(hours=1)

        return result

    def __a_query_2(self):
        time_now = datetime.now(pytz.timezone('Europe/Kiev'))

        end_time = time_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        start_time = time_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)

        rows = self.client.category_a_query_2(
            start_time.strftime("%Y-%m-%d %H:%M:%S+0000"),
            end_time.strftime("%Y-%m-%d %H:%M:%S+0000")
        )

        statistics = {
            "time_start": start_time.strftime("%H:%M"),
            "time_end": end_time.strftime("%H:%M"),
            "statistics": []
        }

        domains = {}
        for row in rows:
            if str(row.domain) not in domains.keys():
                domains[str(row.domain)] = 1
            else:
                domains[str(row.domain)] += 1

        for domain in domains.keys():
            statistics["statistics"].append({"domain": str(domain), "created_by_bots": domains[domain]})

        return statistics

    def __a_query_3(self):

        time_now = datetime.now(pytz.timezone('Europe/Kiev'))

        end_time = time_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        start_time = time_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)

        rows = self.client.category_a_query_3(
            start_time.strftime("%Y-%m-%d %H:%M:%S+0000"),
            end_time.strftime("%Y-%m-%d %H:%M:%S+0000")
        )

        users = {}
        for row in rows:
            if not str(row.user_id) in users.keys():
                users[str(row.user_id)] = {
                    "pages_list": []
                }
            users[str(row.user_id)]["pages_list"].append(str(row.page_title))

        users_sorted_20 = sorted(users.items(), key=lambda item: len(item[1]["pages_list"]), reverse=True)[:20]

        return {
            "start_time": start_time.strftime("%H:%M"),
            "end_time": end_time.strftime("%H:%M"),
            "statistics": users_sorted_20
        }

    def __b_query_1(self):
        rows = self.client.category_b_query_1()
        result = {"domains": []}
        for row in rows:
            if str(row.domain) not in result["domains"]:
                result["domains"].append(str(row.domain))
        return result

    def __b_query_2(self, data):
        try:
            rows = self.client.category_b_query_2(data['user_id'])
        except KeyError as e:
            return jsonify(f"{e}"), 400

        result = {"pages": [], "user_id": data["user_id"]}
        for row in rows:
            result["pages"].append(str(row.page_url))
        return result

    def __b_query_3(self, data):
        try:
            return {"number_of_articles": self.client.category_b_query_3(data["domain"])[0].count}
        except KeyError as e:
            return jsonify(f"{e}"), 400

    def __b_query_4(self, data):
        try:
            rows = self.client.category_b_query_4(data['page_id'])
            if not rows:
                return {"page_url": ""}
            else:
                return {"page_url": rows[0].page_url}
        except KeyError as e:
            return jsonify(f"{e}"), 400

    def __b_query_5(self, data):
        try:
            datetime.strptime(data['start'], "%Y-%m-%d %H:%M:%S")
            datetime.strptime(data['end'], "%Y-%m-%d %H:%M:%S")

            rows = self.client.category_b_query_5(data['start'], data['end'])
        except KeyError as e:
            return jsonify(f"{e}"), 400
        except ValueError as e:
            return jsonify(f"{e}"), 400

        users = {}
        # count the number of created pages for every user
        for row in rows:
            if str(row.user_id) not in users.keys():
                users[str(row.user_id)] = {"pages": 1}
            else:
                users[str(row.user_id)]["pages"] += 1

        return {"users": [
            {"user_id": user_id, "number_of_pages": users[user_id]["pages"]} for user_id in users.keys()
        ]}

    def run(self, host_, port_, debug=False):
        self.app.run(host=host_, port=port_, debug=debug)

    def __del__(self):
        self.client.close()


if __name__ == '__main__':
    host = 'cassandra-node'  # for local run: 'localhost'
    port = 9042
    keyspace = 'pavelko_dobrovolskyi_keyspace'
    app = FlaskAPI(host, port, keyspace)
    app.run("0.0.0.0", 8080, debug=True)
