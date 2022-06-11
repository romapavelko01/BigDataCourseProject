CREATE KEYSPACE pavelko_dobrovolskyi_keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE pavelko_dobrovolskyi_keyspace;

CREATE TABLE all_primary_on_times_and_bots (user_id int, time_created timestamp, domain text, is_bot int, page_title text, page_id int, page_url text, PRIMARY KEY (time_created, is_bot));

CREATE TABLE pages_by_users (user_id int, page_id int, page_title text, page_url text, PRIMARY KEY (user_id));

CREATE TABLE pages_by_domains (domain text, page_id int, page_title text, page_url text, PRIMARY KEY (domain));

CREATE TABLE pages_by_page_id (page_id int, page_title text, page_url text, PRIMARY KEY (page_id));