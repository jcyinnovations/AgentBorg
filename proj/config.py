import psycopg
import os
import json

CFG_DBNAME = "pg_dbname"
CFG_HOST = "pg_host"
CFG_USER  = "pg_user"
CFG_PASS = "pg_pass"
CFG_NEONDB = "postgresql://yardecraft:EuN6jCsqm2bt@ep-late-meadow-a5u70nft.us-east-2.aws.neon.tech/tsdata_v1?sslmode=require"


def setup(config_file="config.json", DEBUG=False):
    pwd = os.environ['PWD']
    print("Current Directory: ",pwd)
    config = json.load(open(config_file))
    host = config[CFG_HOST]
    dbname = config[CFG_DBNAME]
    username = config[CFG_USER]
    passwd = config[CFG_PASS]

    client = psycopg.connect(host=host, database=dbname, user=username, password=passwd)
    return client

