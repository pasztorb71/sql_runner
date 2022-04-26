import multiprocessing

import psycopg2

from Cluster import Cluster
from utils import print_dict


def mproc(db, cmd, return_dict):
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database=db,
        user="postgres",
        password='fLXyFS0RpmIX9uxGII4N')
    cur = conn.cursor()
    cur.execute(cmd)
    record = cur.fetchall()
    d = {}
    for rec in record:
        cur.execute("select count(*) from " + '.'.join(rec))
        r = cur.fetchone()
        d[rec[1]] = r[0]
    return_dict[db] = d
    cur.close()
    conn.close()


if __name__ == '__main__':
    sandbox = Cluster(host='localhost', port=5433, passw='fLXyFS0RpmIX9uxGII4N')
    cmd = "SELECT schemaname,tablename FROM pg_catalog.pg_tables WHERE schemaname NOT in('public', 'pg_catalog', 'information_schema') and tablename not like '%$hist'"

    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    jobs = []
    for db in sandbox.databases:
        p = multiprocessing.Process(target=mproc, args=(db, cmd, return_dict))
        jobs.append(p)
        p.start()
    for job in jobs:
        job.join()
    print_dict(return_dict)

